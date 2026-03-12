# In R/fuel_samples.R

#' Get Fuel Sample Data
#'
#' Retrieves fuel sample data from the FEMS API. This function requires specifying
#' the fuel type and category in addition to a date range.
#'
#' @param start_date The start date for the query (e.g., "2024-01-01").
#' @param end_date The end date for the query (e.g., "2024-07-01").
#' @param fuel_type The type of fuel sample to filter by (e.g., "1000-Hour").
#' @param category The category of fuel sample to filter by (e.g., "Dead").
#' @param method The method used to collect sample (e.g., 'Gravimetric').
#' @param sub_category The sub category used (e.g., 'Disturbance'), rarely used.
#' @param site_id An optional character or numeric vector of one or more site IDs.
#' @param status An optional sample status to filter by (e.g., "Submitted", "Accepted").
#' @param ... Additional GraphQL parameters (e.g., `sortBy`, `sortOrder`).
#' @return A tidy tibble of fuel sample data.
#' @export
#' @examples
#' \dontrun{
#'   # Get "1000-Hour", "Dead" fuel samples for a specific site
#'   samples <- get_fuel_samples(
#'     start_date = "2025-01-01",
#'     end_date = "2026-01-01",
#'     fuel_type = "1000-Hour",
#'     category = "Dead",
#'     site_id = 460,
#'     status = "Submitted"
#'   )
#' }
get_fuel_samples <- function(start_date, end_date, fuel_type, category, method = NULL, sub_category = NULL,site_id = NULL, status = NULL, ...) {

  graphql_query <- "
    query GetFuelSamples(
        $returnAll: Boolean!,
        $siteId: String,
        $startDate: DateTime,
        $endDate: DateTime,
        $filterBySampleType: String,
        $filterByStatus: String,
        $filterByCategory: String,
        $filterBySubCategory: String,
        $filterByMethod: String,
        $perPage: Int
      ) {
      getFuelSamples(
          returnAll: $returnAll, siteId: $siteId, startDate: $startDate,
          endDate: $endDate, filterBySampleType: $filterBySampleType,
          filterByStatus: $filterByStatus, filterByCategory: $filterByCategory,
          filterBySubCategory: $filterBySubCategory, filterByMethod: $filterByMethod,
          perPage: $perPage
      ) {
          pageInfo { page, per_page, page_count, total_count }
          fuelSamples {
            fuel_sample_id, site_id,
            fuel { fuel_type, category },
            sub_category, sample, subSampleCount, method_type, status, sample_average_value
          }
      }
    }
  "

  start_datetime_str <- format(as.POSIXct(paste(start_date, "00:00:00")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  end_datetime_str <- format(as.POSIXct(paste(end_date, "23:59:59")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  # The variables list, now with all required filters
  query_variables <- list(
    returnAll = FALSE,
    siteId = if (!is.null(site_id)) paste(site_id, collapse = ",") else NULL,
    startDate = start_datetime_str,
    endDate = end_datetime_str,
    filterBySampleType = fuel_type, # Add the required filter
    filterByCategory = category,     # Add the required filter
    filterBySubCategory = sub_category,     # Add the required filter
    filterByMethod = method,     # Add the required filter
    filterByStatus = status,
    perPage = 300000,
    ...
  )

  request_body <- list(query = graphql_query, variables = query_variables)

  # Call the correct request builder for the fuel model endpoint
  req <- fems_fuelmodel_request() |>
    httr2::req_body_json(data = request_body)

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)

  if (!is.null(body$errors)) { stop("GraphQL API returned an error: \n", body$errors[[1]]$message, call. = FALSE) }
  fuel_data <- purrr::map_dfr(body$data$getFuelSamples$fuelSamples, \(x) {

    # 1. Clean all NULLs in the record to NA first.
    x[sapply(x, is.null)] <- NA

    # 2. Check if the `fuel` element is a valid list.
    if (is.list(x$fuel)) {
      # If it is, extract its elements and add them to the top level.
      x$fuel_type <- x$fuel$fuel_type
      x$category <- x$fuel$category
    } else {
      # If `fuel` is not a list (i.e., it's NA), create placeholder NA columns.
      x$fuel_type <- NA_character_
      x$category <- NA_character_
    }

    # 3. Remove the now-redundant `fuel` list element.
    x$fuel <- NULL

    # 4. Convert the now-flat list into a one-row tibble.
    tibble::as_tibble(x)
  })
  return(fuel_data)
}

# In R/sites.R

#' @importFrom tibble as_tibble
NULL

#' Get Fuel Sample Site Locations
#'
#' Retrieves the locations and metadata of fuel sample sites that have data
#' matching the specified filter criteria. This uses the `getSites` GraphQL field.
#'
#' @return A tidy, flat tibble of site locations and metadata.
#' @export
#' @examples
#' \dontrun{
#'   # Find all sites that have "1000-Hour", "Dead" fuel samples
#'   # submitted within a specific date range.
#'   sites <- get_fuel_sample_sites(
#'     start_date = "2024-01-01",
#'     end_date = "2024-12-31"
#'   )
#' }
get_fuel_sample_sites <- function() {

  # No changes to the query or request setup...
  graphql_query <- "
    query GetSiteForMap($returnAll: Boolean!, $perPage: Int, $fuelSampleMetadataInput: FuelSampleMetadataInputDTO, $zoomLevel: Int) {
      getSites(fuelSampleMetadataInput: $fuelSampleMetadataInput, zoomLevel: $zoomLevel, returnAll: $returnAll, perPage: $perPage) {
        pageInfo { page, per_page, page_count, total_count }
        sites {
          siteId, siteStartDate, siteName, siteStatus, longitude, active, latitude,
          elevation, latestSampleDate, timeZone, timeZoneOffset,
          fuelSampleMetadata { firstSampleDate, latestSampleDate, latestSampleDateInRange, latestSampleAverageInRange }
        }
      }
    }
  "
  start_datetime_str <- format(as.POSIXct(paste('2000-01-01', "00:00:00")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  end_datetime_str <- format(as.POSIXct(paste('2025-01-01', "23:59:59")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  query_variables <- list(returnAll = FALSE, perPage = 300000, zoomLevel = 5, fuelSampleMetadataInput = list(status = 'Submitted', startDate = start_datetime_str, endDate = end_datetime_str, fuelCategory = 'Dead', fuelType = '1000-Hour'))
  request_body <- list(query = graphql_query, variables = query_variables)
  req <- fems_fuelmodel_request() |> httr2::req_body_json(data = request_body)
  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)
  if (!is.null(body$errors)) { stop("GraphQL API returned an error: \n", body$errors[[1]]$message, call. = FALSE) }

  # --- THIS IS THE FINAL, ROBUST FIX ---
  sites_data <- purrr::map_dfr(body$data$getSites$sites, \(x) {

    # 1. Clean NULLs at the top level of the record.
    x[sapply(x, is.null)] <- NA

    # 2. Check if the nested metadata object exists and is a list.
    if (is.list(x$fuelSampleMetadata)) {
      # It exists, so we process it.
      nested_list <- x$fuelSampleMetadata

      # 3. CRITICAL FIX: Clean the NULLs *inside* the nested list.
      nested_list[sapply(nested_list, is.null)] <- NA

      # 4. Prefix the names and combine with the top-level list.
      names(nested_list) <- paste0("metadata_", names(nested_list))
      x <- c(x, nested_list)

    } else {
      # The nested object is missing. We must create placeholder NA columns
      # to ensure every row has the same structure.
      x$metadata_firstSampleDate <- NA
      x$metadata_latestSampleDate <- NA
      x$metadata_latestSampleDateInRange <- NA
      x$metadata_latestSampleAverageInRange <- NA
    }

    # 5. Remove the original (now redundant) nested list element.
    x$fuelSampleMetadata <- NULL

    # 6. Convert the guaranteed-flat list to a one-row tibble.
    tibble::as_tibble(x)
  })
  # --- END OF FIX ---

  return(sites_data)
}
