#' Get NFDRS Observations
#'
#' Retrieves hourly or daily fire danger observations using the GraphQL endpoint.
#'
#' @param station_ids A character vector of station IDs.
#' @param start_date The start date for the query (YYYY-MM-DD).
#' @param end_date The end date for the query (YYYY-MM-DD).
#' @param fuel_model The fuel model (e.g., "Y").
#' @param ... Additional GraphQL parameters (see API documentation).
#' @return A tidy tibble of NFDRS observations.
#' @export
get_nfdrs <- function(station_ids, start_date, end_date, fuel_model, ...) {

  graphql_query <- "
    query GetNfdrHourly(
        $stationIds: String,
        $fuelModel: String!,
        $nfdrType: String,
        $startDateRange: Date,
        $endDateRange: Date,
        $page: Int,
        $per_page: Int,
        $sortBy: NfdrObsSortBy,
        $sortOrder: SortOrder
      ) {
      nfdrsObs(
          stationIds: $stationIds,
          fuelModels: $fuelModel,
          nfdrType: $nfdrType,
          startDateRange: $startDateRange,
          endDateRange: $endDateRange,
          page: $page,
          per_page: $per_page,
          sortBy: $sortBy,
          sortOrder: $sortOrder
        ) {
        _metadata {
          page
          per_page
          total_count
          page_count
        }
        data {
          station_id
          station_name
          observation_time
          observation_time_lst
          display_hour
          display_hour_lst
          nfdr_type
          fuel_model
          one_hr_tl_fuel_moisture
          ten_hr_tl_fuel_moisture
          hun_hr_tl_fuel_moisture
          thou_hr_tl_fuel_moisture
          ignition_component
          spread_component
          energy_release_component
          burning_index
          herbaceous_lfi_fuel_moisture
          woody_lfi_fuel_moisture
          gsi
          kbdi
          quality_code
        }
      }
    }
  "

  # The variables for the query
  query_variables <- list(
    stationIds = paste(station_ids, collapse = ","),
    startDateRange = as.character(start_date),
    endDateRange = as.character(end_date),
    fuelModel = fuel_model,
    ... # Pass through any other variables
  )

  # The full request body
  request_body <- list(
    query = graphql_query,
    variables = query_variables
  )

  # Use our base request builder, but for the GraphQL endpoint
  req <- fems_climatology_request() |>
    # Use req_body_json for POST requests
    httr2::req_body_json(data = request_body)

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)

  # The data is nested deep inside the GraphQL response
  nfdrs_data <- purrr::map_dfr(body$data$nfdrsObs$data, tibble::as_tibble)

  if(nrow(nfdrs_data) < 1){

    nfdrs_data

  } else {

    nfdrs_data %>% dplyr::mutate(display_hour = lubridate::as_datetime(display_hour))

  }
}

#' Get Long-Duration NFDRS Observations via Chunking
#'
#' Retrieves NFDRS observations over long periods by breaking the request
#' into smaller time chunks. This is designed to avoid API timeouts or
#' overly large GraphQL responses.
#'
#' @param station_ids A character vector of station IDs.
#' @param start_date The start date for the query.
#' @param end_date The end date for the query.
#' @param fuel_model The fuel model (e.g., "Y").
#' @param chunk_by A string defining the time period for each chunk
#'   (e.g., "7 days", "14 days", "1 month"). Defaults to "7 days".
#'   Currently this implementation parses the numeric portion and treats it
#'   as a number of days.
#' @param parallel A logical value. If TRUE, performs API calls in parallel
#'   using the `furrr` package. Defaults to FALSE.
#'
#' @return A tidy tibble of NFDRS observations across the full time period.
#' @export
#' @note If running in parallel make sure to call
#'   `future::plan(future::multisession)` or similar first.
#' @importFrom furrr future_map
#' @examples
#' \dontrun{
#'   nfdrs_long <- get_nfdrs_long(
#'     station_ids = c("123456", "789012"),
#'     start_date = "2024-06-01",
#'     end_date = "2024-08-31",
#'     fuel_model = "Y",
#'     chunk_by = "7 days",
#'     parallel = TRUE
#'   )
#' }
get_nfdrs_long <- function(
    station_ids,
    start_date,
    end_date,
    fuel_model,
    chunk_by = "7 days",
    parallel = FALSE
) {

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  if (start_date > end_date) {
    stop("`start_date` must be less than or equal to `end_date`.", call. = FALSE)
  }

  chunk_n <- as.numeric(gsub("\\D", "", chunk_by))

  if (is.na(chunk_n) || chunk_n < 1) {
    stop("`chunk_by` must contain a positive integer, e.g. '7 days'.", call. = FALSE)
  }

  # Create daily sequence, then split into chunks
  date_sequence <- seq(from = start_date, to = end_date, by = "1 day")

  chunk_list <- split(
    date_sequence,
    ceiling(seq_along(date_sequence) / chunk_n)
  )

  date_pairs <- lapply(chunk_list, function(d) {
    list(
      start = min(d),
      end = max(d)
    )
  })

  message(sprintf(
    "Querying %d station(s) over %d time chunks...",
    length(station_ids),
    length(date_pairs)
  ))

  safe_query_one_nfdrs_chunk <- purrr::safely(query_one_nfdrs_chunk)

  if (parallel) {
    message("Running queries in parallel. This may take a moment to start...")
    results_list <- furrr::future_map(
      .x = date_pairs,
      .f = safe_query_one_nfdrs_chunk,
      station_ids = station_ids,
      fuel_model = fuel_model,
      .progress = TRUE
    )
  } else {
    results_list <- purrr::map(
      .x = date_pairs,
      .f = safe_query_one_nfdrs_chunk,
      station_ids = station_ids,
      fuel_model = fuel_model
    )
  }

  message("All chunks retrieved. Processing results...")

  successful_results <- purrr::map(results_list, "result")
  failed_results <- purrr::map(results_list, "error")

  num_failures <- sum(!vapply(failed_results, is.null, logical(1)))

  if (num_failures > 0) {
    warning(
      sprintf(
        "%d of %d API calls failed. The process continued, but some data may be missing.",
        num_failures,
        length(date_pairs)
      ),
      call. = FALSE
    )
  }

  # Drop NULLs and bind safely
  successful_results <- successful_results[!vapply(successful_results, is.null, logical(1))]

  if (length(successful_results) == 0) {
    message("No successful chunks returned data.")
    return(tibble::tibble())
  }

  all_data <- dplyr::bind_rows(successful_results)

  message("All chunks retrieved successfully.")
  return(all_data)
}


#' Query one NFDRS chunk
#'
#' @param date_pair A list with `start` and `end` elements.
#' @param station_ids A character vector of station IDs.
#' @param fuel_model The fuel model.
#' @param ... Additional GraphQL parameters passed to `get_nfdrs()`.
#' @noRd
query_one_nfdrs_chunk <- function(date_pair, station_ids, fuel_model, ...) {
  get_nfdrs(
    station_ids = station_ids,
    start_date = date_pair$start,
    end_date = date_pair$end,
    fuel_model = fuel_model,
    ...
  )
}
