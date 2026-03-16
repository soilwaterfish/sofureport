# In R/stations.R

#' Get Station Information from GraphQL
#'
#' Retrieves station metadata from the FEMS Climatology API. This function can
#' filter by station IDs or by state IDs.
#'
#' @param station_ids A character vector of one or more station IDs.
#' @param state_id A character vector of one or more two-letter state abbreviations (e.g., c("CA", "OR")).
#' @param has_historic_data A character string filter ("YES", "NO", "ALL"). Defaults to "ALL".
#' @param ... Additional GraphQL parameters to pass to the API query (e.g., `page`, `per_page`, `active`).
#' @return A tidy tibble of station information. The `fuel_models` column will be a nested list-column.
#' @export
#' @examples
#' \dontrun{
#'   # Get all stations in California
#'   ca_stations <- get_stations(state_id = "CA")
#'
#'   # Get metadata for two specific stations
#'   two_stations <- get_stations(station_ids = c("245410", "102004"))
#' }
get_stations <- function(station_ids = NULL, state_id = NULL, has_historic_data = "ALL", ...) {

  # --- Add validation for clarity ---
  if (is.null(station_ids) && is.null(state_id)) {
    warning("No station_ids or state_id provided. This may return all stations and could be a very large request.", call. = FALSE)
  }
  if (!is.null(station_ids) && !is.null(state_id)) {
    stop("Please provide either station_ids or state_id, but not both.", call. = FALSE)
  }

  # --- This is our flexible query ---
  # It defines both $stationIds and $stateId as optional variables.
  graphql_query <- "
    query GetStations(
        $stationIds: String,
        $stateId: String,
        $hasHistoricData: TriState
      ) {
      stationMetaData(
        stationIds: $stationIds,
        stateId: $stateId,
        hasHistoricData: $hasHistoricData
      ){
        _metadata { page per_page total_count page_count }
        data {
          station_id
          fems_station_id
          network_name
          network_id
          wrcc_id
          state
          has_historic_data
          period_record_start
          period_record_stop
          time_zone
          time_zone_offset
          station_name
          latitude
          longitude
          elevation
          agency
          fuel_models {
            slopeclass
            grasstype
          }
        }
      }
    }
  "
  # Handle the state_id case, which requires looping
  if (!is.null(state_id)) {

    # Use purrr::map_dfr to loop over each state, perform the API call,
    # and row-bind the results into a single data frame.
    all_states_data <- purrr::map_dfr(state_id, function(single_state) {

      message(paste("Fetching stations for state:", single_state))

      query_variables <- list(
        stateId = single_state, # Use only one state at a time
        hasHistoricData = has_historic_data,
        ...
      )

      request_body <- list(query = graphql_query, variables = query_variables)
      req <- fems_climatology_request() |> httr2::req_body_json(data = request_body)
      resp <- httr2::req_perform(req)
      body <- httr2::resp_body_json(resp)

      if (!is.null(body$errors)) { stop("GraphQL API returned an error: \n", body$errors[[1]]$message, call. = FALSE) }

      # Return the cleaned data frame for this single state
      purrr::map_dfr(body$data$stationMetaData$data, \(x) { x[sapply(x, is.null)] <- NA; tibble::as_tibble(x) }) %>%
        dplyr::group_by(station_id) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()
    })

    return(all_states_data)
  }

  # Handle the station_ids case (assuming it works with comma-separated values)
  if (!is.null(station_ids)) {
    query_variables <- list(
      stationIds = paste(station_ids, collapse = ","),
      hasHistoricData = has_historic_data,
      ...
    )

    request_body <- list(query = graphql_query, variables = query_variables)
    req <- fems_climatology_request() |> httr2::req_body_json(data = request_body)
    resp <- httr2::req_perform(req)
    body <- httr2::resp_body_json(resp)

    if (!is.null(body$errors)) { stop("GraphQL API returned an error: \n", body$errors[[1]]$message, call. = FALSE) }

    stations_data <- purrr::map_dfr(body$data$stationMetaData$data, \(x) { x[sapply(x, is.null)] <- NA; tibble::as_tibble(x) }) %>%
                     dplyr::slice(1)
    return(stations_data)
  }

}


# In R/synoptic.R (add this new function at the end)

#' Get Station Metadata from the Synoptic API
#'
#' Retrieves a list of stations and their metadata (including location) from the
#' Synoptic (MesoWest) API. This is useful for discovering stations to use with
#' `get_synoptic_timeseries()`.
#'
#' @param state A character string for a two-letter US state abbreviation (e.g., "UT", "CA").
#' @param network A character string or numeric vector of network IDs.
#' @param status A character string to filter by station status. "ACTIVE" is highly recommended. Defaults to "ACTIVE".
#' @param ... Additional parameters to pass to the API. See the Synoptic "Stations Service" documentation for options like `radius`, `bbox`, `country`, etc.
#' @return A tidy tibble of station metadata.
#' @export
#' @examples
#' \dontrun{
#'   # Get all active stations in Utah
#'   mt_stations <- get_synoptic_stations(state = "MT")
#'
#'   # You can then use the STID from this data frame in other functions
#'   if (nrow(mt_stations) > 0) {
#'     a_mt_station_id <- mt_stations$STID[1]
#'
#'     wx_data <- get_synoptic_timeseries(
#'       station_ids = a_mt_station_id,
#'       start_time = "2024-07-01 00:00",
#'       end_time = "2024-07-01 12:00",
#'       variables = "air_temp"
#'     )
#'   }
#' }
get_synoptic_stations <- function(state = NULL, network = NULL, status = "ACTIVE", ...) {

  # Use our Synoptic request builder, but target the 'stations/metadata' endpoint
  req <- synoptic_api_request("stations/metadata") |>
    httr2::req_url_query(
      state = state,
      network = network,
      status = status,
      ...,
      .multi = 'comma'
    )

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)

  # Check for errors from the API
  if (body$SUMMARY$RESPONSE_CODE != 1) {
    stop(
      "Synoptic API returned an error: \n",
      body$SUMMARY$RESPONSE_MESSAGE,
      call. = FALSE
    )
  }

  # The station list is in the 'STATION' element
  stations_list <- body$STATION
  stations_data <- purrr::map_dfr(stations_list, \(x) {

    # 1. Perform a deep clean on the entire record first.
    # This handles all NULLs and logical NAs, no matter how nested.
    cleaned_record <- deep_clean_list(x)

    # 2. Now perform the structural unnesting on the clean data.
    if (is.list(cleaned_record$PERIOD_OF_RECORD)) {
      por <- cleaned_record$PERIOD_OF_RECORD
      names(por) <- paste0("por_", names(por))
      cleaned_record <- c(cleaned_record, por)
    }

    if (is.list(cleaned_record$UNITS)) {
      units <- cleaned_record$UNITS
      names(units) <- paste0("units_", names(units))
      cleaned_record <- c(cleaned_record, units)
    }

    if (is.list(cleaned_record$PROVIDERS) && length(cleaned_record$PROVIDERS) > 0) {
      provider_names <- paste(sapply(cleaned_record$PROVIDERS, function(p) p$name), collapse = ", ")
      cleaned_record$providers <- provider_names
    cleaned_record$PROVIDERS <- NULL
    } else {
      cleaned_record$providers <- NA_character_
      cleaned_record$PROVIDERS <- NULL
    }

    if(is.list(cleaned_record$SITING)) {

      cleaned_record$SITING <- NA_character_

    }

    # 3. Remove the original nested lists.
    cleaned_record$PERIOD_OF_RECORD <- NULL
    cleaned_record$UNITS <- NULL

    # 4. Convert the now-guaranteed-safe list to a tibble.
    tibble::as_tibble(cleaned_record)
  })

  # The API returns lat/lon as strings, so convert them to numeric
  if ("LATITUDE" %in% names(stations_data)) {
    stations_data$LATITUDE <- as.numeric(stations_data$LATITUDE)
  }
  if ("LONGITUDE" %in% names(stations_data)) {
    stations_data$LONGITUDE <- as.numeric(stations_data$LONGITUDE)
  }

  return(stations_data)
}


# In R/synoptic.R (or a new R/utils.R file)

#' Recursively clean a list from a JSON response
#'
#' This function travels through a nested list and replaces all NULL values
#' with NA, and ensures logical NAs are converted to character NAs.
#' @param l A list to be cleaned.
#' @return A cleaned list.
#' @noRd
deep_clean_list <- function(l) {
  # Use lapply to iterate over each element in the list
  lapply(l, function(item) {
    # If the item is NULL, replace it with a single NA
    if (is.null(item)) {
      return(NA)
    }
    # If the item is a logical and its value is NA, convert to character NA
    if (is.logical(item) && is.na(item)) {
      return(NA_character_)
    }
    # If the item is a list itself, call this function recursively
    if (is.list(item)) {
      return(deep_clean_list(item))
    }
    # Otherwise, return the item as is
    return(item)
  })
}
