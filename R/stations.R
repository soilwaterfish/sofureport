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
      purrr::map_dfr(body$data$stationMetaData$data, \(x) { x[sapply(x, is.null)] <- NA; tibble::as_tibble(x) })
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
