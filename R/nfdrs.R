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
  req <- fems_climatology_request("graphql/") |>
    # Use req_body_json for POST requests
    httr2::req_body_json(data = request_body)

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)

  # The data is nested deep inside the GraphQL response
  nfdrs_data <- purrr::map_dfr(body$data$nfdrsObs$data, tibble::as_tibble)

  return(nfdrs_data)
}
