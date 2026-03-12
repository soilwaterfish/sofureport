#' Get Weather Observations
#'
#' Retrieves daily or hourly weather observations from the FEMS Climatology API
#' using the `weatherObs` GraphQL field.
#'
#' @param station_ids A character vector of one or more station IDs.
#' @param start_date The start date for the query (can be a Date object or a string in "YYYY-MM-DD" format). The time will default to the beginning of the day (00:00:00).
#' @param end_date The end date for the query (can be a Date object or a string in "YYYY-MM-DD" format). The time will default to the end of the day (23:59:59).
#' @param per_page The number of records to return per page. Defaults to a very large number to retrieve all data in one request, mimicking the frontend.
#' @param ... Additional GraphQL parameters (e.g., `page`, `sortBy`, `hasHistoricData`).
#' @return A tidy tibble of weather observations.
#' @export
#' @examples
#' \dontrun{
#'   # Get weather data for a specific station
#'   wx_data <- get_weather(
#'     station_ids = "245410",
#'     start_date = "2024-07-01",
#'     end_date = "2024-07-05"
#'   )
#' }
get_weather <- function(station_ids, start_date, end_date, per_page = 100000000, ...) {

  # --- THIS IS THE UPDATED QUERY, MATCHING THE WORKING PAYLOAD ---
  graphql_query <- "
    query GetWeatherObs($stationId: String!, $startDate: DateTime!, $endDate: DateTime!) {
      weatherObs(
          stationIds: $stationId
          startDateTimeRange: $startDate
          endDateTimeRange: $endDate
          per_page: 100000000
        ) {
        _metadata { page per_page total_count page_count }
        data {
          station_id
          observation_time
          observation_time_lst
          temperature
          hourly_precip
          relative_humidity
          wind_speed
          wind_direction
          peak_gust_speed
          peak_gust_dir
          sol_rad
          snow_flag
          observation_type
        }
      }
    }
  "

  # Convert R Date objects to the required ISO 8601 UTC timestamp format
  start_datetime_str <- format(as.POSIXct(paste(start_date, "00:00:00")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  end_datetime_str <- format(as.POSIXct(paste(end_date, "23:59:59")), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  # The variables for the query, now using the correct names and format
  query_variables <- list(
    stationId = paste(station_ids, collapse = ","),
    startDate = start_datetime_str,
    endDate = end_datetime_str,
    per_page = per_page,
    ... # Pass through any other variables
  )

  request_body <- list(
    query = graphql_query,
    variables = query_variables
  )

  req <- fems_climatology_request("graphql/") |>
    httr2::req_body_json(data = request_body)

  resp <- httr2::req_perform(req)
  body <- httr2::resp_body_json(resp)

  if (!is.null(body$errors)) {
    stop(
      "GraphQL API returned an error: \n",
      body$errors[[1]]$message,
      call. = FALSE
    )
  }

  weather_data <- purrr::map_dfr(body$data$weatherObs$data, tibble::as_tibble)

  return(weather_data)
}
