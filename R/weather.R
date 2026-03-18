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

#' Get Timeseries Weather Data from the Synoptic API
#'
#' Retrieves timeseries weather observation data from the Synoptic (MesoWest) API.
#' This function automatically requests data in US standard units (Fahrenheit, etc.).
#'
#' @param station_ids A character vector of one or more station IDs (e.g., "WBB").
#' @param start_time The start time for the query (can be a Date, POSIXct, or string like "YYYY-MM-DD HH:MM").
#' @param end_time The end time for the query.
#' @param ob_timezone The desired timezone for the observation timestamps, either "UTC" (default) or "local".
#' @param ... Additional parameters to pass to the API. See the Synoptic API documentation for options.
#' @return A tidy, flat tibble of weather observations from all requested stations.
#' @export
#' @importFrom purrr map_dfr
#' @importFrom tibble as_tibble
#' @importFrom dplyr bind_cols
#' @examples
#' \dontrun{
#'   # Get temperature and wind speed for a single station
#'   wx_data <- get_synoptic_timeseries(
#'     station_ids = "EURM8",
#'     start_time = "2024-07-01 00:00",
#'     end_time = "2024-07-01 12:00",
#'     variables = c("air_temp", "wind_speed", "wind_direction")
#'   )
#' }
get_synoptic_timeseries <- function(station_ids, start_time, end_time, ob_timezone = "UTC", ...) {

  # Convert R date/time objects to the required YYYYMMDDHHMM format
  start_str <- format(as.POSIXct(start_time), "%Y%m%d%H%M")
  end_str <- format(as.POSIXct(end_time), "%Y%m%d%H%M")

  # Use our new Synoptic request builder
  req <- synoptic_api_request("stations/timeseries") |>
    httr2::req_url_query(
      stid = paste(station_ids, collapse = ","),
      start = start_str,
      end = end_str,
      #vars = paste(variables, collapse = ","),
      obtimezone = ob_timezone,
      ...
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

  # The data is nested in a list of stations. We loop through it.
  all_station_data <- purrr::map_dfr(body$STATION, function(station) {

    # The observations are parallel vectors. as_tibble handles this perfectly.
    obs_data <- tibble::as_tibble(station$OBSERVATIONS)

    # If there are no observations, return an empty tibble for this station
    if (nrow(obs_data) == 0) {
      return(tibble::tibble())
    }

    # Add the station ID and other metadata to every row
    obs_data$station_id <- station$STID
    obs_data$name <- station$NAME
    obs_data$timezone <- station$TIMEZONE
#
#     # The API returns timestamps as seconds since the epoch. Convert to POSIXct.
#     obs_data$date_time <- as.POSIXct(obs_data$date_time, origin = "1970-01-01", tz = "UTC")

    obs_data
  })

  return(suppressWarnings(tidyr::unnest(all_station_data)))
}



#' Get Long-Duration Timeseries Data from the Synoptic API via Chunking
#'
#' Retrieves timeseries weather data over long periods by breaking the request
#' into smaller, more reliable time chunks. This is designed to avoid API timeouts.
#'
#' @param station_ids A character vector of one or more station IDs.
#' @param start_time The start time for the query.
#' @param end_time The end time for the query.
#' @param chunk_by A string defining the time period for each chunk (e.g., "2 days", "1 week"). Defaults to "2 days".
#' @param parallel A logical value. If TRUE, performs API calls in parallel using the `furrr` package, which can be much faster. Defaults to FALSE.
#' @param ... Additional parameters to pass to the API.
#' @return A tidy, flat tibble of all weather observations across the entire time period.
#' @export
#' @note If running in parallel make sure to call `future::plan(future::multisession)` or whatever format you like.
#' @importFrom furrr future_map_dfr
#' @examples
#' \dontrun{
#'   # Get 30 days of temperature data for a station, running in parallel
#'   long_wx_data <- get_synoptic_timeseries_long(
#'     station_ids = "WBB",
#'     start_time = "2024-06-01",
#'     end_time = "2024-06-30",
#'     parallel = TRUE
#'   )
#' }
get_synoptic_timeseries_long <- function(station_ids, start_time, end_time, chunk_by = "2 days", parallel = FALSE, ...) {

  # 1. Create the sequence of date chunks
  date_sequence <- seq(from = as.Date(start_time), to = as.Date(end_time), by = "1 day")

  # Create start/end pairs for each chunk
  chunk_list <- split(date_sequence, ceiling(seq_along(date_sequence) / as.numeric(gsub("\\D", "", chunk_by))))
  date_pairs <- lapply(chunk_list, function(d) {
    list(start = min(d), end = max(d))
  })

  message(sprintf("Querying %d station(s) over %d time chunks...", length(station_ids), length(date_pairs)))


  safe_query_one_chunk <- purrr::safely(query_one_chunk)

  # 2. Execute the queries
  if (parallel) {
    message("Running queries in parallel. This may take a moment to start...")
    # Use future_map (not future_map_dfr) because the output is now a complex list
    results_list <- furrr::future_map(
      .x = date_pairs,
      .f = safe_query_one_chunk,
      station_ids = station_ids,
      ...,
      .progress = TRUE
    )
  } else {
    results_list <- purrr::map(
      .x = date_pairs,
      .f = safe_query_one_chunk,
      station_ids = station_ids,
      ...
    )
  }

  # 3. Process the results
  message("All chunks retrieved. Processing results...")

  # Separate the successful results from the errors
  successful_results <- purrr::map(results_list, "result")
  failed_results <- purrr::map(results_list, "error")

  # Report any errors that occurred
  num_failures <- sum(!sapply(failed_results, is.null))
  if (num_failures > 0) {
    warning(sprintf("%d of %d API calls failed. The process continued, but some data may be missing.", num_failures, length(date_pairs)), call. = FALSE)
    # You could optionally print the first error message for debugging:
    # print(failed_results[!sapply(failed_results, is.null)][[1]])
  }

  # 4. Combine all the successful data frames into one
  all_data <- dplyr::bind_rows(successful_results)

  message("All chunks retrieved successfully.")
  return(all_data)
}


#' @param date_pair
#' @param station_ids
#' @param ...
#' @noRd
query_one_chunk <- function(date_pair, station_ids, ...) {
  get_synoptic_timeseries(
    station_ids = station_ids,
    start_time = date_pair$start,
    end_time = date_pair$end,
    ...
  )
}
