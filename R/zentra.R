
#' Get Timeseries Readings from the ZentraCloud v5 API
#'
#' @param device_id A character string for a single device serial number.
#' @param start_datetime The start time for the query.
#' @param end_datetime The end time for the query.
#' @param token Your ZentraCloud API key. Defaults to `get_zentracloud_apikey()`.
#' @param units The units for the data. Defaults to "imperial".
#' @param ... Additional query parameters to pass to the API.
#' @return A tidy tibble of all readings.
#' @export
get_zentracloud_v5_data <- function(device_id, start_datetime, end_datetime,
                                    token = get_zentracloud_apikey(), units = "imperial", ...) {

  start_str <- format(as.POSIXct(start_datetime), "%Y-%m-%dT%H:%M:%S%z")
  end_str <- format(as.POSIXct(end_datetime), "%Y-%m-%dT%H:%M:%S%z")

  # --- FIX: Pass the token to the request builder ---
  req <- zentracloud_v5_request(token = token) |>
    httr2::req_url_path_append("devices", device_id, "data") |>
    httr2::req_url_query(direction = 'ascending',
                         start_datetime = start_str,
                         end_datetime = end_str,
  latest = FALSE,
  expand = 'settings',
                         units = units, ...)

  all_data_list <- list()
  current_req <- req
  page_num <- 1

  message(sprintf("Retrieving data for device %s...", device_id))

  while (!is.null(current_req)) {
    message(sprintf("  - Fetching page %d...", page_num))
    resp <- httr2::req_perform(current_req)
    body <- httr2::resp_body_json(resp)
    cleaned_body <- deep_clean_list(body)

    if (length(cleaned_body$values) > 0) {
      page_data <- purrr::map_dfr(cleaned_body$values, tibble::as_tibble)
      all_data_list <- c(all_data_list, list(page_data))
    }

    next_token <- cleaned_body$pagination$next_token
    if (!is.null(next_token) && !is.na(next_token) && next_token != "") {
      # Correctly modify the *base* request by adding the next_token as a query parameter
      current_req <- httr2::req_url_query(req, next_token = next_token)
      page_num <- page_num + 1
    } else {
      # If there's no next_token, we are done. End the loop.
      current_req <- NULL
    }
  }

  if (length(all_data_list) == 0) {
    warning("Query successful, but no data returned.", call. = FALSE)
    return(tibble::tibble())
  }

  final_data <- dplyr::bind_rows(all_data_list)
  print(final_data)
  message("Page retrieval complete.")
  return(final_data)
}


#' Get Long-Duration Timeseries Data from the ZentraCloud v5 API via Chunking
#'
#' @param device_id A character string for a single device serial number.
#' @param start_datetime The start date for the query.
#' @param end_datetime The end date for the query.
#' @param token Your ZentraCloud API token. Defaults to `get_zentracloud_apikey()`.
#' @param chunk_by A string for the time period of each chunk. Defaults to "7 days".
#' @param parallel A logical value. If TRUE, performs API calls in parallel.
#' @return A tidy tibble of all successfully retrieved device readings.
#' @export
get_zentracloud_v5_data_long <- function(device_id, start_datetime, end_datetime,
                                         token = get_zentracloud_apikey(),
                                         chunk_by = "7 days", parallel = FALSE, ...) {

  date_sequence <- seq(from = as.Date(start_datetime), to = as.Date(end_datetime), by = "day")
  num_days <- as.numeric(gsub("\\D", "", chunk_by))
  grouping_factor <- ceiling(seq_along(date_sequence) / num_days)
  chunk_list <- split(date_sequence, grouping_factor)
  date_pairs <- lapply(chunk_list, function(d) {
    list(start = paste(min(d), "00:00:00"), end = paste(max(d), "23:59:59"))
  })

  message(sprintf("Querying device %s over %d time chunks...", device_id, length(date_pairs)))

  safe_query_chunk <- purrr::safely(get_zentracloud_v5_data)

  if (parallel) {
    message("Running queries in parallel...")
    # --- THIS IS THE FINAL, CRITICAL FIX ---
    results_list <- furrr::future_map(
      .x = date_pairs,
      .f = ~ safe_query_chunk(
        device_id = device_id,
        start_datetime = .x$start,
        end_datetime = .x$end,
        token = token
        # The `...` is removed from here
      ),
      .options = furrr::furrr_options(packages = "sofureport"), # Replace with your package name
      .progress = TRUE
    )
  } else {
    # Sequential version for comparison
    results_list <- purrr::map(
      .x = date_pairs,
      .f = ~ safe_query_chunk(device_id = device_id, start_datetime = .x$start, end_datetime = .x$end, token = token)
    )
  }

  message("All chunks retrieved. Processing results...")

  successful_results <- purrr::map(results_list, "result")
  failed_results <- purrr::map(results_list, "error")

  num_failures <- sum(!sapply(failed_results, is.null))
  if (num_failures > 0) {
    warning(sprintf("%d of %d API calls failed. Some data may be missing.", num_failures, length(date_pairs)), call. = FALSE)
  }

  all_data <- dplyr::bind_rows(successful_results)

  message("Processing complete.")
  return(successful_results)
}
