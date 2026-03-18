# In R/zentracloud_wrapper.R (replace the get_zentracloud_readings_long function)

# The helper function now accepts the token as an argument
query_one_chunk <- function(date_pair, device_sn, token) {
  # --- THIS IS THE KEY FIX ---
  # Set the token option *inside* the worker session before making the call
  zentracloud::setZentracloudOptions(token = token)

  zentracloud::getReadings(
    device_sn = device_sn,
    start_time = date_pair$start,
    end_time = date_pair$end
  )
}

#' Get Long-Duration Timeseries Data from ZentraCloud via Chunking
#'
#' This function acts as a wrapper around `zentracloud::getReadings()` to retrieve
#' data over long periods. It avoids API timeouts by breaking the request into
#' smaller, sequential time chunks.
#'
#' @param device_sn A character string for a single device serial number.
#' @param start_time The start date for the query.
#' @param end_time The end date for the query.
#' @param token Your ZentraCloud API token. Defaults to the value stored in the `ZENTRACLOUD_TOKEN` environment variable.
#' @param chunk_by A string defining the time period for each chunk (e.g., "7 days", "1 month"). Defaults to "7 days".
#' @param parallel A logical value. If TRUE, performs API calls in parallel. Defaults to FALSE.
#' @return A tidy tibble of all successfully retrieved device readings.
#' @export
#' @examples
#' \dontrun{
#'   # Get a full year of data for a device, running in parallel.
#'   # The function will automatically use the token from your .Renviron file.
#'   yearly_data <- get_zentracloud_readings_long(
#'     device_sn = "z6-32393",
#'     start_time = "2023-01-01",
#'     end_time = "2023-12-31",
#'     chunk_by = "1 month",
#'     parallel = TRUE
#'   )
#' }
get_zentracloud_readings_long <- function(device_sn, start_time, end_time,
                                          token = Sys.getenv('ZENTRACLOUD_TOKEN'),
                                          chunk_by = "7 days", parallel = FALSE) {

  if (!requireNamespace("zentracloud", quietly = TRUE)) {
    stop("The 'zentracloud' package is required for this function. Please install it.", call. = FALSE)
  }
  if (token == "") {
    stop("ZentraCloud token not found. Please supply it or set ZENTRACLOUD_TOKEN in .Renviron.", call. = FALSE)
  }

  date_sequence <- seq(from = as.Date(start_time), to = as.Date(end_time), by = "day")
  num_days <- as.numeric(gsub("\\D", "", chunk_by))
  grouping_factor <- ceiling(seq_along(date_sequence) / num_days)
  chunk_list <- split(date_sequence, grouping_factor)
  date_pairs <- lapply(chunk_list, function(d) {
    list(start = paste(min(d), "00:00:00"), end = paste(max(d), "23:59:59"))
  })

  message(sprintf("Querying device %s over %d time chunks...", device_sn, length(date_pairs)))

  safe_query_chunk <- purrr::safely(query_one_chunk)

  if (parallel) {
    message("Running queries in parallel...")
    future::plan(future::multisession)
    # --- THIS IS THE SECOND PART OF THE FIX ---
    # We now explicitly pass the `token` to each parallel worker.
    results_list <- furrr::future_map(
      .x = date_pairs, .f = safe_query_chunk, device_sn = device_sn, token = token, .progress = TRUE
    )
  } else {
    results_list <- purrr::map(
      .x = date_pairs, .f = safe_query_chunk, device_sn = device_sn, token = token
    )
  }

  message("All chunks retrieved. Processing results...")

  successful_results <- purrr::map(results_list, "result")
  failed_results <- purrr::map(results_list, "error")

  num_failures <- sum(!sapply(failed_results, is.null))
  if (num_failures > 0) {
    warning(sprintf("%d of %d API calls failed. The process continued, but some data may be missing.", num_failures, length(date_pairs)), call. = FALSE)
  }

  all_data <- dplyr::bind_rows(successful_results)

  message("Processing complete.")
  return(all_data)
}
