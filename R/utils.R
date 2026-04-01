#' Floor to Hourly
#'
#' This function takes a data.frame, floors the 'date_time' to the hour,
#' and calculates the mean for all numeric columns by group.
#'
#' @param data A data.frame or data.table with 'date_time', 'station_id', and 'name' columns.
#'
#' @return A data.table that's been aggregated to an hourly summary using the mean.
#' @export
#' @noRd
wrangle_to_hourly <- function(data) {

  # --- THIS IS THE FIX ---
  # Step 1: Immediately create a copy of the input data.
  # Any modifications from here on will only affect 'dt', not the original 'data'.
  dt <- data.table::copy(data)
  # --- END OF FIX ---

  # Ensure it's a data.table for the subsequent operations
  data.table::setDT(dt)

  # Step 2: Create the hourly grouping column in-place on our copy 'dt'
  dt[, date := lubridate::floor_date(date_time, "hour")]

  # Step 3: Identify numeric columns to summarize
  numeric_cols <- names(dt)[sapply(dt, is.numeric)]
  numeric_cols <- setdiff(numeric_cols, c("station_id"))

  # Step 4: Perform the high-speed aggregation on our copy 'dt'
  hourly_summary <- dt[, {
    summaries <- lapply(.SD, function(col) {
      if (all(is.na(col))) {
        list(mean = NA_real_)
      } else {
        list(mean = mean(col, na.rm = TRUE))
      }
    })
    unlist(summaries, recursive = FALSE)
  },
  by = .(station_id, date),
  .SDcols = numeric_cols
  ]

  # Step 5: Return the new, summarized data.table
  return(dplyr::tibble(hourly_summary))
}
