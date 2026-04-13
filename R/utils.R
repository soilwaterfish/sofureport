#' Floor to Hourly
#'
#' This function takes a data.frame, floors the 'date_time' to the hour,
#' and calculates the mean for all numeric columns by group.
#'
#' @param data A data.frame or data.table with 'date_time', 'station_id', and 'name' columns.
#' @param type A flag to include precip for 'zentra', default (NULL).
#'
#' @return A tibble that's been aggregated to an hourly summary using the mean. This works for precipitation because
#' synoptic pre computes accumulation in an hour with `precip_accum_one_hour_set_1` which is the same when taking the mean.
#' @export
wrangle_to_hourly <- function(data, type = 'synoptic') {

  dt <- data.table::copy(data)

  # Ensure it's a data.table for the subsequent operations
  data.table::setDT(dt)

  # Step 2: Create the hourly grouping column in-place on our copy 'dt'
  dt[, date := lubridate::floor_date(date_time, "hour")]

  # Step 3: Identify numeric columns to summarize
  numeric_cols <- names(dt)[sapply(dt, is.numeric)]
  numeric_cols <- setdiff(numeric_cols, c("station_id"))

  if (type == 'zentra') {

    hourly_summary <- dt[, {
      # Initialize an empty list to store the results of our aggregations
      results_list <- list()

      # Loop through each numeric column identified
      for (col_name in numeric_cols) {
        current_col_data <- .SD[[col_name]] # Access the actual column data for the current group

        # Check if all values in the current column for the group are NA
        if (all(is.na(current_col_data))) {
          results_list[[col_name]] <- NA_real_ # Assign NA if all are NA
        } else if (grepl("precip", col_name, ignore.case = TRUE)) {
          # If the column name contains 'precip' (case-insensitive), sum it
          results_list[[col_name]] <- sum(current_col_data, na.rm = TRUE)
        } else {
          # Otherwise (for all other numeric columns), take the mean
          results_list[[col_name]] <- mean(current_col_data, na.rm = TRUE)
        }
      }
      # Return the named list. data.table will automatically convert these into new columns.
      results_list
    },
    by = .(station_id, date), # Group by station_id and the newly created hourly 'date'
    .SDcols = numeric_cols # Specify which columns are available in .SD for the loop
    ]

  } else if (type =='synoptic') {
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

  }


  # Step 5: Return the new, summarized data.table
  return(dplyr::tibble(hourly_summary))
}
