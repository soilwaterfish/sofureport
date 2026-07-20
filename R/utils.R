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
        NA_real_
      } else {
        mean(col, na.rm = TRUE)
      }
    })
    stats::setNames(summaries, names(.SD))
  },
  by = .(station_id, date),
  .SDcols = numeric_cols
  ]

  }


  # Step 5: Return the new, summarized data.table
  return(dplyr::tibble(hourly_summary))
}

# Synoptic returns raw variable names (for example, `air_temp_set_1`), while
# the existing SQLite contract stores hourly summaries as `<variable>.mean`.
# FEMS callers already provide that suffix, so this is intentionally applied
# only to Synoptic API output at the ingestion boundary.
normalize_synoptic_hourly_metric_names <- function(data) {
  identity_cols <- c("station_id", "date")
  metric_cols <- setdiff(names(data), identity_cols)

  if (!length(metric_cols)) {
    return(data)
  }

  renamed <- ifelse(grepl("\\.mean$", metric_cols), metric_cols, paste0(metric_cols, ".mean"))
  names(data)[match(metric_cols, names(data))] <- renamed
  data
}

parse_api_datetime <- function(x, default_tz = "UTC", output_tz = default_tz) {
  if (inherits(x, "POSIXt")) {
    return(lubridate::with_tz(as.POSIXct(x), output_tz))
  }

  if (is.numeric(x)) {
    return(lubridate::with_tz(
      as.POSIXct(x, origin = "1970-01-01", tz = "UTC"),
      output_tz
    ))
  }

  values <- trimws(as.character(x))
  values[!nzchar(values)] <- NA_character_

  parsed <- rep(as.POSIXct(NA_real_, origin = "1970-01-01", tz = output_tz), length(values))
  has_value <- !is.na(values)

  if (!any(has_value)) {
    return(parsed)
  }

  has_explicit_tz <- grepl("(Z|[+-][0-9]{2}:?[0-9]{2})$", values)

  if (any(has_value & has_explicit_tz)) {
    parsed_explicit <- suppressWarnings(
      lubridate::ymd_hms(values[has_value & has_explicit_tz], quiet = TRUE)
    )
    parsed[has_value & has_explicit_tz] <- lubridate::with_tz(parsed_explicit, output_tz)
  }

  if (any(has_value & !has_explicit_tz)) {
    parsed_local <- suppressWarnings(
      lubridate::ymd_hms(
        values[has_value & !has_explicit_tz],
        tz = default_tz,
        quiet = TRUE
      )
    )
    parsed[has_value & !has_explicit_tz] <- parsed_local
  }

  parsed
}
