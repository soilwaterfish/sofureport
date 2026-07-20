# FEMS timestamp helpers.
#
# The FEMS deployment used by SOFU currently reports its observation clock six
# hours ahead of the observed local station time. Normalize that known source
# clock behavior before data reaches hourly keys or cross-source joins. The
# original API values remain available in `source_*` columns on client output.

fems_clock_offset_seconds <- function(offset_hours = 0) {
  offset_seconds <- suppressWarnings(as.numeric(offset_hours)) * 60 * 60
  if (!is.finite(offset_seconds)) {
    stop("FEMS clock offset must be numeric.", call. = FALSE)
  }
  offset_seconds
}

fems_local_query_window <- function(start_date, end_date, station_tz = "America/Denver") {
  start_time <- as.POSIXct(paste(as.Date(start_date), "00:00:00"), tz = station_tz)
  end_time <- as.POSIXct(paste(as.Date(end_date), "23:59:59"), tz = station_tz)
  format_iso_offset <- function(value) {
    offset <- format(value, "%z", tz = station_tz)
    offset <- sub("([+-][0-9]{2})([0-9]{2})$", "\\1:\\2", offset)
    paste0(format(value, "%Y-%m-%dT%H:%M:%S", tz = station_tz), offset)
  }

  list(
    start = format_iso_offset(start_time),
    end = format_iso_offset(end_time)
  )
}

normalize_fems_observation_times <- function(
    data,
    offset_hours = 0,
    timestamp_columns = c(
      "observation_time", "observation_time_lst",
      "display_hour", "display_hour_lst"
    )) {
  if (!is.data.frame(data) || !nrow(data)) {
    return(data)
  }

  offset_seconds <- fems_clock_offset_seconds(offset_hours)
  available_columns <- intersect(timestamp_columns, names(data))

  for (column_name in available_columns) {
    source_column <- paste0("source_", column_name)
    if (!source_column %in% names(data)) {
      data[[source_column]] <- data[[column_name]]
    }

    timestamp <- data[[column_name]]
    if (!inherits(timestamp, "POSIXt")) {
      stop(
        sprintf("FEMS timestamp column `%s` must be parsed before normalization.", column_name),
        call. = FALSE
      )
    }

    timestamp_tz <- attr(timestamp, "tzone")
    if (is.null(timestamp_tz) || !length(timestamp_tz) || !nzchar(timestamp_tz[[1]])) {
      timestamp_tz <- "UTC"
    }
    data[[column_name]] <- as.POSIXct(
      as.numeric(timestamp) + offset_seconds,
      origin = "1970-01-01",
      tz = timestamp_tz[[1]]
    )
  }

  data
}
