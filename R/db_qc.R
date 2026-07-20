#' Validate a SOFU SQLite database
#'
#' Runs a small QA/QC suite against the core SOFU SQLite tables and returns a
#' structured summary of pass/fail checks.
#'
#' @param db_path Path to a SQLite database file.
#' @param require_derived_tables Logical; if `TRUE`, require daily stats and
#'   percentile tables in addition to the raw tables.
#' @param timezone Time zone used for station freshness checks.
#' @param freshness_lag_days Maximum allowed lag in days. Stations are
#'   considered current if their latest local date is at least
#'   `Sys.Date(timezone) - freshness_lag_days`.
#'
#' @return A `sofu_db_validation` object with `ok` and `checks` fields.
#' @export
validate_sofu_database <- function(
  db_path,
  require_derived_tables = TRUE,
  timezone = "America/Denver",
  freshness_lag_days = 1L
) {
  if (!file.exists(db_path)) {
    stop("Database not found: ", db_path, call. = FALSE)
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  checks <- list()
  stale_stations <- tibble::tibble(
    station_id = character(),
    station_name = character(),
    api = character(),
    latest_local_date = as.Date(character()),
    status = character()
  )

  add_check <- function(name, passed, details) {
    checks[[length(checks) + 1L]] <<- tibble::tibble(
      check = as.character(name),
      passed = isTRUE(passed),
      details = as.character(details)
    )
  }

  required_tables <- c(
    "sites_in_cg",
    "synoptic_fems_data",
    "zentracloud_data"
  )

  if (isTRUE(require_derived_tables)) {
    required_tables <- c(
      required_tables,
      "synoptic_fems_daily_stats",
      "zentracloud_daily_stats",
      "synoptic_fems_daily_percentiles",
      "zentracloud_daily_percentiles"
    )
  }

  for (table_name in required_tables) {
    add_check(
      paste0("table_exists:", table_name),
      DBI::dbExistsTable(con, table_name),
      if (DBI::dbExistsTable(con, table_name)) {
        "ok"
      } else {
        "missing required table"
      }
    )
  }

  validate_columns <- function(table_name, required_cols) {
    if (!DBI::dbExistsTable(con, table_name)) {
      return(invisible(NULL))
    }

    fields <- DBI::dbListFields(con, table_name)
    missing_cols <- setdiff(required_cols, fields)

    add_check(
      paste0("required_columns:", table_name),
      length(missing_cols) == 0L,
      if (!length(missing_cols)) {
        "ok"
      } else {
        paste("missing:", paste(missing_cols, collapse = ", "))
      }
    )
  }

  duplicate_count <- function(table_name, key_cols) {
    if (!DBI::dbExistsTable(con, table_name)) {
      return(NA_integer_)
    }

    fields <- DBI::dbListFields(con, table_name)
    if (!all(key_cols %in% fields)) {
      return(NA_integer_)
    }

    key_sql <- paste(DBI::dbQuoteIdentifier(con, key_cols), collapse = ", ")
    sql <- sprintf(
      paste(
        "SELECT COUNT(*) AS n_dup_groups",
        "FROM (",
        "  SELECT %s",
        "  FROM %s",
        "  GROUP BY %s",
        "  HAVING COUNT(*) > 1",
        ")"
      ),
      key_sql,
      DBI::dbQuoteIdentifier(con, table_name),
      key_sql
    )

    as.integer(DBI::dbGetQuery(con, sql)$n_dup_groups[[1]])
  }

  null_or_blank_count <- function(table_name, col_name) {
    if (!DBI::dbExistsTable(con, table_name)) {
      return(NA_integer_)
    }

    fields <- DBI::dbListFields(con, table_name)
    if (!col_name %in% fields) {
      return(NA_integer_)
    }

    sql <- sprintf(
      paste(
        "SELECT COUNT(*) AS n_bad",
        "FROM %s",
        "WHERE %s IS NULL OR TRIM(CAST(%s AS TEXT)) = ''"
      ),
      DBI::dbQuoteIdentifier(con, table_name),
      DBI::dbQuoteIdentifier(con, col_name),
      DBI::dbQuoteIdentifier(con, col_name)
    )

    as.integer(DBI::dbGetQuery(con, sql)$n_bad[[1]])
  }

  orphan_station_count <- function(table_name) {
    if (!DBI::dbExistsTable(con, table_name) || !DBI::dbExistsTable(con, "sites_in_cg")) {
      return(NA_integer_)
    }

    fields <- DBI::dbListFields(con, table_name)
    if (!"station_id" %in% fields) {
      return(NA_integer_)
    }

    sql <- sprintf(
      paste(
        "SELECT COUNT(*) AS n_orphans",
        "FROM (",
        "  SELECT DISTINCT CAST(src.station_id AS TEXT) AS station_id",
        "  FROM %s AS src",
        "  WHERE src.station_id IS NOT NULL",
        "    AND TRIM(CAST(src.station_id AS TEXT)) != ''",
        ") AS ids",
        "LEFT JOIN (",
        "  SELECT DISTINCT CAST(station_id AS TEXT) AS station_id",
        "  FROM sites_in_cg",
        "  WHERE station_id IS NOT NULL",
        "    AND TRIM(CAST(station_id AS TEXT)) != ''",
        ") AS sites",
        "ON ids.station_id = sites.station_id",
        "WHERE sites.station_id IS NULL"
      ),
      DBI::dbQuoteIdentifier(con, table_name)
    )

    as.integer(DBI::dbGetQuery(con, sql)$n_orphans[[1]])
  }

  station_freshness <- function() {
    if (!DBI::dbExistsTable(con, "sites_in_cg")) {
      return(tibble::tibble())
    }

    site_fields <- DBI::dbListFields(con, "sites_in_cg")
    if (!all(c("station_id", "station_name", "api") %in% site_fields)) {
      return(tibble::tibble())
    }

    sites <- DBI::dbGetQuery(
      con,
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id,",
        "CAST(station_name AS TEXT) AS station_name,",
        "CAST(api AS TEXT) AS api",
        "FROM sites_in_cg",
        "WHERE station_id IS NOT NULL",
        "AND TRIM(CAST(station_id AS TEXT)) != ''"
      )
    )

    if (!nrow(sites)) {
      return(tibble::tibble())
    }

    synoptic_latest <- if (DBI::dbExistsTable(con, "synoptic_fems_data")) {
      DBI::dbGetQuery(
        con,
        paste(
          "SELECT CAST(station_id AS TEXT) AS station_id, MAX(date) AS max_date",
          "FROM synoptic_fems_data",
          "WHERE station_id IS NOT NULL",
          "AND TRIM(CAST(station_id AS TEXT)) != ''",
          "GROUP BY station_id"
        )
      )
    } else {
      data.frame(station_id = character(), max_date = numeric())
    }

    zentra_latest <- if (DBI::dbExistsTable(con, "zentracloud_data")) {
      DBI::dbGetQuery(
        con,
        paste(
          "SELECT CAST(station_id AS TEXT) AS station_id, MAX(date) AS max_date",
          "FROM zentracloud_data",
          "WHERE station_id IS NOT NULL",
          "AND TRIM(CAST(station_id AS TEXT)) != ''",
          "GROUP BY station_id"
        )
      )
    } else {
      data.frame(station_id = character(), max_date = numeric())
    }

    if (nrow(synoptic_latest)) {
      synoptic_latest$station_id <- as.character(synoptic_latest$station_id)
      synoptic_latest$latest_local_date <- as.Date(format(
        as.POSIXct(as.numeric(synoptic_latest$max_date), origin = "1970-01-01", tz = "UTC"),
        tz = timezone
      ))
    }

    if (nrow(zentra_latest)) {
      zentra_latest$station_id <- as.character(zentra_latest$station_id)
      zentra_latest$latest_local_date <- as.Date(format(
        as.POSIXct(as.numeric(zentra_latest$max_date), origin = "1970-01-01", tz = "UTC"),
        tz = timezone
      ))
    }

    ref_date <- as.Date(format(Sys.time(), tz = timezone, usetz = FALSE))
    min_fresh_date <- ref_date - as.integer(freshness_lag_days)

    synoptic_latest <- dplyr::rename(
      dplyr::as_tibble(synoptic_latest)[, c("station_id", "latest_local_date"), drop = FALSE],
      latest_local_date_synoptic = latest_local_date
    )
    zentra_latest <- dplyr::rename(
      dplyr::as_tibble(zentra_latest)[, c("station_id", "latest_local_date"), drop = FALSE],
      latest_local_date_zentra = latest_local_date
    )

    sites <- dplyr::left_join(
      dplyr::as_tibble(sites),
      synoptic_latest,
      by = "station_id"
    )
    sites <- dplyr::left_join(sites, zentra_latest, by = "station_id")
    sites <- dplyr::mutate(
      sites,
      latest_local_date = dplyr::case_when(
        .data$api == "Zentra" ~ .data$latest_local_date_zentra,
        TRUE ~ .data$latest_local_date_synoptic
      ),
      status = dplyr::case_when(
        is.na(.data$latest_local_date) ~ "missing",
        .data$latest_local_date < min_fresh_date ~ "stale",
        TRUE ~ "current"
      )
    )
    sites <- dplyr::select(
      sites,
      "station_id",
      "station_name",
      "api",
      "latest_local_date",
      "status"
    )

    sites
  }

  validate_columns("sites_in_cg", c("station_id", "station_name", "api"))
  validate_columns("synoptic_fems_data", c("station_id", "date"))
  validate_columns("zentracloud_data", c("station_id", "date"))
  validate_columns("synoptic_fems_daily_stats", c("station_id", "local_date", "metric_name", "value_min", "value_mean", "value_max"))
  validate_columns("zentracloud_daily_stats", c("station_id", "local_date", "metric_name", "value_min", "value_mean", "value_max"))
  validate_columns("synoptic_fems_daily_percentiles", c("station_id", "metric_name", "day_of_year_key", "stat_type"))
  validate_columns("zentracloud_daily_percentiles", c("station_id", "metric_name", "day_of_year_key", "stat_type"))

  dup_sites <- duplicate_count("sites_in_cg", c("station_id", "api"))
  add_check("duplicate_keys:sites_in_cg", identical(dup_sites, 0L), sprintf("%s duplicate station_id/api groups", dup_sites))

  dup_synoptic_raw <- duplicate_count("synoptic_fems_data", c("station_id", "date"))
  add_check("duplicate_keys:synoptic_fems_data", identical(dup_synoptic_raw, 0L), sprintf("%s duplicate station_id/date groups", dup_synoptic_raw))

  dup_zentra_raw <- duplicate_count("zentracloud_data", c("station_id", "date"))
  add_check("duplicate_keys:zentracloud_data", identical(dup_zentra_raw, 0L), sprintf("%s duplicate station_id/date groups", dup_zentra_raw))

  dup_synoptic_stats <- duplicate_count("synoptic_fems_daily_stats", c("station_id", "local_date", "metric_name"))
  add_check("duplicate_keys:synoptic_fems_daily_stats", identical(dup_synoptic_stats, 0L), sprintf("%s duplicate station/day/metric groups", dup_synoptic_stats))

  dup_zentra_stats <- duplicate_count("zentracloud_daily_stats", c("station_id", "local_date", "metric_name"))
  add_check("duplicate_keys:zentracloud_daily_stats", identical(dup_zentra_stats, 0L), sprintf("%s duplicate station/day/metric groups", dup_zentra_stats))

  dup_synoptic_pct <- duplicate_count("synoptic_fems_daily_percentiles", c("station_id", "metric_name", "day_of_year_key", "stat_type"))
  add_check("duplicate_keys:synoptic_fems_daily_percentiles", identical(dup_synoptic_pct, 0L), sprintf("%s duplicate station/metric/doy/stat groups", dup_synoptic_pct))

  dup_zentra_pct <- duplicate_count("zentracloud_daily_percentiles", c("station_id", "metric_name", "day_of_year_key", "stat_type"))
  add_check("duplicate_keys:zentracloud_daily_percentiles", identical(dup_zentra_pct, 0L), sprintf("%s duplicate station/metric/doy/stat groups", dup_zentra_pct))

  blank_station_names <- null_or_blank_count("sites_in_cg", "station_name")
  add_check("non_blank:sites_in_cg.station_name", identical(blank_station_names, 0L), sprintf("%s blank station_name rows", blank_station_names))

  orphan_synoptic_raw <- orphan_station_count("synoptic_fems_data")
  add_check(
    "historical_rows_outside_active_roster:synoptic_fems_data",
    TRUE,
    sprintf("%s retained historical station_id values outside sites_in_cg", orphan_synoptic_raw)
  )

  orphan_zentra_raw <- orphan_station_count("zentracloud_data")
  add_check(
    "historical_rows_outside_active_roster:zentracloud_data",
    TRUE,
    sprintf("%s retained historical station_id values outside sites_in_cg", orphan_zentra_raw)
  )

  orphan_synoptic_stats <- orphan_station_count("synoptic_fems_daily_stats")
  add_check(
    "historical_rows_outside_active_roster:synoptic_fems_daily_stats",
    TRUE,
    sprintf("%s retained historical station_id values outside sites_in_cg", orphan_synoptic_stats)
  )

  orphan_zentra_stats <- orphan_station_count("zentracloud_daily_stats")
  add_check(
    "historical_rows_outside_active_roster:zentracloud_daily_stats",
    TRUE,
    sprintf("%s retained historical station_id values outside sites_in_cg", orphan_zentra_stats)
  )

  freshness_df <- station_freshness()
  if (nrow(freshness_df)) {
    stale_stations <- freshness_df[freshness_df$status != "current", , drop = FALSE]
    add_check(
      "station_freshness:latest_date",
      nrow(stale_stations) == 0L,
      if (!nrow(stale_stations)) {
        sprintf("all stations are current to %s or %s", as.character(as.Date(format(Sys.time(), tz = timezone, usetz = FALSE))), as.character(as.Date(format(Sys.time(), tz = timezone, usetz = FALSE)) - 1L))
      } else {
        paste(
          "stale/missing stations:",
          paste(sprintf("%s[%s]=%s", stale_stations$station_id, stale_stations$api, ifelse(is.na(stale_stations$latest_local_date), "missing", as.character(stale_stations$latest_local_date))), collapse = ", ")
        )
      }
    )
  }

  checks_df <- dplyr::bind_rows(checks)

  structure(
    list(
      ok = all(checks_df$passed),
      checks = checks_df,
      stale_stations = stale_stations
    ),
    class = "sofu_db_validation"
  )
}

#' Assert that a SOFU SQLite database passes validation
#'
#' @param db_path Path to a SQLite database file.
#' @param require_derived_tables Logical; if `TRUE`, require daily stats and
#'   percentile tables in addition to the raw tables.
#'
#' @return Invisibly returns the validation result when successful.
#' @export
assert_valid_sofu_database <- function(db_path, require_derived_tables = TRUE) {
  result <- validate_sofu_database(
    db_path = db_path,
    require_derived_tables = require_derived_tables
  )

  if (!isTRUE(result$ok)) {
    failed <- result$checks[result$checks$passed == FALSE, , drop = FALSE]
    stop(
      paste(
        "SOFU database validation failed:",
        paste(sprintf("%s (%s)", failed$check, failed$details), collapse = "; ")
      ),
      call. = FALSE
    )
  }

  invisible(result)
}

#' @export
print.sofu_db_validation <- function(x, ...) {
  cat("SOFU DB validation:", if (isTRUE(x$ok)) "PASS" else "FAIL", "\n")
  print(x$checks, row.names = FALSE)
  if (!is.null(x$stale_stations) && nrow(x$stale_stations)) {
    cat("\nStale or missing stations:\n")
    print(x$stale_stations, row.names = FALSE)
  }
  invisible(x)
}
