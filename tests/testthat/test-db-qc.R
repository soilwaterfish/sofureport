source(testthat::test_path("../../R/db_qc.R"), local = environment())

make_sofu_test_db <- function(with_failures = FALSE, missing_derived = FALSE) {
  db_path <- tempfile(fileext = ".sqlite")
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  recent_date <- as.numeric(as.POSIXct(format(Sys.time() - 12 * 3600, tz = "UTC", usetz = TRUE), tz = "UTC"))
  stale_date <- as.numeric(as.POSIXct(format(Sys.time() - 5 * 24 * 3600, tz = "UTC", usetz = TRUE), tz = "UTC"))

  sites <- data.frame(
    station_id = c("SYN1", "ZEN1"),
    station_name = c("Synoptic One", "Zentra One"),
    api = c("Synoptic", "Zentra"),
    stringsAsFactors = FALSE
  )

  if (with_failures) {
    sites <- rbind(sites, data.frame(
      station_id = "SYN1",
      station_name = "",
      api = "Synoptic",
      stringsAsFactors = FALSE
    ))
  }

  DBI::dbWriteTable(con, "sites_in_cg", sites, overwrite = TRUE)

  synoptic_raw <- data.frame(
    station_id = c("SYN1"),
    date = c(recent_date),
    stringsAsFactors = FALSE
  )
  zentra_raw <- data.frame(
    station_id = c("ZEN1"),
    date = c(if (with_failures) stale_date else recent_date),
    stringsAsFactors = FALSE
  )

  if (with_failures) {
    synoptic_raw <- rbind(synoptic_raw, synoptic_raw)
  }

  DBI::dbWriteTable(con, "synoptic_fems_data", synoptic_raw, overwrite = TRUE)
  DBI::dbWriteTable(con, "zentracloud_data", zentra_raw, overwrite = TRUE)

  if (!missing_derived) {
    synoptic_stats <- data.frame(
      station_id = "SYN1",
      local_date = 20000,
      metric_name = "air_temp_set_1.mean",
      value_min = 1,
      value_mean = 2,
      value_max = 3,
      stringsAsFactors = FALSE
    )
    zentra_stats <- data.frame(
      station_id = "ZEN1",
      local_date = 20000,
      metric_name = "port_1_atmos_41_f_air_temperature",
      value_min = 1,
      value_mean = 2,
      value_max = 3,
      stringsAsFactors = FALSE
    )

    synoptic_pct <- data.frame(
      station_id = "SYN1",
      metric_name = "air_temp_set_1.mean",
      day_of_year_key = "06-01",
      stat_type = "p50",
      stringsAsFactors = FALSE
    )
    zentra_pct <- data.frame(
      station_id = "ZEN1",
      metric_name = "port_1_atmos_41_f_air_temperature",
      day_of_year_key = "06-01",
      stat_type = "p50",
      stringsAsFactors = FALSE
    )

    if (with_failures) {
      zentra_stats$station_id <- "MISSING_SITE"
    }

    DBI::dbWriteTable(con, "synoptic_fems_daily_stats", synoptic_stats, overwrite = TRUE)
    DBI::dbWriteTable(con, "zentracloud_daily_stats", zentra_stats, overwrite = TRUE)
    DBI::dbWriteTable(con, "synoptic_fems_daily_percentiles", synoptic_pct, overwrite = TRUE)
    DBI::dbWriteTable(con, "zentracloud_daily_percentiles", zentra_pct, overwrite = TRUE)
  }

  db_path
}

test_that("validate_sofu_database passes for a minimal valid database", {
  db_path <- make_sofu_test_db()

  result <- validate_sofu_database(db_path)

  expect_s3_class(result, "sofu_db_validation")
  expect_true(result$ok)
  expect_true(all(result$checks$passed))
})

test_that("validate_sofu_database reports duplicate and orphan failures", {
  db_path <- make_sofu_test_db(with_failures = TRUE)

  result <- validate_sofu_database(db_path)

  expect_false(result$ok)
  expect_false(result$checks$passed[result$checks$check == "duplicate_keys:sites_in_cg"])
  expect_false(result$checks$passed[result$checks$check == "duplicate_keys:synoptic_fems_data"])
  expect_false(result$checks$passed[result$checks$check == "non_blank:sites_in_cg.station_name"])
  expect_false(result$checks$passed[result$checks$check == "station_lookup:zentracloud_daily_stats"])
  expect_false(result$checks$passed[result$checks$check == "station_freshness:latest_date"])
  expect_true(any(result$stale_stations$station_id == "ZEN1"))
})

test_that("validate_sofu_database catches missing derived tables when required", {
  db_path <- make_sofu_test_db(missing_derived = TRUE)

  result <- validate_sofu_database(db_path, require_derived_tables = TRUE)

  expect_false(result$ok)
  expect_false(any(result$checks$passed[grepl("^table_exists:synoptic_fems_daily_stats$|^table_exists:zentracloud_daily_stats$|^table_exists:synoptic_fems_daily_percentiles$|^table_exists:zentracloud_daily_percentiles$", result$checks$check)]))
})

test_that("assert_valid_sofu_database errors on invalid databases", {
  db_path <- make_sofu_test_db(with_failures = TRUE)

  expect_error(
    assert_valid_sofu_database(db_path),
    "SOFU database validation failed"
  )
})
