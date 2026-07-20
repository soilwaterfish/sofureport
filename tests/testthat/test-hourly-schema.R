source(file.path("..", "..", "R", "utils.R"), local = globalenv())

test_that("Synoptic hourly metrics retain the SQLite summary-name contract", {
  hourly <- tibble::tibble(
    station_id = "TEST1",
    date = as.POSIXct("2026-07-15 12:00:00", tz = "America/Denver"),
    air_temp_set_1 = 72.5,
    soil_moisture_set_1 = 14.2
  )

  normalized <- normalize_synoptic_hourly_metric_names(hourly)

  expect_true(all(c(
    "station_id",
    "date",
    "air_temp_set_1.mean",
    "soil_moisture_set_1.mean"
  ) %in% names(normalized)))
  expect_false("air_temp_set_1" %in% names(normalized))
  expect_equal(normalized$air_temp_set_1.mean, 72.5)
})

test_that("Synoptic hourly schema normalization does not double the mean suffix", {
  hourly <- tibble::tibble(
    station_id = "TEST1",
    date = as.POSIXct("2026-07-15 12:00:00", tz = "America/Denver"),
    `air_temp_set_1.mean` = 72.5
  )

  normalized <- normalize_synoptic_hourly_metric_names(hourly)

  expect_true("air_temp_set_1.mean" %in% names(normalized))
  expect_false("air_temp_set_1.mean.mean" %in% names(normalized))
})
