
source(file.path("..", "..", "R", "utils.R"), local = globalenv())
source(file.path("..", "..", "R", "weather.R"), local = globalenv())
source(file.path("..", "..", "R", "fems_time.R"), local = globalenv())

# Helper function to skip tests if the API key is not available
skip_if_no_key <- function() {
  if (Sys.getenv("FEMS_API_KEY") == "") {
    skip("FEMS API key not found, skipping tests that require it.")
  }
}

test_that("get_weather() retrieves data correctly", {
  skip_if_no_key() # Check for the key before running

  # Use a known valid station and a short, historical date range
  weather_data <- get_weather(
    station_ids = "244705", # Using the station ID from our successful debug
    start_date = "2023-07-01",
    end_date = "2023-07-07"
  )

  # Test 1: The output should be a data frame
  expect_s3_class(weather_data, "data.frame")

  # Test 2: The data frame should contain rows
  expect_gt(nrow(weather_data), 0)

  # Test 3: The data frame must have essential columns
  expected_cols <- c("station_id", "observation_time", "temperature", "relative_humidity", "wind_speed")
  expect_true(all(expected_cols %in% names(weather_data)))
})

test_that("get_weather() handles no-data responses gracefully", {
  skip_if_no_key()

  # Use a date range in the future where no data should exist
  no_data <- get_weather(
    station_ids = "244705",
    start_date = "3000-01-01",
    end_date = "3000-01-07"
  )

  # The function should return an empty data frame, not an error or NULL
  expect_s3_class(no_data, "data.frame")
  expect_equal(nrow(no_data), 0)
})

test_that("FEMS weather records preserve rows with optional NULL fields", {
  normalized <- normalize_fems_weather_record(list(
    station_id = "123",
    observation_time = "2026-07-15T18:00:00Z",
    observation_time_lst = "2026-07-15 12:00:00",
    hourly_precip = 0
  ))

  expect_equal(nrow(normalized), 1)
  expect_equal(normalized$station_id, "123")
  expect_equal(normalized$hourly_precip, 0)
  expect_true(is.na(normalized$temperature))
  expect_true(is.na(normalized$relative_humidity))
})

test_that("FEMS local timestamps retain their Mountain-time instant", {
  local_time <- parse_api_datetime(
    "2026-07-15 17:00:00",
    default_tz = "America/Denver",
    output_tz = "America/Denver"
  )

  expect_equal(format(local_time, tz = "UTC", usetz = TRUE), "2026-07-15 23:00:00 UTC")
  expect_equal(format(local_time, tz = "America/Denver", usetz = TRUE), "2026-07-15 17:00:00 MDT")
})

test_that("FEMS source clock normalization preserves raw values and shifts canonical times", {
  source_time <- as.POSIXct("2026-07-15 23:00:00", tz = "UTC")
  normalized <- normalize_fems_observation_times(
    tibble::tibble(observation_time = source_time),
    offset_hours = -6
  )

  expect_equal(
    format(normalized$source_observation_time, tz = "UTC", usetz = TRUE),
    "2026-07-15 23:00:00 UTC"
  )
  expect_equal(
    format(normalized$observation_time, tz = "America/Denver", usetz = TRUE),
    "2026-07-15 11:00:00 MDT"
  )
})

test_that("FEMS weather request windows use the local station offset", {
  window <- fems_local_query_window("2026-07-15", "2026-07-15", "America/Denver")

  expect_equal(window$start, "2026-07-15T00:00:00-06:00")
  expect_equal(window$end, "2026-07-15T23:59:59-06:00")
})



test_that("test get_synoptic_timeseries()", {

  skip_if_no_key()

  wx_data <- get_synoptic_timeseries(
    station_ids = 'KBHK',
    start_time = "2024-07-01 00:00",
    end_time = "2024-07-01 12:00",
    variables = "air_temp"
  )

  # Check 1: Is it a data frame?
  expect_s3_class(wx_data, "data.frame")

  expect_equal(wx_data$station_id[1], 'KBHK')

}
)
