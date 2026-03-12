
# Helper function to skip tests if the API key is not available
skip_if_no_key <- function() {
  if (Sys.getenv("FEMS_API_KEY") == "") {
    skip("FEMS API key not found, skipping tests that require it.")
  }
}

test_that("get_nfdrs() retrieves data correctly", {
  skip_if_no_key() # Check for the key before running

  # Use a known valid station and a short, historical date range for a stable test
  nfdrs_data <- get_nfdrs(
    station_ids = "244705",
    start_date = "2023-07-01",
    end_date = "2023-07-07",
    fuel_model = "Y"
  )

  # Test 1: The output should be a data frame
  expect_s3_class(nfdrs_data, "data.frame")

  # Test 2: The data frame should contain rows
  expect_gt(nrow(nfdrs_data), 0)

  # Test 3: The data frame must have essential columns
  expected_cols <- c("station_id", "observation_time", "burning_index", "energy_release_component", "fuel_model")
  expect_true(all(expected_cols %in% names(nfdrs_data)))

  # Test 4: The returned fuel model should match the input
  expect_true(all(nfdrs_data$fuel_model == "Y"))
})

test_that("get_nfdrs() handles no-data responses gracefully", {
  skip_if_no_key()

  # Use a date range in the future where no data should exist
  no_data <- get_nfdrs(
    station_ids = "244705",
    start_date = "3000-01-01",
    end_date = "3000-01-07",
    fuel_model = "Y"
  )

  # The function should return an empty data frame, not an error or NULL
  expect_s3_class(no_data, "data.frame")
  expect_equal(nrow(no_data), 0)
})
