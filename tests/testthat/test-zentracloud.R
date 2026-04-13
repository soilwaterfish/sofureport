
# Helper function to skip tests if the API key is not available
skip_if_no_key <- function() {
  if (Sys.getenv("ZENTRACLOUD_TOKEN") == "") {
    skip("ZENTRACLOUD TOKEN not found, skipping tests that require it.")
  }
}

test_that("get_zentracloud_v5_data() retrieves data correctly", {
  skip_if_no_key() # Check for the key before running

  # Use a known valid station and a short, historical date range for a stable test
  zentracloud_data <- get_zentracloud_v5_data(device_id = 'z6-32392',
                                              start_datetime = '2026-04-01',
                                              end_datetime = '2026-04-02',
                                              token = get_zentracloud_apikey())


  # Test 1: The output should be a data frame
  expect_s3_class(zentracloud_data, "data.frame")

  # Test 2: The data frame should contain rows
  expect_gt(nrow(nfdrs_data), 0)

  # Test 3: The data frame must have essential columns
  expected_cols <- c("port_num", "measurement", "unit", "sensor_name", "value", "timestamp", "datetime", 'error_code')
  expect_true(all(expected_cols %in% names(zentracloud_data)))

  # Test 4: The returned fuel model should match the input
  expect_true(all(zentracloud_data[1,]$measurement == "Gust Speed"))
})

