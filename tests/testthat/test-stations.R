# In tests/testthat/test-stations.R

# This test will only run if the API key is available in the environment.
# This is crucial so that tests don't fail for other users or on systems
# without the key (like CRAN).
skip_if_no_key <- function() {
  if (Sys.getenv("FEMS_API_KEY") == "") {
    skip("FEMS API key not found, skipping tests that require it.")
  }
}

test_that("get_stations() works with a state_id", {
  skip_if_no_key() # Check for the key before running

  # Run the function with a known good query
  ca_stations <- get_stations(state_id = "CA")

  # Check 1: Is the output a data frame?
  expect_s3_class(ca_stations, "data.frame")

  # Check 2: Does it have more than zero rows?
  expect_gt(nrow(ca_stations), 0)

  # Check 3: Does it have the expected columns?
  expect_true(all(c("station_id", "station_name", "latitude", "longitude") %in% names(ca_stations)))
})

test_that("get_stations() works with station_ids", {
  skip_if_no_key()

  station_data <- get_stations(station_ids = "244705")

  # Check 1: Is it a data frame?
  expect_s3_class(station_data, "data.frame")

  # Check 2: Does it have exactly one row?
  expect_equal(nrow(station_data), 1)

  # Check 3: Does the returned station_id match the input?
  expect_equal(station_data$station_id[1], 244705)
})

test_that("test get_fuel_sample_sites()", {

  skip_if_no_key()

  station_data <- get_fuel_sample_sites()

  # Check 1: Is it a data frame?
  expect_s3_class(station_data, "data.frame")
}
          )

test_that("test get_synoptic_stations()", {

  skip_if_no_key()

  station_data <- get_synoptic_stations(
             state = 'MT'
        )

  # Check 1: Is it a data frame?
  expect_s3_class(station_data, "data.frame")

  expect_equal(station_data$STID[1], 'KBHK')

}
          )







