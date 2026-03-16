# You can reuse the skip function if you define it in a helper file,
# or just redefine it here.
skip_if_no_key <- function() {
  if (Sys.getenv("FEMS_API_KEY") == "") {
    skip("FEMS API key not found, skipping tests that require it.")
  }
}

test_that("get_fuel_samples() works with valid filters", {
  skip_if_no_key()

  # Use a historical date range known to have data
  fuel_data <- get_fuel_samples(
    start_date = "2000-01-01",
    end_date = "2026-01-01",
    fuel_type = "1000-Hour",
    category = "Dead",
    site_id = 746,
    status = "Submitted"
  )
  fuel_data_gravimetric <- get_fuel_samples(
    start_date = "2000-01-01",
    end_date = "2026-01-01",
    fuel_type = "1000-Hour",
    category = "Dead",
    method = 'Gravimetric',
    site_id = 746,
    status = "Submitted"
  )

  # Check 1: Is it a data frame?
  expect_s3_class(fuel_data, "data.frame")

  # Check 2: Does it have the expected columns after unnesting?
  expect_true(all(c("fuel_sample_id", "site_id", "fuel_type", "category") %in% names(fuel_data)))


  # Use a historical date range known to have data
  fuel_data_whortle <- get_fuel_samples(
    start_date = "2000-01-01",
    end_date = "2026-01-01",
    fuel_type = "Whortleberry, Grouse",
    category = "Shrub",
    site_id = 746,
    status = "Submitted"
  )



  })
