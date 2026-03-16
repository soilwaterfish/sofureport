# These tests perform live downloads, so we skip them on CRAN and if offline.
# Note: These specific endpoints do not require an API key, so we don't need skip_if_no_key().

test_that("get_fordri_stack() downloads and processes a raster stack", {
  # Skip this test if on CRAN or if there is no internet connection
  testthat::skip_on_cran()
  testthat::skip_if_offline(host = "fordri.unl.edu")

  # Test with a short, recent date range to ensure speed and data availability
  start_date <- Sys.Date() - 7
  end_date <- Sys.Date()

  # Use expect_no_error to confirm the download and processing runs without crashing
  fordri_stack <- testthat::expect_no_error(
    get_fordri_stack(start_date = start_date, end_date = end_date)
  )

  # If no files were found in the last 7 days, skip further checks
  testthat::skip_if(is.null(fordri_stack) || terra::nlyr(fordri_stack) == 0, "No ForDRI files found in the last 7 days to test.")

  # Test 1: The output should be a SpatRaster object
  testthat::expect_s4_class(fordri_stack, "SpatRaster")

  # Test 2: The raster stack should have at least one layer
  testthat::expect_gt(terra::nlyr(fordri_stack), 0)

  # Test 4: The layer names should look like dates
  testthat::expect_true(all(grepl("^20", names(fordri_stack))))
})


test_that("get_eddi_stack() downloads and processes a raster stack", {
  # Skip this test if on CRAN or if there is no internet connection
  testthat::skip_on_cran()
  testthat::skip_if_offline(host = "downloads.psl.noaa.gov")

  # Test with a short, recent date range
  start_date <- Sys.Date() - 7
  end_date <- Sys.Date()

  eddi_stack <- testthat::expect_no_error(
    get_eddi_stack(start_date = start_date, end_date = end_date)
  )

  testthat::skip_if(is.null(eddi_stack) || terra::nlyr(eddi_stack) == 0, "No EDDI files found in the last 7 days to test.")

  # Test 1: The output should be a SpatRaster object
  testthat::expect_s4_class(eddi_stack, "SpatRaster")

  # Test 2: The raster stack should have at least one layer
  testthat::expect_gt(terra::nlyr(eddi_stack), 0)

  # Test 4: The layer names should look like dates
  testthat::expect_true(all(grepl("^20", names(eddi_stack))))
})
