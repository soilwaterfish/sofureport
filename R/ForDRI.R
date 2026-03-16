# In R/noaa_rasters.R (add this new function at the end)

#' Download the Latest ForDRI Raster from UNL
#'
#' This function finds and downloads the most recently available ForDRI (Flash
#' Drought Risk Index) raster file from the University of Nebraska-Lincoln's
#' data archive and loads it as a SpatRaster object.
#'
#' @param search_days An integer for how many past days to search for a valid file. Defaults to 14.
#' @return A `SpatRaster` object from the `terra` package.
#' @export
#' @examples
#' \dontrun{
#'   # Get the latest ForDRI raster
#'   latest_fordri_raster <- get_latest_fordri_raster()
#'
#'   # Plot the raster
#'   terra::plot(latest_fordri_raster)
#' }
get_latest_fordri_raster <- function(search_days = 14) {

  base_url <- "https://fordri.unl.edu/data/ForDRI_CONUS/tif/"

  # Loop backwards from today to find the most recent available file
  for (i in 0:search_days) {

    current_date <- Sys.Date() - i
    # The API requires the date in YYYY-MM-DD format
    date_str <- format(current_date, "%Y-%m-%d")

    # Construct the filename and full URL
    filename <- sprintf("fordri_%s.tif", date_str)
    full_url <- paste0(base_url, filename)

    # Create a temporary file path to download to
    temp_file <- tempfile(fileext = ".tif")

    message(paste("Attempting to download:", filename))

    # Build the request, gracefully handling 404 errors
    req <- httr2::request(full_url) |>
      httr2::req_error(is_error = function(resp) FALSE)

    resp <- httr2::req_perform(req, path = temp_file)

    # Check if the download was successful (status code 200)
    if (httr2::resp_status(resp) == 200) {
      message("Download successful!")

      # Load the downloaded GeoTIFF file as a raster object
      raster_object <- terra::rast(temp_file)

      # Return the raster object, which exits the loop
      return(raster_object)
    }
  }

  # If the loop finishes without finding a file, throw an error
  stop(sprintf("Could not find a valid ForDRI raster file in the last %d days.", search_days))
}


#' Download a Time Series Stack of ForDRI Rasters from UNL
#'
#' This function finds and downloads all available ForDRI raster files within a
#' specified date range and stacks them into a single multi-layer SpatRaster object.
#'
#' @param start_date The start date for the query (e.g., "2023-01-01").
#' @param end_date The end date for the query. Defaults to the current system date.
#' @return A multi-layer `SpatRaster` object from the `terra` package, with each layer named by its date.
#' @export
get_fordri_stack <- function(start_date, end_date = Sys.Date()) {

  date_sequence <- seq(from = as.Date(start_date), to = as.Date(end_date), by = "days")
  base_url <- "https://fordri.unl.edu/data/ForDRI_CONUS/tif/"
  raster_list <- list()
  successful_dates <- c()

  # --- FIX #1: Explicitly convert dates to character for the message ---
  message(sprintf("Attempting to download ForDRI rasters from %s to %s...", as.character(start_date), as.character(end_date)))

  for (current_date in date_sequence) {
    filename <- sprintf("fordri_%s.tif", as.Date(current_date))
    full_url <- paste0(base_url, filename)
    temp_file <- tempfile(fileext = ".tif")

    req <- httr2::request(full_url) |> httr2::req_error(is_error = \(resp) FALSE)
    resp <- httr2::req_perform(req, path = temp_file)

    if (httr2::resp_status(resp) == 200) {
      message(paste("Success:", filename))
      raster_object <- terra::rast(temp_file)
      raster_list <- c(raster_list, raster_object)
      successful_dates <- c(successful_dates,as.character(as.Date(current_date)))
    }
  }

  if (length(raster_list) == 0) { stop("No valid ForDRI raster files were found in the specified date range.") }

  message(sprintf("Successfully downloaded %d rasters. Stacking...", length(raster_list)))

  # --- FIX #2: Use terra::rast() to correctly stack the list of rasters ---
  stacked_raster <- terra::rast(raster_list)
  names(stacked_raster) <- successful_dates

  return(stacked_raster)
}
