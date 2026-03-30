# In R/core.R

#' Get the stored FEMS API Key
#' @noRd
get_fems_api_key <- function() {
  key <- Sys.getenv("FEMS_API_KEY")
  if (key == "") {
    stop("FEMS API key not found. Please set it using set_fems_api_key() or by adding FEMS_API_KEY to your .Renviron file.", call. = FALSE)
  }
  key
}


#' @noRd
fems_climatology_request <- function() {
  base_url <- "https://fems.fs2c.usda.gov/api/ext-climatology/graphql/"

  # --- THIS IS THE FINAL AUTHENTICATION FIX ---
  # It uses Basic Auth with the user's email and the FEMS token as the password.
  # We assume the user has set their email in an environment variable.
  user_email <- Sys.getenv("FEMS_EMAIL")
  if (user_email == "") {
    stop("FEMS user email not found. Please set FEMS_EMAIL in your .Renviron file.", call. = FALSE)
  }

  httr2::request(base_url) |>
    httr2::req_auth_basic(username = user_email, password = get_fems_api_key()) |>
    httr2::req_user_agent("sofor R package")
}

#' Base request builder for the Fuel Model GraphQL endpoint
#' @noRd
fems_fuelmodel_request <- function() {
  base_url <- "https://fems.fs2c.usda.gov/fuelmodel/apis/graphql"

  httr2::request(base_url) |>
    httr2::req_auth_bearer_token(token = get_fems_api_key()) |>
    httr2::req_user_agent("soforeport R package")
}

#' Get the stored Synoptic API Token
#' @noRd
get_synoptic_token <- function() {
  token <- Sys.getenv("SYNOPTIC_TOKEN")
  if (token == "") {
    stop("Synoptic API token not found. Please add SYNOPTIC_TOKEN to your .Renviron file.", call. = FALSE)
  }
  token
}

#' Base request builder for the Synoptic (MesoWest) API
#' @noRd
synoptic_api_request <- function(endpoint) {
  base_url <- "https://api.synopticdata.com/v2/"

  httr2::request(base_url) |>
    httr2::req_url_path_append(endpoint) |>
    # Add the token and units as default parameters to every request
    httr2::req_url_query(
      token = get_synoptic_token(),
      units = "english"
    ) |>
    httr2::req_user_agent("soforeport R package (https://github.com/soilwaterfish/soforeport)")
}

