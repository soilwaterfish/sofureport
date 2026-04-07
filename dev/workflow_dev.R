library(sofureport)
library(RSQLite)
library(tidyverse)
library(sf)
library(data.table) # For the wrangle_to_hourly function
library(future)
plan(multisession(workers = 10))

# =============================================================================
# 1. SETUP AND DATABASE CONNECTION
# =============================================================================
db_path <- "dev/sofu.sqlite"
con <- dbConnect(SQLite(), db_path)

# =============================================================================
# 2. SITE DISCOVERY AND SPATIAL FILTERING
# =============================================================================
message("Fetching and filtering site locations...")

# Fetch site metadata from both APIs
mt_sd_fems_sites <- get_stations(state_id = c('MT', 'SD'))

mt_sd_synoptic_sites <- get_synoptic_stations(state = c('SD', 'MT'), complete = 1)

# Convert to spatial objects (sf)
mt_sd_synoptic_sites_sf <- mt_sd_synoptic_sites %>%
  st_as_sf(coords = c('LONGITUDE', 'LATITUDE'), crs = 4326) %>%
  mutate(api = 'Synoptic')

mt_sd_fems_sites_sf <- mt_sd_fems_sites %>%
  st_as_sf(coords = c('longitude', 'latitude'), crs = 4326) %>%
  mutate(api = 'FEMS')

# Spatially filter sites to the cg_bbox
cg_bbox <- read_sf('dev/cg_bbox.gpkg')
sites_together <- bind_rows(mt_sd_fems_sites_sf, mt_sd_synoptic_sites_sf) %>% st_as_sf()
sites_for_cg_indices <- st_intersects(sites_together, st_transform(cg_bbox, st_crs(sites_together)), sparse = FALSE)
sites_in_cg <- sites_together[sites_for_cg_indices[,1], ]

message(sprintf("Found %d total sites within the bounding box.", nrow(sites_in_cg)))

# --- NEW: Write the final sites list to the database ---
# Using st_write for spatial data. `delete_layer = TRUE` overwrites the table on each run.
st_write(sites_in_cg %>% select(-fuel_models, -state, -elevation), dsn = db_path, layer = "sites", delete_layer = TRUE)
message("Successfully wrote/updated 'sites' table in the database.")


# =============================================================================
# 3. INCREMENTAL DATA FETCH FOR SYNOPTIC AND FEMS
# =============================================================================

# Read the existing data from the database to find the last date
# Use dbExistsTable and a try-catch for the very first run
if (dbExistsTable(con, "synoptic_fems_data")) {
  old_synoptic_fems_data <- dbReadTable(con, "synoptic_fems_data")
  dbWriteTable(con, "synoptic_fems_data_copy", old_synoptic_fems_data, overwrite = TRUE)
  # Ensure date column is POSIXct
  old_synoptic_fems_data$date <- as.POSIXct(old_synoptic_fems_data$date, tz = "UTC")
  max_date_synoptic_fems <- old_synoptic_fems_data %>%
    filter(nfdr_type != 'F') %>%
    pull(date) %>%
    max(na.rm = TRUE)
} else {
  old_synoptic_fems_data <- tibble() # Start with an empty tibble
  max_date_synoptic_fems <- Sys.Date() - 30 # Fetch last 30 days on first run
}

message(sprintf("Fetching new Synoptic/FEMS data since: %s", max_date_synoptic_fems))

# Fetch new data
synoptic_data <- get_synoptic_timeseries_long(
  station_ids = sites_in_cg %>% filter(api == 'Synoptic') %>% pull(STID),
  start_time = max_date_synoptic_fems,
  end_time = Sys.time(),
  parallel = TRUE,
  chunk_by = '14 days'
)

fems_data <- get_nfdrs(
  station_ids = sites_in_cg %>% filter(api == 'FEMS') %>% pull(station_id),
  start_date = as.Date(max_date_synoptic_fems),
  end_date = Sys.Date() + 7,
  fuel_model = 'Y'
)

# --- Process and combine new data ---
synoptic_data_hourly <- synoptic_data %>%
  mutate(date_time = as_datetime(date_time)) %>%
  wrangle_to_hourly()

new_synoptic_fems_data <- synoptic_data_hourly %>%
  left_join(mt_sd_synoptic_sites %>% select(STID, WIMS_ID), by = c('station_id' = 'STID')) %>%
  full_join(fems_data %>% mutate(station_id = as.character(station_id)), by = c('WIMS_ID' = 'station_id', 'date' = 'display_hour'))

# --- NEW: Combine, De-duplicate, and Write to Database ---
# Combine old and new data
combined_synoptic_fems <- bind_rows(old_synoptic_fems_data, new_synoptic_fems_data)

# This is the crucial de-duplication step
# It keeps only the first unique row based on WIMS_ID and date
final_synoptic_fems <- combined_synoptic_fems %>%
  distinct(station_id, WIMS_ID, date, .keep_all = TRUE)

message(sprintf("Synoptic/FEMS: Combined %d old and %d new rows, resulting in %d unique rows.",
                nrow(old_synoptic_fems_data), nrow(new_synoptic_fems_data), nrow(final_synoptic_fems)))

# Write the complete, clean table back to the database, overwriting the old one
dbWriteTable(con, "synoptic_fems_data", final_synoptic_fems, overwrite = TRUE)
message("Successfully wrote/updated 'synoptic_fems_data' table in the database.")

# =============================================================================
# 4. INCREMENTAL DATA FETCH FOR ZENTRACLOUD
# =============================================================================

usfs_soil_sites <- c('z6-28071',
                     'z6-32392',
                     'z6-32393',
                     'z6-32483'
                     ,'z6-28073'
)

if (dbExistsTable(con, "zentracloud_data")) {
  old_zentra_data <- dbReadTable(con, "zentracloud_data") %>% mutate(date = as_datetime(date))

  dbWriteTable(con, "zentracloud_data_copy", old_zentra_data, overwrite = TRUE)

  max_date_zentra <- max(old_zentra_data$date, na.rm = TRUE)

} else {
  old_zentra_data <- tibble()
  max_date_zentra <- Sys.Date() - 30
}

new_zentra_data <- purrr::map(usfs_soil_sites,
                              safely(~get_zentracloud_v5_data(device_id = .,
                                                        start_datetime = as.character(max_date_zentra),
                                                        end_datetime = as.character(Sys.time())) %>% mutate(station_id = .x)))

new_zentra_data_bind <- bind_rows(purrr::map(new_zentra_data, 'result'))

new_zentra_data_bind <- new_zentra_data_bind %>%
                        select(-timestamp, -error_code) %>%
                        group_by(station_id) %>%
                        pivot_wider(names_from = c('port_num', 'sensor_name', 'unit', 'measurement'),
                                     values_from = c('value'),

                                     names_glue  = "port_{port_num}_{sensor_name}_{unit}_{measurement}") %>%
                        ungroup() %>%
                        janitor::clean_names() %>%
                        mutate(date_time = as_datetime(datetime, tz = "America/Denver")) %>%
                        select(-datetime) %>% wrangle_to_hourly(type = 'zentra')

combined_zentra <- bind_rows(old_zentra_data, new_zentra_data_bind)

final_zentra <- combined_zentra %>% distinct(station_id, date, .keep_all = TRUE)

dbWriteTable(con, "zentracloud_data", final_zentra, overwrite = TRUE)


# =============================================================================
# 5. DISCONNECT
# =============================================================================
dbDisconnect(con)
message("Script finished and database connection closed.")



# path <- r"{Z:\GIT\sofureport\dev\zentracloud_data\use_this}"
#
# files <- list.files(path)
#
# zentra_data <- map(files, ~read_csv(paste0(path, '\\', .x)) %>%
#                      mutate(station_id = sub(" .*", "", .x))) %>%
#   bind_rows() %>%
#   janitor::clean_names()
#
# zentra_data <- zentra_data %>%
#   mutate(datetime = mdy_hm(timestamp),
#     date_time = as_datetime(datetime, tz = "America/Denver")) %>%
#   select(-datetime, -timestamp) %>%
#   wrangle_to_hourly()
