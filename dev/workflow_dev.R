library(DBI)
library(RSQLite)
library(dplyr)
library(data.table)
library(sf)
library(future)
library(sofureport)

plan(multisession(workers = 25))

db_path <- "dev/sofu.sqlite"
con <- dbConnect(SQLite(), db_path)

on.exit(dbDisconnect(con), add = TRUE)

# Good SQLite pragmas for bulk work
dbExecute(con, "PRAGMA journal_mode = WAL;")
dbExecute(con, "PRAGMA synchronous = NORMAL;")
dbExecute(con, "PRAGMA temp_store = MEMORY;")
dbExecute(con, "PRAGMA cache_size = -200000;")  # about 200 MB cache

# -------------------------------------------------------------------
# 1. Site discovery
# -------------------------------------------------------------------
message("Fetching and filtering site locations...")

mt_sd_fems_sites <- get_stations(state_id = c("MT", "SD"))
mt_sd_synoptic_sites <- get_synoptic_stations(state = c("SD", "MT"), complete = 1)

mt_sd_synoptic_sites_sf <- mt_sd_synoptic_sites %>%
  st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326) %>%
  mutate(api = "Synoptic")

mt_sd_fems_sites_sf <- mt_sd_fems_sites %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  mutate(api = "FEMS")

cg_bbox <- sf::read_sf("dev/cg_bbox.shp")

sites_together <- bind_rows(mt_sd_fems_sites_sf, mt_sd_synoptic_sites_sf) %>%
  st_as_sf()

sites_for_cg_indices <- st_intersects(
  sites_together,
  st_transform(cg_bbox, st_crs(sites_together)),
  sparse = FALSE
)

sites_in_cg <- sites_together[sites_for_cg_indices[, 1], ]

message(sprintf("Found %d total sites within the bounding box.", nrow(sites_in_cg)))

# -------------------------------------------------------------------
# 2. Ensure target table exists
# -------------------------------------------------------------------
if (!dbExistsTable(con, "synoptic_fems_data")) {
  message("Creating synoptic_fems_data table on first run...")

  # Create an empty table from an empty template later if needed,
  # or create after first batch of new data.
  max_date_synoptic_fems <- Sys.time() - 30 * 24 * 3600
} else {
  max_date_synoptic_fems <- dbGetQuery(
    con,
    "
    SELECT MAX(date) AS max_date
    FROM synoptic_fems_data
    WHERE nfdr_type != 'F'
    "
  )$max_date

  max_date_synoptic_fems <- as.POSIXct(max_date_synoptic_fems, tz = "UTC")

  if (is.na(max_date_synoptic_fems)) {
    max_date_synoptic_fems <- Sys.time() - 30 * 24 * 3600
  }
}

message(sprintf("Fetching new Synoptic/FEMS data since: %s", max_date_synoptic_fems))

# -------------------------------------------------------------------
# 3. Fetch only new data
# -------------------------------------------------------------------
synoptic_data <- get_synoptic_timeseries_long(
  station_ids = sites_in_cg %>% filter(api == "Synoptic") %>% pull(STID),
  start_time = max_date_synoptic_fems,
  end_time = Sys.time(),
  parallel = TRUE,
  chunk_by = "14 days"
)

fems_data <- get_nfdrs(
  station_ids = sites_in_cg %>% filter(api == "FEMS") %>% pull(station_id),
  start_date = as.Date(max_date_synoptic_fems),
  end_date = Sys.Date() + 7,
  fuel_model = "Y"
)

plan(sequential)

synoptic_data_hourly <- synoptic_data %>%
  mutate(date_time = lubridate::as_datetime(date_time)) %>%
  wrangle_to_hourly(type = "synoptic")

new_synoptic_fems_data <- synoptic_data_hourly %>%
  left_join(
    mt_sd_synoptic_sites %>% select(STID, WIMS_ID),
    by = c("station_id" = "STID")
  ) %>%
  full_join(
    fems_data %>% mutate(station_id = as.character(station_id)),
    by = c("WIMS_ID" = "station_id", "date" = "display_hour")
  ) %>%
  distinct(station_id, WIMS_ID, date, .keep_all = TRUE)


db_cols <- dbListFields(con, "synoptic_fems_data")
new_cols <- names(new_synoptic_fems_data)

missing_in_new <- setdiff(db_cols, new_cols)

for (col in missing_in_new) {
  new_synoptic_fems_data[[col]] <- NA
}

new_synoptic_fems_data <- new_synoptic_fems_data[, intersect(names(new_synoptic_fems_data), db_cols), drop = FALSE]

# -------------------------------------------------------------------
# 4. First run: create table
# -------------------------------------------------------------------
if (!dbExistsTable(con, "synoptic_fems_data")) {
  dbWriteTable(con, "synoptic_fems_data", new_synoptic_fems_data, overwrite = TRUE)

  dbExecute(con, "
    CREATE UNIQUE INDEX IF NOT EXISTS idx_syn_fems_unique
    ON synoptic_fems_data (station_id, WIMS_ID, date)
  ")

  message(sprintf("Created synoptic_fems_data with %d rows.", nrow(new_synoptic_fems_data)))

} else if (nrow(new_synoptic_fems_data) > 0) {

  # -----------------------------------------------------------------
  # 5. Incremental load via staging table
  # -----------------------------------------------------------------

  dbWriteTable(con, "synoptic_fems_stage", new_synoptic_fems_data, overwrite = TRUE, temporary = TRUE)

  # Insert only rows not already present
  common_cols <- colnames(new_synoptic_fems_data)
  col_sql <- paste(DBI::dbQuoteIdentifier(con, common_cols), collapse = ", ")

  sql <- sprintf("
    INSERT OR IGNORE INTO synoptic_fems_data (%s)
    SELECT %s
    FROM synoptic_fems_stage
  ", col_sql, col_sql)

  dbWithTransaction(con, {
    dbExecute(con, sql)
  })

  message(sprintf("Appended up to %d new rows into synoptic_fems_data.", nrow(new_synoptic_fems_data)))

} else {
  message("No new rows to append.")
}


# copy file for backup

file.copy("dev/sofu.sqlite", paste0("dev/sofu_", Sys.Date(), ".sqlite"))


# =============================================================================
# 4. INCREMENTAL DATA FETCH FOR ZENTRACLOUD
# =============================================================================

usfs_soil_sites <- c(
  "z6-28071",
  "z6-32392",
  "z6-32393",
  "z6-32483",
  "z6-28073"
)

# ------------------------------------------------------------
# Get last date from SQLite instead of reading whole table
# ------------------------------------------------------------
if (dbExistsTable(con, "zentracloud_data")) {
  max_date_zentra <- dbGetQuery(
    con,
    "SELECT MAX(date) AS max_date FROM zentracloud_data"
  )$max_date

  max_date_zentra <- as.POSIXct(max_date_zentra, tz = "UTC")

  if (is.na(max_date_zentra)) {
    max_date_zentra <- Sys.time() - 30 * 24 * 3600
  }
} else {
  max_date_zentra <- Sys.time() - 30 * 24 * 3600
}

message(sprintf("Fetching new ZentraCloud data since: %s", max_date_zentra))

# ------------------------------------------------------------
# Pull only new data
# ------------------------------------------------------------
new_zentra_data <- purrr::map(
  usfs_soil_sites,
  purrr::safely(~ get_zentracloud_v5_data(
    device_id = .x,
    start_datetime = as.character(max_date_zentra),
    end_datetime   = as.character(Sys.time())
  ) %>%
    mutate(station_id = .x))
)

# Keep only successful pulls
new_zentra_data_bind <- bind_rows(purrr::map(new_zentra_data, "result"))

if (nrow(new_zentra_data_bind) > 0) {

  # ----------------------------------------------------------
  # Transform only the new batch
  # ----------------------------------------------------------
  new_zentra_data_bind <- new_zentra_data_bind %>%
    mutate(sensor_name = stringr::str_remove(sensor_name, ' G2')) %>%
    select(-any_of(c("timestamp", "error_code"))) %>%
    group_by(station_id) %>%
    tidyr::pivot_wider(
      names_from = c("port_num", "sensor_name", "unit", "measurement"),
      values_from = c("value"),
      names_glue = "port_{port_num}_{sensor_name}_{unit}_{measurement}"
    ) %>%
    ungroup() %>%
    janitor::clean_names() %>%
    mutate(date_time = lubridate::as_datetime(datetime, tz = "America/Denver")) %>%
    select(-any_of(c(
      "datetime",
      "port_1_atmos_41_f_min_air_temperature",
      "port_1_atmos_41_spoon_tips",
      "port_1_atmos_41_drop_counts",
      "port_1_atmos_41_f_max_air_temperature",
      "port_1_atmos_41_m_s_cm_ec",
      "port_1_atmos_41_tilt_angle",
      "port_3_unrecognized_sensor_output",
      "port_1_signal_strength_percent_signal"
    ))) %>%
    wrangle_to_hourly(type = "zentra") %>%
    distinct(station_id, date, .keep_all = TRUE)

  db_cols <- dbListFields(con, "zentracloud_data")
  new_cols <- names(new_zentra_data_bind)

  missing_in_new <- setdiff(db_cols, new_cols)

  for (col in missing_in_new) {
    new_zentra_data_bind[[col]] <- NA
  }

  new_zentra_data_bind <- new_zentra_data_bind[, intersect(names(new_zentra_data_bind), db_cols), drop = FALSE]

  # ----------------------------------------------------------
  # First run: create table + unique index
  # ----------------------------------------------------------
  if (!dbExistsTable(con, "zentracloud_data")) {
    dbWriteTable(con, "zentracloud_data", new_zentra_data_bind, overwrite = TRUE)

    dbExecute(con, "
      CREATE UNIQUE INDEX IF NOT EXISTS idx_zentra_unique
      ON zentracloud_data (station_id, date)
    ")

    dbExecute(con, "
      CREATE INDEX IF NOT EXISTS idx_zentra_date
      ON zentracloud_data (date)
    ")

    dbExecute(con, "
      CREATE INDEX IF NOT EXISTS idx_zentra_station
      ON zentracloud_data (station_id)
    ")

    message(sprintf(
      "Created 'zentracloud_data' with %d rows.",
      nrow(new_zentra_data_bind)
    ))

  } else {
    # --------------------------------------------------------
    # Incremental append via staging table
    # --------------------------------------------------------
    dbWriteTable(
      con,
      "zentracloud_stage",
      new_zentra_data_bind,
      overwrite = TRUE,
      temporary = TRUE
    )

    common_cols <- colnames(new_zentra_data_bind)
    col_sql <- paste(DBI::dbQuoteIdentifier(con, common_cols), collapse = ", ")

    sql <- sprintf("
      INSERT OR IGNORE INTO zentracloud_data (%s)
      SELECT %s
      FROM zentracloud_stage
    ", col_sql, col_sql)

    dbWithTransaction(con, {
      dbExecute(con, sql)
    })

    message(sprintf(
      "Attempted to append %d ZentraCloud rows into 'zentracloud_data'.",
      nrow(new_zentra_data_bind)
    ))
  }

} else {
  message("No new ZentraCloud rows returned.")
}




# copy file for backup

file.copy("dev/sofu.sqlite", paste0("dev/sofu_", Sys.Date(), ".sqlite"))

# =============================================================================
# 5. DISCONNECT
# =============================================================================
dbDisconnect(con)
message("Script finished and database connection closed.")

#
#
# library(tidyverse)
# path <- "dev/use_this"
#
# files <- list.files(path)
#
# zentra_data <- map(files, ~read_csv(paste0(path, '/', .x)) %>%
#                      mutate(station_id = sub(" .*", "", .x))%>%
#   rename_with(~str_remove(.x, ' G2')))  %>%
#   bind_rows() %>%
#   janitor::clean_names()
#
# zentra_data <- zentra_data %>%
#   mutate(datetime = mdy_hm(timestamp),
#     date_time = as_datetime(datetime, tz = "America/Denver")) %>%
#   select(-datetime, -timestamp, port_1_atmos_41_drop_counts:port_3_unrecognized_sensor_output) %>%
#   wrangle_to_hourly(type = 'zentra')
#  glimpse(zentra_data)
#
#  zentra_data %>% filter(!is.na(port_1_atmos_41_drop_counts
#                                )) %>% view()


# zentra_data <- zentra_data  %>%
#   select(-any_of(c(
#     "datetime",
#     "port_1_atmos_41_f_min_air_temperature",
#     "port_1_atmos_41_spoon_tips",
#     "port_1_atmos_41_drop_counts",
#     "port_1_atmos_41_f_max_air_temperature",
#     "port_1_atmos_41_m_s_cm_ec",
#     "port_1_atmos_41_tilt_angle",
#     "port_3_unrecognized_sensor_output",
#     "port_9_signal_strength_percent_signal"
#   )))

