library(DBI)
library(RSQLite)
library(dplyr)
library(data.table)
library(sf)
library(terra)
library(jsonlite)
#library(future)
library(sofureport)

load_live_sofureport_sources <- function() {
  candidate_dirs <- c(
    "/app/sofureport/R",
    file.path(getwd(), "sofureport", "R"),
    "sofureport/R"
  )
  source_dir <- candidate_dirs[file.exists(candidate_dirs)][1]

  if (is.na(source_dir) || !nzchar(source_dir)) {
    message("No live sofureport source directory found; using installed package only.")
    return(invisible(FALSE))
  }

  source_files <- list.files(source_dir, pattern = "\\.[Rr]$", full.names = TRUE)
  if (!length(source_files)) {
    message("No live sofureport R files found in ", source_dir, "; using installed package only.")
    return(invisible(FALSE))
  }

  # Source in a stable order so helpers/request builders exist before callers.
  source_files <- source_files[order(basename(source_files))]
  for (source_file in source_files) {
    source(source_file, local = FALSE)
  }

  message("Loaded live sofureport sources from: ", source_dir)
  invisible(TRUE)
}

load_live_sofureport_sources()

#plan(multisession(workers = 25))

db_path <- Sys.getenv("SOFU_DB_PATH", unset = "/data/sofu.sqlite")
gpkg_path <- Sys.getenv("SOFU_GPKG_PATH", unset = "/data/cg_bbox.gpkg")
output_dir <- Sys.getenv("SOFU_OUTPUT_DIR", unset = "/output")
renviron_path <- Sys.getenv("SOFU_RENVIRON_PATH", "/run/secrets/sofureport_renviron")
prism_annual_ppt_path <- Sys.getenv(
  "SOFU_PRISM_ANNUAL_PPT_PATH",
  unset = "/data/prism_ppt_us_30s_2020_avg_30y/prism_ppt_us_30s_2020_avg_30y.tif"
)
zone_csv_path <- Sys.getenv(
  "SOFU_ZONE_CSV_PATH",
  unset = file.path(dirname(db_path), "main_ExportTable_zones.csv")
)
local_today_boot <- as.Date(format(Sys.time(), tz = "America/Denver", usetz = FALSE))
water_year_start_boot <- as.Date(sprintf(
  "%d-10-01",
  ifelse(
    as.integer(format(local_today_boot, "%m")) >= 10L,
    as.integer(format(local_today_boot, "%Y")),
    as.integer(format(local_today_boot, "%Y")) - 1L
  )
))
env_flag <- function(name, default = "1") {
  value <- trimws(Sys.getenv(name, default))
  !tolower(value) %in% c("", "0", "false", "f", "no", "n", "off")
}

query_scalar_value <- function(df, column, default = NA) {
  if (is.null(df) || !is.data.frame(df) || !nrow(df) || !column %in% names(df)) {
    return(default)
  }

  values <- df[[column]]
  if (!length(values)) {
    return(default)
  }

  values[[1]]
}

force_water_year_backfill <- env_flag("SOFU_FORCE_WATER_YEAR_BACKFILL", "0")
default_zentra_backfill_days <- if (force_water_year_backfill) {
  as.integer(local_today_boot - water_year_start_boot) + 1L
} else {
  30L
}
zentra_backfill_days <- suppressWarnings(as.integer(Sys.getenv(
  "SOFU_ZENTRA_BACKFILL_DAYS",
  as.character(default_zentra_backfill_days)
)))
if (is.na(zentra_backfill_days) || zentra_backfill_days < 1) {
  zentra_backfill_days <- default_zentra_backfill_days
}
default_fems_backfill_days <- if (force_water_year_backfill) {
  as.integer(local_today_boot - water_year_start_boot) + 1L
} else {
  7L
}
fems_backfill_days <- suppressWarnings(as.integer(Sys.getenv(
  "SOFU_FEMS_BACKFILL_DAYS",
  as.character(default_fems_backfill_days)
)))
if (is.na(fems_backfill_days) || fems_backfill_days < 1) {
  fems_backfill_days <- default_fems_backfill_days
}
default_synoptic_backfill_days <- if (force_water_year_backfill) {
  as.integer(local_today_boot - water_year_start_boot) + 1L
} else {
  3L
}
synoptic_backfill_days <- suppressWarnings(as.integer(Sys.getenv(
  "SOFU_SYNOPTIC_BACKFILL_DAYS",
  as.character(default_synoptic_backfill_days)
)))
if (is.na(synoptic_backfill_days) || synoptic_backfill_days < 1) {
  synoptic_backfill_days <- default_synoptic_backfill_days
}
skip_computed_indices <- identical(Sys.getenv("SOFU_SKIP_COMPUTED_INDICES", "0"), "1")
skip_percentiles <- identical(Sys.getenv("SOFU_SKIP_PERCENTILES", "0"), "1")

run_stage_ingest <- env_flag("SOFU_STAGE_INGEST", "1")
run_stage_daily_stats <- env_flag("SOFU_STAGE_DAILY_STATS", "1")
run_stage_computed_indices <- env_flag(
  "SOFU_STAGE_COMPUTED_INDICES",
  if (skip_computed_indices) "0" else "1"
)
run_stage_percentiles <- env_flag(
  "SOFU_STAGE_PERCENTILES",
  if (skip_percentiles) "0" else "1"
)
only_establish_derived <- env_flag("SOFU_ONLY_ESTABLISH_DERIVED", "0")

daily_stats_lookback_days <- suppressWarnings(as.integer(Sys.getenv("SOFU_DAILY_STATS_LOOKBACK_DAYS", "14")))
if (is.na(daily_stats_lookback_days) || daily_stats_lookback_days < 1L) {
  daily_stats_lookback_days <- 14L
}

synoptic_stats_lookback_days <- suppressWarnings(as.integer(Sys.getenv(
  "SOFU_SYNOPTIC_STATS_LOOKBACK_DAYS",
  as.character(daily_stats_lookback_days)
)))
if (is.na(synoptic_stats_lookback_days) || synoptic_stats_lookback_days < 1L) {
  synoptic_stats_lookback_days <- daily_stats_lookback_days
}

fems_future_guard_hours <- suppressWarnings(as.numeric(Sys.getenv(
  "SOFU_FEMS_FUTURE_GUARD_HOURS",
  "6"
)))
if (!is.finite(fems_future_guard_hours) || fems_future_guard_hours < 0) {
  stop("SOFU_FEMS_FUTURE_GUARD_HOURS must be a non-negative number.", call. = FALSE)
}

# Apply the FEMS source-clock correction before creating canonical hourly keys.
# Serializers must use the stored canonical timestamp without another shift.
fems_clock_offset_hours <- suppressWarnings(as.numeric(Sys.getenv(
  "SOFU_FEMS_CLOCK_OFFSET_HOURS",
  "-6"
)))
if (!is.finite(fems_clock_offset_hours)) {
  stop("SOFU_FEMS_CLOCK_OFFSET_HOURS must be numeric.", call. = FALSE)
}

# A historical weather repair should not fan out into one NFDRS request per
# station and chunk. Existing NFDRS values remain untouched in that mode.
skip_fems_nfdrs <- env_flag("SOFU_FEMS_SKIP_NFDRS", "0")
# API-isolated source repairs must not consume Zentra's restricted request
# budget merely because they reuse the common workflow entry point.
skip_zentra_ingest <- env_flag("SOFU_SKIP_ZENTRA_INGEST", "0")

zentra_stats_lookback_days <- suppressWarnings(as.integer(Sys.getenv(
  "SOFU_ZENTRA_STATS_LOOKBACK_DAYS",
  as.character(max(daily_stats_lookback_days, 10L))
)))
if (is.na(zentra_stats_lookback_days) || zentra_stats_lookback_days < 1L) {
  zentra_stats_lookback_days <- max(daily_stats_lookback_days, 10L)
}

percentiles_lookback_days <- suppressWarnings(as.integer(Sys.getenv("SOFU_PERCENTILES_LOOKBACK_DAYS", "30")))
if (is.na(percentiles_lookback_days) || percentiles_lookback_days < 1L) {
  percentiles_lookback_days <- 30L
}

if (file.exists(renviron_path)) {
  readRenviron(renviron_path)
  cat("Loaded .Renviron from:", renviron_path, "\n")
} else {
  cat("No external .Renviron found at:", renviron_path, "\n")
}

required_vars <- c("SYNOPTIC_TOKEN", "FEMS_API_KEY", "ZENTRACLOUD_TOKEN")

missing_vars <- required_vars[Sys.getenv(required_vars) == ""]

if (length(missing_vars) > 0) {
  stop("Missing required environment variables: ", paste(missing_vars, collapse = ", "))
} else {
  cat("Required environment variables found:", paste(required_vars, collapse = ", "), "\n")
}

# check paths
cat("Starting workflow at:", as.character(Sys.time()), "\n")
cat("DB path:", db_path, "\n")
cat("GPKG path:", gpkg_path, "\n")
cat("Zone CSV path:", zone_csv_path, "\n")
cat("Output dir:", output_dir, "\n")
cat(
  "Workflow stages:",
  sprintf(
    "ingest=%s daily_stats=%s computed_indices=%s percentiles=%s",
    run_stage_ingest,
    run_stage_daily_stats,
    run_stage_computed_indices,
    run_stage_percentiles
  ),
  "\n"
)
cat(
  "Lookbacks:",
  sprintf(
    "synoptic_fetch=%s fems_fetch=%s zentra_fetch=%s synoptic_stats=%s zentra_stats=%s percentiles=%s skip_fems_nfdrs=%s force_wy_backfill=%s",
    synoptic_backfill_days,
    fems_backfill_days,
    zentra_backfill_days,
    synoptic_stats_lookback_days,
    zentra_stats_lookback_days,
    percentiles_lookback_days,
    skip_fems_nfdrs,
    force_water_year_backfill
  ),
  "\n"
)

if (!file.exists(db_path)) {
  stop("Database not found: ", db_path)
}

if (!file.exists(zone_csv_path)) {
  stop("Zone station CSV not found: ", zone_csv_path)
}

if (!file.exists(gpkg_path)) {
  warning("GPKG not found, continuing because site roster now comes from zone CSV: ", gpkg_path, call. = FALSE)
}

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
}
con <- dbConnect(SQLite(), db_path)

cat("Connected to database\n")
cat("dbIsValid(con): ", DBI::dbIsValid(con), "\n")
cat("dbGetInfo(con):\n")
print(DBI::dbGetInfo(con))

zentra_site_metadata <- tibble::tribble(
  ~station_id, ~station_name, ~latitude, ~longitude, ~source_note,
  "z6-28071", "Langhor", 45.53300001, -111.01502, "Inferred from nearby Forest Service Langohr Campground coordinates",
  "z6-28073", "Hebgen", 44.66597, -111.09911, "Matched to existing Forest Service Hebgen Lake site coordinates",
  "z6-32392", "W. Bridger Cr", 45.62737, -109.82784, "Inferred from Forest Service West Bridger Station coordinates",
  "z6-32393", "Whitetail", 45.640477, -105.976075, "Inferred from Forest Service Whitetail Cabin coordinates",
  "z6-32483", "Bear Palmer", 45.10979, -110.5998, "Approximate project-area point inferred from Forest Service Bear Creek Campground coordinates near Jardine"
) %>%
  mutate(
    api = "Zentra",
    station_key = station_id
  )

read_zone_site_roster <- function(path) {
  roster <- utils::read.csv(
    path,
    check.names = FALSE,
    stringsAsFactors = FALSE,
    fileEncoding = "UTF-8-BOM"
  )

  names(roster) <- sub("^\ufeff", "", names(roster))

  roster <- tibble::as_tibble(roster) %>%
    mutate(across(where(is.character), trimws))

  if (!all(c("station_id", "station_name", "api") %in% names(roster))) {
    stop("Zone station CSV must include station_id, station_name, and api columns.")
  }

  if (!"fems_fetch_id" %in% names(roster)) {
    roster$fems_fetch_id <- NA_character_
  }

  roster %>%
    mutate(
      station_id = as.character(.data$station_id),
      station_name = as.character(.data$station_name),
      api = as.character(.data$api),
      fems_fetch_id = dplyr::na_if(as.character(.data$fems_fetch_id), "")
    ) %>%
    distinct(.data$station_id, .data$api, .keep_all = TRUE)
}

read_station_source_mappings <- function(path) {
  if (!file.exists(path)) {
    return(tibble::tibble(
      station_id = character(),
      synoptic_stid = character(),
      overlay_fields = character()
    ))
  }

  mappings <- utils::read.csv(
    path,
    stringsAsFactors = FALSE,
    fileEncoding = "UTF-8-BOM"
  )
  names(mappings) <- sub("^\\ufeff", "", names(mappings))

  if (!all(c("station_id", "synoptic_stid", "overlay_fields") %in% names(mappings))) {
    stop(
      "Station source mappings must include station_id, synoptic_stid, and overlay_fields.",
      call. = FALSE
    )
  }

  tibble::as_tibble(mappings) %>%
    mutate(
      station_id = dplyr::na_if(trimws(as.character(.data$station_id)), ""),
      synoptic_stid = dplyr::na_if(trimws(as.character(.data$synoptic_stid)), ""),
      overlay_fields = dplyr::na_if(trimws(as.character(.data$overlay_fields)), "")
    ) %>%
    filter(!is.na(.data$station_id), !is.na(.data$synoptic_stid)) %>%
    distinct(.data$station_id, .keep_all = TRUE)
}

normalize_station_token <- function(x) {
  normalized <- trimws(as.character(x))
  normalized[is.na(normalized)] <- ""
  normalized <- toupper(normalized)
  gsub("[^A-Z0-9]+", "", normalized)
}

resolve_fems_station_ids <- function(roster) {
  fems_roster <- roster %>%
    filter(.data$api == "FEMS") %>%
    transmute(
      wims_id = as.character(.data$station_id),
      roster_station_name = as.character(.data$station_name),
      roster_latitude = suppressWarnings(as.numeric(.data$latitude)),
      roster_longitude = suppressWarnings(as.numeric(.data$longitude))
    ) %>%
    distinct(.data$wims_id, .keep_all = TRUE)

  if (!nrow(fems_roster)) {
    return(tibble::tibble(
      wims_id = character(),
      fems_fetch_id = character(),
      fems_station_id = character(),
      wrcc_id = character(),
      metadata_station_name = character(),
      match_source = character()
    ))
  }

  candidate_states <- if ("state" %in% names(roster)) {
    sort(unique(toupper(trimws(as.character(roster$state)))))
  } else {
    character()
  }
  candidate_states <- candidate_states[!is.na(candidate_states) & nzchar(candidate_states)]
  if (!length(candidate_states)) {
    candidate_states <- c("MT", "SD", "WY")
  }

  fems_metadata <- tryCatch(
    get_stations(
      state_id = candidate_states,
      has_historic_data = "ALL"
    ),
    error = function(err) {
      warning(
        sprintf("Unable to resolve numeric FEMS station IDs from station metadata: %s", conditionMessage(err)),
        call. = FALSE
      )
      tibble::tibble()
    }
  )

  if (!nrow(fems_metadata)) {
    return(fems_roster %>%
      mutate(
        fems_fetch_id = NA_character_,
        fems_station_id = NA_character_,
        wrcc_id = NA_character_,
        metadata_station_name = NA_character_,
        match_source = NA_character_
      ))
  }

  fems_metadata <- fems_metadata %>%
    mutate(
      station_id = as.character(.data$station_id),
      fems_station_id = as.character(.data$fems_station_id),
      wrcc_id = as.character(.data$wrcc_id),
      station_name = as.character(.data$station_name),
      latitude = suppressWarnings(as.numeric(.data$latitude)),
      longitude = suppressWarnings(as.numeric(.data$longitude)),
      wrcc_id_norm = normalize_station_token(.data$wrcc_id),
      station_name_norm = normalize_station_token(.data$station_name),
      fems_fetch_id = dplyr::coalesce(dplyr::na_if(.data$station_id, ""), dplyr::na_if(.data$fems_station_id, ""))
    ) %>%
    filter(!is.na(.data$fems_fetch_id), nzchar(.data$fems_fetch_id)) %>%
    distinct(.data$fems_fetch_id, .keep_all = TRUE)

  wrcc_matches <- fems_roster %>%
    mutate(wims_id_norm = normalize_station_token(.data$wims_id)) %>%
    left_join(
      fems_metadata %>%
        transmute(
          wrcc_id_norm = .data$wrcc_id_norm,
          fems_fetch_id = .data$fems_fetch_id,
          fems_station_id = .data$station_id,
          wrcc_id = .data$wrcc_id,
          metadata_station_name = .data$station_name,
          match_source = "wrcc_id"
        ),
      by = c("wims_id_norm" = "wrcc_id_norm")
    )

  unresolved <- wrcc_matches %>%
    filter(is.na(.data$fems_fetch_id) | !nzchar(.data$fems_fetch_id)) %>%
    select(.data$wims_id, .data$roster_station_name, .data$roster_latitude, .data$roster_longitude)

  if (nrow(unresolved)) {
    exact_name_matches <- unresolved %>%
      mutate(station_name_norm = normalize_station_token(.data$roster_station_name)) %>%
      left_join(
        fems_metadata %>%
          transmute(
            station_name_norm = .data$station_name_norm,
            fems_fetch_id = .data$fems_fetch_id,
            fems_station_id = .data$station_id,
            wrcc_id = .data$wrcc_id,
            metadata_station_name = .data$station_name,
            match_source = "station_name"
          ) %>%
          distinct(.data$station_name_norm, .keep_all = TRUE),
        by = "station_name_norm"
      ) %>%
      select(
        .data$wims_id,
        .data$roster_station_name,
        .data$roster_latitude,
        .data$roster_longitude,
        .data$fems_fetch_id,
        .data$fems_station_id,
        .data$wrcc_id,
        .data$metadata_station_name,
        .data$match_source
      )

    still_unresolved <- exact_name_matches %>%
      filter(is.na(.data$fems_fetch_id) | !nzchar(.data$fems_fetch_id)) %>%
      select(.data$wims_id, .data$roster_station_name, .data$roster_latitude, .data$roster_longitude)

    partial_name_matches <- if (nrow(still_unresolved)) {
      purrr::map_dfr(seq_len(nrow(still_unresolved)), function(idx) {
        target_name <- still_unresolved$roster_station_name[[idx]]
        target_norm <- normalize_station_token(target_name)
        target_lat <- suppressWarnings(as.numeric(still_unresolved$roster_latitude[[idx]]))
        target_lon <- suppressWarnings(as.numeric(still_unresolved$roster_longitude[[idx]]))

        if (!nzchar(target_norm)) {
          return(tibble::tibble())
        }

        candidates <- fems_metadata %>%
          filter(
            nzchar(.data$station_name_norm),
            grepl(target_norm, .data$station_name_norm, fixed = TRUE) |
              grepl(.data$station_name_norm, target_norm, fixed = TRUE)
          )

        if (nrow(candidates) > 1 && !is.na(target_lat) && !is.na(target_lon)) {
          candidates <- candidates %>%
            mutate(
              distance_sq = (.data$latitude - target_lat)^2 + (.data$longitude - target_lon)^2
            ) %>%
            filter(!is.na(.data$distance_sq)) %>%
            arrange(.data$distance_sq)
        }

        if (nrow(candidates) != 1) {
          return(tibble::tibble())
        }

        candidates %>%
          transmute(
            wims_id = still_unresolved$wims_id[[idx]],
            fems_fetch_id = as.character(.data$fems_fetch_id),
            fems_station_id = as.character(.data$station_id),
            wrcc_id = as.character(.data$wrcc_id),
            metadata_station_name = as.character(.data$station_name),
            match_source = "station_name_partial"
          )
      })
    } else {
      tibble::tibble(
        wims_id = character(),
        fems_fetch_id = character(),
        fems_station_id = character(),
        wrcc_id = character(),
        metadata_station_name = character(),
        match_source = character()
      )
    }

    name_matches <- exact_name_matches

    if (nrow(partial_name_matches)) {
      name_matches <- name_matches %>%
        left_join(
          partial_name_matches,
          by = "wims_id",
          suffix = c("", ".partial")
        ) %>%
        mutate(
          fems_fetch_id = dplyr::coalesce(.data$fems_fetch_id, .data$fems_fetch_id.partial),
          fems_station_id = dplyr::coalesce(.data$fems_station_id, .data$fems_station_id.partial),
          wrcc_id = dplyr::coalesce(.data$wrcc_id, .data$wrcc_id.partial),
          metadata_station_name = dplyr::coalesce(.data$metadata_station_name, .data$metadata_station_name.partial),
          match_source = dplyr::coalesce(.data$match_source, .data$match_source.partial)
        ) %>%
        select(
          .data$wims_id,
          .data$roster_station_name,
          .data$roster_latitude,
          .data$roster_longitude,
          .data$fems_fetch_id,
          .data$fems_station_id,
          .data$wrcc_id,
          .data$metadata_station_name,
          .data$match_source
        )
    }

    wrcc_matches <- wrcc_matches %>%
      select(-any_of(c("fems_fetch_id", "fems_station_id", "wrcc_id", "metadata_station_name", "match_source"))) %>%
      left_join(name_matches, by = "wims_id")
  }

  wrcc_matches %>%
    transmute(
      wims_id = as.character(.data$wims_id),
      fems_fetch_id = as.character(.data$fems_fetch_id),
      fems_station_id = as.character(.data$fems_station_id),
      wrcc_id = as.character(.data$wrcc_id),
      metadata_station_name = as.character(.data$metadata_station_name),
      match_source = as.character(.data$match_source)
    ) %>%
    distinct(.data$wims_id, .keep_all = TRUE)
}


# -------------------------------------------------------------------
# 1. Site discovery
# -------------------------------------------------------------------
message("Fetching and filtering site locations...")

zone_site_roster <- read_zone_site_roster(zone_csv_path)
source_mapping_path <- file.path(dirname(zone_csv_path), "station_source_mappings.csv")
station_source_mappings <- read_station_source_mappings(source_mapping_path)
leaflet_layer_values <- if ("leaflet_layer" %in% names(zone_site_roster)) {
  as.character(zone_site_roster$leaflet_layer)
} else {
  rep(NA_character_, nrow(zone_site_roster))
}
roster_wims_ids <- if ("wims_id" %in% names(zone_site_roster)) {
  dplyr::na_if(as.character(zone_site_roster$wims_id), "")
} else {
  rep(NA_character_, nrow(zone_site_roster))
}

zone_site_roster <- zone_site_roster %>%
  left_join(station_source_mappings, by = "station_id") %>%
  mutate(
    station_id = as.character(.data$station_id),
    station_name = as.character(.data$station_name),
    api = as.character(.data$api),
    station_key = as.character(.data$station_id),
    leaflet_layer = dplyr::coalesce(
      leaflet_layer_values,
      dplyr::case_when(
        .data$api == "FEMS" ~ "RAWS",
        .data$api == "Synoptic" ~ "Synoptic Soil",
        .data$api == "Zentra" ~ "Zentra",
        TRUE ~ .data$api
      )
    ),
    latitude = suppressWarnings(as.numeric(.data$latitude)),
    longitude = suppressWarnings(as.numeric(.data$longitude)),
    STID = dplyr::coalesce(
      .data$synoptic_stid,
      dplyr::if_else(.data$api == "Synoptic", .data$station_id, NA_character_)
    ),
    WIMS_ID = dplyr::coalesce(
      dplyr::if_else(.data$api == "FEMS", .data$station_id, NA_character_),
      roster_wims_ids
    )
  ) %>%
  distinct(.data$station_id, .data$api, .keep_all = TRUE)

roster_synoptic_ids <- zone_site_roster %>%
  filter(!is.na(.data$STID), nzchar(.data$STID)) %>%
  pull(.data$STID) %>%
  unique()
roster_synoptic_ids <- roster_synoptic_ids[!is.na(roster_synoptic_ids) & nzchar(roster_synoptic_ids)]

# Restrict only the Synoptic API request when repairing a known source gap.
# This never changes the active roster or the canonical station mapping.
synoptic_fetch_station_ids <- trimws(unlist(strsplit(
  Sys.getenv("SOFU_SYNOPTIC_STATION_IDS", ""),
  ",",
  fixed = TRUE
)))
synoptic_fetch_station_ids <- unique(synoptic_fetch_station_ids[nzchar(synoptic_fetch_station_ids)])
synoptic_targeted_repair <- length(synoptic_fetch_station_ids) > 0L
if (length(synoptic_fetch_station_ids)) {
  unknown_synoptic_ids <- setdiff(synoptic_fetch_station_ids, roster_synoptic_ids)
  if (length(unknown_synoptic_ids)) {
    stop(
      "SOFU_SYNOPTIC_STATION_IDS contains IDs without an active Synoptic mapping: ",
      paste(unknown_synoptic_ids, collapse = ", "),
      call. = FALSE
    )
  }
} else {
  synoptic_fetch_station_ids <- roster_synoptic_ids
}

roster_fems_ids <- zone_site_roster %>%
  filter(.data$api == "FEMS") %>%
  pull(.data$fems_fetch_id) %>%
  unique()
roster_fems_ids <- roster_fems_ids[!is.na(roster_fems_ids) & nzchar(roster_fems_ids)]

unresolved_fems_ids <- zone_site_roster %>%
  filter(.data$api == "FEMS", is.na(.data$fems_fetch_id) | !nzchar(.data$fems_fetch_id)) %>%
  pull(.data$station_id)

if (length(unresolved_fems_ids)) {
  warning(
    sprintf(
      "Zone roster is missing fems_fetch_id for %d RAWS station(s): %s",
      length(unresolved_fems_ids),
      paste(unresolved_fems_ids, collapse = ", ")
    ),
    call. = FALSE
  )
}

# The zone roster is the canonical station and WIMS mapping. Avoid a metadata
# API call during every ingest; it adds latency without changing the roster.
mt_sd_synoptic_sites <- tibble::tibble(STID = character(), WIMS_ID = character())

if (!"STID" %in% names(mt_sd_synoptic_sites)) {
  mt_sd_synoptic_sites$STID <- NA_character_
}
if (!"WIMS_ID" %in% names(mt_sd_synoptic_sites)) {
  mt_sd_synoptic_sites$WIMS_ID <- NA_character_
}

mt_sd_synoptic_sites <- mt_sd_synoptic_sites %>%
  mutate(
    STID = as.character(.data$STID),
    WIMS_ID = as.character(.data$WIMS_ID)
  )

synoptic_wims_lookup <- zone_site_roster %>%
  mutate(
    roster_wims_id = dplyr::coalesce(
      roster_wims_ids,
      dplyr::if_else(.data$api == "FEMS", .data$station_id, NA_character_)
    ),
    is_fems_soil_overlay = .data$api == "FEMS" & .data$overlay_fields == "soil_moisture"
  ) %>%
  filter(!is.na(.data$STID), nzchar(.data$STID)) %>%
  transmute(
    STID = as.character(.data$STID),
    canonical_station_id = as.character(.data$station_id),
    roster_wims_id = as.character(.data$roster_wims_id),
    is_fems_soil_overlay = .data$is_fems_soil_overlay
  ) %>%
  left_join(
    mt_sd_synoptic_sites %>%
      transmute(
        STID = as.character(.data$STID),
        metadata_wims_id = dplyr::na_if(as.character(.data$WIMS_ID), "")
      ),
    by = "STID"
  ) %>%
  transmute(
    STID = as.character(.data$STID),
    canonical_station_id = as.character(.data$canonical_station_id),
    WIMS_ID = dplyr::coalesce(.data$roster_wims_id, .data$metadata_wims_id),
    is_fems_soil_overlay = .data$is_fems_soil_overlay
  ) %>%
  distinct(.data$STID, .keep_all = TRUE)

sites_with_coords <- zone_site_roster %>%
  filter(!is.na(.data$latitude), !is.na(.data$longitude)) %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)

sites_without_coords <- zone_site_roster %>%
  filter(is.na(.data$latitude) | is.na(.data$longitude)) %>%
  mutate(geometry = sf::st_sfc(rep(list(sf::st_point()), dplyr::n()), crs = 4326)) %>%
  sf::st_as_sf()

sites_in_cg <- bind_rows(
  sites_with_coords,
  sites_without_coords
) %>%
  st_as_sf()

if (any(is.na(zone_site_roster$latitude) | is.na(zone_site_roster$longitude))) {
  warning(
    sprintf(
      "Zone roster contains %d station(s) without coordinates; they will be retained but unmapped.",
      sum(is.na(zone_site_roster$latitude) | is.na(zone_site_roster$longitude))
    ),
    call. = FALSE
  )
}

message(sprintf(
  "Selected %d total stations from zone roster (%d Synoptic, %d FEMS, %d Zentra).",
  nrow(sites_in_cg),
  sum(sites_in_cg$api == "Synoptic", na.rm = TRUE),
  sum(sites_in_cg$api == "FEMS", na.rm = TRUE),
  sum(sites_in_cg$api == "Zentra", na.rm = TRUE)
))

# -------------------------------------------------------------------
# 1b. Persist station metadata for downstream web mapping
# -------------------------------------------------------------------
sites_in_cg_coords <- matrix(NA_real_, nrow = nrow(sites_in_cg), ncol = 2)
colnames(sites_in_cg_coords) <- c("X", "Y")

has_geometry <- !sf::st_is_empty(sites_in_cg)

if (any(has_geometry, na.rm = TRUE)) {
  sites_in_cg_coords[has_geometry, ] <- sf::st_coordinates(sites_in_cg[has_geometry, ])
}

normalize_sqlite_names <- function(x) {
  normalized <- tolower(x)
  normalized <- gsub("[^a-z0-9]+", "_", normalized)
  normalized <- gsub("(^_+|_+$)", "", normalized)
  normalized[normalized == ""] <- "col"
  make.unique(normalized, sep = "_")
}

if (!"station_id" %in% names(sites_in_cg)) {
  sites_in_cg$station_id <- NA_character_
}
if (!"station_name" %in% names(sites_in_cg)) {
  sites_in_cg$station_name <- NA_character_
}
if (!"STID" %in% names(sites_in_cg)) {
  sites_in_cg$STID <- NA_character_
}

sites_in_cg_db <- sites_in_cg %>%
  mutate(station_id = as.character(.data$station_id)) %>%
  mutate(
    station_key = dplyr::coalesce(.data$station_id, .data$STID),
    station_label = dplyr::coalesce(.data$station_name, .data$STID, .data$station_id),
    longitude = dplyr::coalesce(.data$longitude, sites_in_cg_coords[, "X"]),
    latitude = dplyr::coalesce(.data$latitude, sites_in_cg_coords[, "Y"]),
    geometry_wkt = dplyr::if_else(
      has_geometry,
      sf::st_as_text(sf::st_geometry(.)),
      NA_character_
    )
  ) %>%
  sf::st_drop_geometry()

names(sites_in_cg_db) <- normalize_sqlite_names(names(sites_in_cg_db))

list_cols <- vapply(sites_in_cg_db, is.list, logical(1))

if (any(list_cols)) {
  sites_in_cg_db[list_cols] <- lapply(sites_in_cg_db[list_cols], function(col) {
    vapply(
      col,
      function(value) {
        if (is.null(value)) {
          return(NA_character_)
        }

        jsonlite::toJSON(value, auto_unbox = TRUE, null = "null")
      },
      character(1)
    )
  })
}

dbWriteTable(con, "sites_in_cg", sites_in_cg_db, overwrite = TRUE)

dbExecute(con, "
  CREATE INDEX IF NOT EXISTS idx_sites_in_cg_station_key
  ON sites_in_cg (station_key)
")

dbExecute(con, "
  CREATE INDEX IF NOT EXISTS idx_sites_in_cg_api
  ON sites_in_cg (api)
")

dedupe_table_on_keys <- function(con, table_name, key_cols) {
  if (!dbExistsTable(con, table_name)) {
    return(invisible(NULL))
  }

  table_fields <- DBI::dbListFields(con, table_name)
  if (!all(key_cols %in% table_fields)) {
    message(sprintf(
      "Skipping duplicate cleanup for %s because one or more key columns are missing.",
      table_name
    ))
    return(invisible(NULL))
  }

  quoted_keys <- paste(DBI::dbQuoteIdentifier(con, key_cols), collapse = ", ")
  dup_count <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT COUNT(*) AS dup_groups",
        "FROM (",
        "SELECT %s, COUNT(*) AS n",
        "FROM %s",
        "GROUP BY %s",
        "HAVING COUNT(*) > 1",
        ")"
      ),
      quoted_keys,
      DBI::dbQuoteIdentifier(con, table_name),
      quoted_keys
    )
  )

  if (!nrow(dup_count) || !"dup_groups" %in% names(dup_count)) {
    return(invisible(0L))
  }

  dup_count <- query_scalar_value(dup_count, "dup_groups", default = 0L)

  if (is.na(dup_count) || dup_count == 0) {
    return(invisible(0L))
  }

  DBI::dbExecute(
    con,
    sprintf(
      paste(
        "DELETE FROM %s",
        "WHERE rowid IN (",
        "SELECT rowid FROM (",
        "SELECT rowid,",
        "ROW_NUMBER() OVER (PARTITION BY %s ORDER BY rowid DESC) AS rn",
        "FROM %s",
        ")",
        "WHERE rn > 1",
        ")"
      ),
      DBI::dbQuoteIdentifier(con, table_name),
      quoted_keys,
      DBI::dbQuoteIdentifier(con, table_name)
    )
  )

  message(sprintf(
    "Removed duplicate rows from %s across %d duplicate key groups.",
    table_name,
    dup_count
  ))

  invisible(dup_count)
}

ensure_unique_index <- function(con, table_name, index_name, key_cols) {
  if (!dbExistsTable(con, table_name)) {
    return(invisible(NULL))
  }

  quoted_keys <- paste(DBI::dbQuoteIdentifier(con, key_cols), collapse = ", ")
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s)",
      DBI::dbQuoteIdentifier(con, index_name),
      DBI::dbQuoteIdentifier(con, table_name),
      quoted_keys
    )
  )

  invisible(TRUE)
}

ensure_index <- function(con, table_name, index_name, key_cols, unique = FALSE) {
  if (!dbExistsTable(con, table_name)) {
    return(invisible(NULL))
  }

  quoted_keys <- paste(DBI::dbQuoteIdentifier(con, key_cols), collapse = ", ")
  unique_sql <- if (isTRUE(unique)) "UNIQUE " else ""
  DBI::dbExecute(
    con,
    sprintf(
      "CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
      unique_sql,
      DBI::dbQuoteIdentifier(con, index_name),
      DBI::dbQuoteIdentifier(con, table_name),
      quoted_keys
    )
  )

  invisible(TRUE)
}

clamp01 <- function(x) {
  pmin(pmax(x, 0), 1)
}

fahrenheit_to_celsius <- function(x) {
  (as.numeric(x) - 32) * 5 / 9
}

vpd_kpa_from_temp_rh <- function(temp_f, rh_pct) {
  temp_c <- fahrenheit_to_celsius(temp_f)
  rh_frac <- as.numeric(rh_pct) / 100

  es <- 0.6108 * exp((17.27 * temp_c) / (temp_c + 237.3))
  es * (1 - rh_frac)
}

daylength_hours <- function(date_value, latitude) {
  doy <- as.POSIXlt(as.Date(date_value), tz = "UTC")$yday + 1
  lat_rad <- as.numeric(latitude) * pi / 180
  decl <- 0.409 * sin((2 * pi / 365) * doy - 1.39)
  ws_arg <- -tan(lat_rad) * tan(decl)
  ws_arg <- pmin(pmax(ws_arg, -1), 1)
  ws <- acos(ws_arg)
  24 / pi * ws
}

compute_gsi_value <- function(tmin_f, vpd_kpa, latitude, local_date, prcp_mm = NA_real_, use_precip_limiter = FALSE) {
  tmin_c <- fahrenheit_to_celsius(tmin_f)
  vpd_pa <- as.numeric(vpd_kpa) * 1000
  photo <- daylength_hours(local_date, latitude)
  prcp_mm <- as.numeric(prcp_mm)

  i_tmin <- clamp01((tmin_c - (-2)) / (5 - (-2)))
  i_vpd <- clamp01(1 - ((vpd_pa - 900) / (4100 - 900)))
  i_photo <- clamp01((photo - 10) / (11 - 10))
  i_prcp <- rep(1, length(prcp_mm))
  if (isTRUE(use_precip_limiter)) {
    has_prcp <- !is.na(prcp_mm)
    i_prcp[has_prcp] <- clamp01((prcp_mm[has_prcp] - 0) / (10 - 0))
  }

  gsi <- i_tmin * i_vpd * i_photo * i_prcp
  gsi[is.na(tmin_f) | is.na(vpd_kpa) | is.na(latitude) | is.na(local_date)] <- NA_real_
  gsi
}

build_synoptic_daily_precip <- function(con, raw_table, station_id_col = "station_id", station_ids = NULL, wims_col = NULL, only_non_wims = FALSE) {
  station_filter_sql <- if (!is.null(station_ids) && length(station_ids)) {
    sprintf(
      "AND %s IN (%s)",
      DBI::dbQuoteIdentifier(con, station_id_col),
      paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    )
  } else {
    ""
  }

  wims_filter_sql <- if (only_non_wims && !is.null(wims_col) && wims_col %in% DBI::dbListFields(con, raw_table)) {
    sprintf("AND (%s IS NULL OR TRIM(%s) = '')", DBI::dbQuoteIdentifier(con, wims_col), DBI::dbQuoteIdentifier(con, wims_col))
  } else {
    ""
  }

  precip_rows <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT CAST(%s AS TEXT) AS station_id,",
        "CAST(date(date, 'unixepoch', 'localtime') AS TEXT) AS local_date,",
        "MAX(%s) AS precip_accum_since_midnight,",
        "MAX(%s) AS precip_24h,",
        "MAX(%s) AS precip_accum",
        "FROM %s",
        "WHERE 1 = 1 %s %s",
        "GROUP BY 1, 2"
      ),
      DBI::dbQuoteIdentifier(con, station_id_col),
      DBI::dbQuoteIdentifier(con, "precip_accum_since_local_midnight_set_1.mean"),
      DBI::dbQuoteIdentifier(con, "precip_accum_24_hour_set_1.mean"),
      DBI::dbQuoteIdentifier(con, "precip_accum_set_1.mean"),
      raw_table,
      station_filter_sql,
      wims_filter_sql
    )
  )

  if (!nrow(precip_rows)) {
    return(tibble::tibble(
      station_id = character(),
      local_date = as.Date(character()),
      precip_in = numeric(),
      has_precip_support = logical()
    ))
  }

  precip_rows %>%
    mutate(
      station_id = as.character(.data$station_id),
      local_date = as.Date(.data$local_date),
      precip_accum_since_midnight = as.numeric(.data$precip_accum_since_midnight),
      precip_24h = as.numeric(.data$precip_24h),
      precip_accum = as.numeric(.data$precip_accum)
    ) %>%
    arrange(.data$station_id, .data$local_date) %>%
    group_by(.data$station_id) %>%
    mutate(
      precip_accum_delta = dplyr::if_else(
        !is.na(.data$precip_accum) & !is.na(dplyr::lag(.data$precip_accum)),
        pmax(.data$precip_accum - dplyr::lag(.data$precip_accum), 0),
        NA_real_
      ),
      precip_in = dplyr::coalesce(.data$precip_accum_since_midnight, .data$precip_24h, .data$precip_accum_delta),
      has_precip_support = !is.na(.data$precip_accum_since_midnight) | !is.na(.data$precip_24h) | !is.na(.data$precip_accum)
    ) %>%
    ungroup() %>%
    select(.data$station_id, .data$local_date, .data$precip_in, .data$has_precip_support)
}

rolling_mean_right <- function(x, n = 21L) {
  x <- as.numeric(x)
  if (!length(x)) {
    return(x)
  }

  vapply(seq_along(x), function(idx) {
    start_idx <- max(1L, idx - n + 1L)
    window_vals <- x[start_idx:idx]
    if (all(is.na(window_vals))) {
      return(NA_real_)
    }
    mean(window_vals, na.rm = TRUE)
  }, numeric(1))
}

rolling_sum_right <- function(x, n = 28L) {
  x <- as.numeric(x)
  if (!length(x)) {
    return(x)
  }

  vapply(seq_along(x), function(idx) {
    start_idx <- max(1L, idx - n + 1L)
    window_vals <- x[start_idx:idx]
    if (all(is.na(window_vals))) {
      return(NA_real_)
    }
    sum(window_vals, na.rm = TRUE)
  }, numeric(1))
}

compute_kbdi_series <- function(local_date, tmax_f, precip_in, annual_ppt_in) {
  if (!length(local_date) || all(is.na(annual_ppt_in))) {
    return(rep(NA_real_, length(local_date)))
  }

  local_date <- as.Date(local_date)
  tmax_f <- as.numeric(tmax_f)
  precip_in <- as.numeric(precip_in)
  annual_ppt_in <- as.numeric(annual_ppt_in)

  if (!length(tmax_f) || !length(precip_in) || !length(annual_ppt_in)) {
    return(rep(NA_real_, length(local_date)))
  }

  if (length(tmax_f) != length(local_date) ||
      length(precip_in) != length(local_date) ||
      length(annual_ppt_in) != length(local_date)) {
    warning(
      sprintf(
        "KBDI input length mismatch: local_date=%d tmax=%d precip=%d annual_ppt=%d. Returning NA series.",
        length(local_date),
        length(tmax_f),
        length(precip_in),
        length(annual_ppt_in)
      ),
      call. = FALSE
    )
    return(rep(NA_real_, length(local_date)))
  }

  order_idx <- order(local_date)
  kbdi_sorted <- rep(NA_real_, length(local_date))

  prev_kbdi <- 0
  ordered_ppt <- annual_ppt_in[order_idx]
  if (!length(ordered_ppt)) {
    return(rep(NA_real_, length(local_date)))
  }
  map_in <- ordered_ppt[[1]]

  for (idx in seq_along(order_idx)) {
    row_idx <- order_idx[[idx]]
    rainfall_in <- precip_in[[row_idx]]
    rainfall_in <- ifelse(is.na(rainfall_in), 0, rainfall_in)

    current_kbdi <- prev_kbdi
    if (rainfall_in > 0.2) {
      current_kbdi <- max(prev_kbdi - 100 * (rainfall_in - 0.2), 0)
    }

    tmax_val <- tmax_f[[row_idx]]
    drought_factor <- 0

    if (!is.na(tmax_val) && !is.na(map_in)) {
      drought_factor <- (
        (800 - current_kbdi) *
          (0.968 * exp(0.0486 * tmax_val) - 8.30) *
          0.001
      ) / (1 + 10.88 * exp(-0.0441 * map_in))

      drought_factor <- max(drought_factor, 0)
    }

    current_kbdi <- min(max(current_kbdi + drought_factor, 0), 800)
    kbdi_sorted[[idx]] <- current_kbdi
    prev_kbdi <- current_kbdi
  }

  kbdi <- rep(NA_real_, length(local_date))
  kbdi[order_idx] <- kbdi_sorted
  kbdi
}

extract_station_annual_ppt <- function(station_lookup, raster_path) {
  if (!file.exists(raster_path)) {
    message(sprintf("Skipping computed KBDI because annual PRISM raster is missing: %s", raster_path))
    return(tibble::tibble(
      station_id = character(),
      station_key = character(),
      stid = character(),
      wims_id = character(),
      latitude = numeric(),
      longitude = numeric(),
      annual_ppt_in = numeric()
    ))
  }

  lookup_sf <- station_lookup

  if ("STID" %in% names(lookup_sf) && !"stid" %in% names(lookup_sf)) {
    lookup_sf$stid <- lookup_sf$STID
  }
  if ("WIMS_ID" %in% names(lookup_sf) && !"wims_id" %in% names(lookup_sf)) {
    lookup_sf$wims_id <- lookup_sf$WIMS_ID
  }

  coords <- matrix(NA_real_, nrow = nrow(lookup_sf), ncol = 2)
  colnames(coords) <- c("X", "Y")
  has_geom <- !sf::st_is_empty(lookup_sf)
  if (any(has_geom, na.rm = TRUE)) {
    coords[has_geom, ] <- sf::st_coordinates(lookup_sf[has_geom, ])
  }

  lookup_sf <- lookup_sf %>%
    mutate(
      longitude = dplyr::coalesce(suppressWarnings(as.numeric(.data$longitude)), coords[, "X"]),
      latitude = dplyr::coalesce(suppressWarnings(as.numeric(.data$latitude)), coords[, "Y"])
    ) %>%
    st_drop_geometry()

  for (lookup_col in c("station_id", "station_key", "stid", "wims_id", "latitude", "longitude")) {
    if (!lookup_col %in% names(lookup_sf)) {
      lookup_sf[[lookup_col]] <- NA
    }
  }

  lookup_sf <- lookup_sf %>%
    mutate(
      station_id = as.character(.data$station_id),
      station_key = as.character(.data$station_key),
      stid = as.character(.data$stid),
      wims_id = as.character(.data$wims_id),
      latitude = as.numeric(.data$latitude),
      longitude = as.numeric(.data$longitude)
    )

  lookup_sf <- lookup_sf %>%
    filter(!is.na(.data$latitude), !is.na(.data$longitude)) %>%
    distinct(coalesce(.data$stid, .data$station_id, .data$station_key), .keep_all = TRUE)

  if (!nrow(lookup_sf)) {
    return(tibble::tibble(
      station_id = character(),
      station_key = character(),
      stid = character(),
      wims_id = character(),
      latitude = numeric(),
      longitude = numeric(),
      annual_ppt_in = numeric()
    ))
  }

  prism_rast <- terra::rast(raster_path)
  points_vect <- terra::vect(
    sf::st_as_sf(
      lookup_sf %>% st_drop_geometry(),
      coords = c("longitude", "latitude"),
      crs = 4326
    )
  )
  extracted <- terra::extract(prism_rast, points_vect)
  extracted_value_col <- setdiff(names(extracted), "ID")
  annual_ppt_mm <- if (length(extracted_value_col)) {
    as.numeric(extracted[[extracted_value_col[[1]]]])
  } else if (ncol(extracted) >= 2L) {
    as.numeric(extracted[[2]])
  } else {
    warning(
      sprintf(
        "PRISM extract from %s returned no raster value column; annual_ppt_in will be NA for these stations.",
        raster_path
      ),
      call. = FALSE
    )
    rep(NA_real_, nrow(lookup_sf))
  }

  lookup_sf %>%
    st_drop_geometry() %>%
    mutate(
      annual_ppt_mm = annual_ppt_mm,
      annual_ppt_in = .data$annual_ppt_mm / 25.4
    ) %>%
    select(
      .data$station_id,
      .data$station_key,
      .data$stid,
      .data$wims_id,
      .data$latitude,
      .data$longitude,
      .data$annual_ppt_in
    )
}

append_computed_gsi_stats <- function(
  con,
  raw_table,
  stats_table,
  source_name,
  temp_col,
  station_lookup,
  station_id_col = "station_id",
  wims_col = NULL,
  only_non_wims = FALSE,
  rh_col = NULL,
  vpd_col = NULL,
  timezone = "America/Denver"
) {
  delete_source_rows <- function(station_ids) {
    station_ids <- unique(as.character(station_ids))
    station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
    if (!length(station_ids)) {
      return(invisible(NULL))
    }

    station_filter <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    DBI::dbExecute(
      con,
      sprintf(
        paste(
          "DELETE FROM %s",
          "WHERE metric_name = 'gsi'",
          "AND source = %s",
          "AND CAST(station_id AS TEXT) IN (%s)"
        ),
        DBI::dbQuoteIdentifier(con, stats_table),
        DBI::dbQuoteString(con, paste0(source_name, " computed")),
        station_filter
      )
    )
  }

  if (!dbExistsTable(con, raw_table) || !dbExistsTable(con, stats_table)) {
    message(sprintf("Skipping computed GSI for %s because one or more tables are missing.", stats_table))
    return(invisible(NULL))
  }

  if (is.null(vpd_col) && is.null(rh_col)) {
    message(sprintf("Skipping computed GSI for %s because neither VPD nor RH inputs were provided.", stats_table))
    return(invisible(NULL))
  }

  temp_stats <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id, local_date,",
        "value_min AS tmin_f, value_max AS tmax_f",
        "FROM %s",
        "WHERE metric_name = %s"
      ),
      stats_table,
      DBI::dbQuoteString(con, temp_col)
    )
  )

  if (!nrow(temp_stats)) {
    message(sprintf("Skipping computed GSI for %s because no temperature daily stats were available.", stats_table))
    return(invisible(NULL))
  }

  temp_stats <- temp_stats %>%
    mutate(
      station_id = as.character(.data$station_id),
      local_date = as.Date(as.numeric(.data$local_date), origin = "1970-01-01"),
      tmin_f = as.numeric(.data$tmin_f),
      tmax_f = as.numeric(.data$tmax_f)
    )

  rh_stats <- NULL
  vpd_stats <- NULL
  if (!is.null(rh_col)) {
    rh_stats <- DBI::dbGetQuery(
      con,
      sprintf(
        paste(
          "SELECT CAST(station_id AS TEXT) AS station_id, local_date,",
          "value_min AS rh_min, value_max AS rh_max",
          "FROM %s",
          "WHERE metric_name = %s"
        ),
        stats_table,
        DBI::dbQuoteString(con, rh_col)
      )
    )

    if (nrow(rh_stats)) {
      rh_stats <- rh_stats %>%
        mutate(
          station_id = as.character(.data$station_id),
          local_date = as.Date(as.numeric(.data$local_date), origin = "1970-01-01"),
          rh_min = as.numeric(.data$rh_min),
          rh_max = as.numeric(.data$rh_max)
        )
    }
  }

  if (!is.null(vpd_col)) {
    vpd_stats <- DBI::dbGetQuery(
      con,
      sprintf(
        paste(
          "SELECT CAST(station_id AS TEXT) AS station_id, local_date,",
          "value_max AS vpd_kpa",
          "FROM %s",
          "WHERE metric_name = %s"
        ),
        stats_table,
        DBI::dbQuoteString(con, vpd_col)
      )
    )

    if (nrow(vpd_stats)) {
      vpd_stats <- vpd_stats %>%
        mutate(
          station_id = as.character(.data$station_id),
          local_date = as.Date(as.numeric(.data$local_date), origin = "1970-01-01"),
          vpd_kpa = as.numeric(.data$vpd_kpa)
        )
    }
  }

  daily_precip <- if (identical(raw_table, "zentracloud_data")) {
    precip_rows <- DBI::dbGetQuery(
      con,
      sprintf(
        paste(
          "SELECT CAST(%s AS TEXT) AS station_id,",
          "CAST(date(date, 'unixepoch', 'localtime') AS TEXT) AS local_date,",
          "SUM(COALESCE(%s, 0)) AS precip_in",
          "FROM %s",
          "GROUP BY 1, 2"
        ),
        DBI::dbQuoteIdentifier(con, station_id_col),
        DBI::dbQuoteIdentifier(con, "port_1_atmos_41_in_precipitation"),
        raw_table
      )
    )

    precip_rows %>%
      mutate(
        station_id = as.character(.data$station_id),
        local_date = as.Date(.data$local_date),
        precip_in = as.numeric(.data$precip_in)
      )
  } else {
    build_synoptic_daily_precip(
      con = con,
      raw_table = raw_table,
      station_id_col = station_id_col,
      wims_col = wims_col,
      only_non_wims = only_non_wims
    ) %>%
      select(.data$station_id, .data$local_date, .data$precip_in, .data$has_precip_support)
  }

  station_lookup_clean <- station_lookup

  if ("STID" %in% names(station_lookup_clean) && !"stid" %in% names(station_lookup_clean)) {
    station_lookup_clean$stid <- station_lookup_clean$STID
  }
  if ("WIMS_ID" %in% names(station_lookup_clean) && !"wims_id" %in% names(station_lookup_clean)) {
    station_lookup_clean$wims_id <- station_lookup_clean$WIMS_ID
  }

  coords <- matrix(NA_real_, nrow = nrow(station_lookup_clean), ncol = 2)
  colnames(coords) <- c("X", "Y")
  has_geom <- !sf::st_is_empty(station_lookup_clean)
  if (any(has_geom, na.rm = TRUE)) {
    coords[has_geom, ] <- sf::st_coordinates(station_lookup_clean[has_geom, ])
  }

  for (lookup_col in c("station_key", "station_id", "stid", "wims_id", "latitude")) {
    if (!lookup_col %in% names(station_lookup_clean)) {
      station_lookup_clean[[lookup_col]] <- NA
    }
  }

  station_lookup_clean <- station_lookup_clean %>%
    mutate(
      longitude = dplyr::coalesce(suppressWarnings(as.numeric(.data$longitude)), coords[, "X"]),
      latitude = dplyr::coalesce(suppressWarnings(as.numeric(.data$latitude)), coords[, "Y"]),
      station_key = as.character(.data$station_key),
      station_id = as.character(.data$station_id),
      stid = as.character(.data$stid),
      wims_id = as.character(.data$wims_id),
      latitude = as.numeric(.data$latitude)
    ) %>%
    st_drop_geometry()

  lookup_by_station <- station_lookup_clean %>%
    transmute(
      station_id_join = coalesce(.data$stid, .data$station_id, .data$station_key),
      latitude_station = as.numeric(.data$latitude)
    ) %>%
    filter(!is.na(.data$station_id_join), !is.na(.data$latitude_station)) %>%
    distinct(.data$station_id_join, .keep_all = TRUE)

  lookup_by_wims <- station_lookup_clean %>%
    transmute(
      wims_id_join = coalesce(.data$wims_id, .data$station_key),
      latitude_wims = as.numeric(.data$latitude)
    ) %>%
    filter(!is.na(.data$wims_id_join), nzchar(.data$wims_id_join), !is.na(.data$latitude_wims)) %>%
    distinct(.data$wims_id_join, .keep_all = TRUE)

  eligible_station_ids <- if (only_non_wims) {
    station_lookup_clean %>%
      filter(is.na(.data$wims_id) | !nzchar(trimws(.data$wims_id))) %>%
      transmute(station_id_join = coalesce(.data$stid, .data$station_id, .data$station_key)) %>%
      filter(!is.na(.data$station_id_join), nzchar(.data$station_id_join)) %>%
      distinct(.data$station_id_join) %>%
      pull(.data$station_id_join)
  } else {
    unique(temp_stats$station_id)
  }

  delete_source_rows(eligible_station_ids)

  gsi_input <- temp_stats
  if (!is.null(rh_stats) && nrow(rh_stats)) {
    gsi_input <- gsi_input %>%
      left_join(rh_stats, by = c("station_id", "local_date"))
  }
  if (!is.null(vpd_stats) && nrow(vpd_stats)) {
    gsi_input <- gsi_input %>%
      left_join(vpd_stats, by = c("station_id", "local_date"))
  }

  gsi_input <- gsi_input %>%
    left_join(daily_precip, by = c("station_id", "local_date")) %>%
    left_join(lookup_by_station, by = c("station_id" = "station_id_join")) %>%
    mutate(
      latitude = .data$latitude_station,
      precip_mm = as.numeric(.data$precip_in) * 25.4
    )

  if ("vpd_kpa" %in% names(gsi_input) && "rh_min" %in% names(gsi_input)) {
    gsi_input <- gsi_input %>%
      mutate(
        vpd_kpa = dplyr::coalesce(.data$vpd_kpa, vpd_kpa_from_temp_rh(.data$tmax_f, .data$rh_min))
      )
  } else if ("vpd_kpa" %in% names(gsi_input)) {
    gsi_input <- gsi_input %>%
      mutate(
        vpd_kpa = as.numeric(.data$vpd_kpa)
      )
  } else if ("rh_min" %in% names(gsi_input)) {
    gsi_input <- gsi_input %>%
      mutate(
        vpd_kpa = vpd_kpa_from_temp_rh(.data$tmax_f, .data$rh_min)
      )
  } else {
    message(sprintf("Skipping computed GSI for %s because neither daily RH nor VPD inputs were available after joins.", raw_table))
    return(invisible(NULL))
  }

  gsi_input <- gsi_input %>%
    filter(!is.na(.data$latitude))

  if (!is.null(eligible_station_ids)) {
    gsi_input <- gsi_input %>%
      filter(.data$station_id %in% eligible_station_ids)
  }

  if (!nrow(gsi_input)) {
    message(sprintf("No computed GSI rows could be generated for %s.", raw_table))
    return(invisible(NULL))
  }

  gsi_input <- gsi_input %>%
    arrange(.data$station_id, .data$local_date) %>%
    group_by(.data$station_id) %>%
    arrange(.data$local_date, .by_group = TRUE) %>%
    mutate(
      precip_mm_28d = rolling_sum_right(.data$precip_mm, n = 28L),
      igsi = compute_gsi_value(
        .data$tmin_f,
        .data$vpd_kpa,
        .data$latitude,
        .data$local_date,
        .data$precip_mm_28d,
        use_precip_limiter = FALSE
      ),
      gsi = rolling_mean_right(.data$igsi, n = 28L)
    ) %>%
    ungroup() %>%
    filter(!is.na(.data$gsi))

  gsi_stats <- gsi_input %>%
    transmute(
      station_id = .data$station_id,
      local_date = .data$local_date,
      metric_name = "gsi",
      value_min = .data$gsi,
      value_max = .data$gsi,
      value_mean = .data$gsi,
      n_obs = 1L,
      first_timestamp_utc = as.numeric(as.POSIXct(.data$local_date, tz = timezone)),
      last_timestamp_utc = as.numeric(as.POSIXct(.data$local_date, tz = timezone)),
      source = paste0(source_name, " computed"),
      is_complete_day = .data$local_date < Sys.Date(),
      updated_at_utc = as.numeric(Sys.time())
    ) %>%
    arrange(.data$station_id, .data$local_date, desc(.data$last_timestamp_utc)) %>%
    distinct(.data$station_id, .data$local_date, .data$metric_name, .keep_all = TRUE)

  if (!nrow(gsi_stats)) {
    message(sprintf("No computed GSI rows remained after deduping for %s.", stats_table))
    return(invisible(NULL))
  }

  DBI::dbWriteTable(con, stats_table, gsi_stats, append = TRUE)
  dedupe_table_on_keys(con, stats_table, c("station_id", "local_date", "metric_name"))

  message(sprintf(
    "Appended %d computed GSI rows into %s.",
    nrow(gsi_stats),
    stats_table
  ))

  invisible(gsi_stats)
}

append_computed_kbdi_stats <- function(
  con,
  raw_table,
  stats_table,
  source_name,
  temp_col,
  station_lookup,
  prism_raster_path,
  station_id_col = "station_id",
  wims_col = NULL,
  only_non_wims = FALSE,
  precip_mode = c("synoptic", "zentra"),
  timezone = "America/Denver"
) {
  precip_mode <- match.arg(precip_mode)

  delete_source_rows <- function(station_ids) {
    station_ids <- unique(as.character(station_ids))
    station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
    if (!length(station_ids)) {
      return(invisible(NULL))
    }

    station_filter <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    DBI::dbExecute(
      con,
      sprintf(
        paste(
          "DELETE FROM %s",
          "WHERE metric_name = 'kbdi'",
          "AND source = %s",
          "AND CAST(station_id AS TEXT) IN (%s)"
        ),
        DBI::dbQuoteIdentifier(con, stats_table),
        DBI::dbQuoteString(con, paste0(source_name, " computed")),
        station_filter
      )
    )
  }

  if (!dbExistsTable(con, raw_table) || !dbExistsTable(con, stats_table)) {
    message(sprintf("Skipping computed KBDI for %s because one or more tables are missing.", stats_table))
    return(invisible(NULL))
  }

  station_meta <- extract_station_annual_ppt(station_lookup, prism_raster_path)
  if (!nrow(station_meta)) {
    message(sprintf("Skipping computed KBDI for %s because no station precipitation normals were available.", stats_table))
    return(invisible(NULL))
  }

  if (only_non_wims) {
    station_meta <- station_meta %>%
      filter(is.na(.data$wims_id) | !nzchar(trimws(.data$wims_id)))
  }

  if (!nrow(station_meta)) {
    message(sprintf("Skipping computed KBDI for %s because no eligible non-WIMS stations were found.", stats_table))
    return(invisible(NULL))
  }

  eligible_station_ids <- unique(dplyr::coalesce(
    station_meta$stid,
    station_meta$station_id,
    station_meta$station_key
  ))
  eligible_station_ids <- eligible_station_ids[!is.na(eligible_station_ids) & nzchar(eligible_station_ids)]
  delete_source_rows(eligible_station_ids)

  tmax_rows <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id, local_date, value_max AS tmax_f",
        "FROM %s",
        "WHERE metric_name = %s"
      ),
      stats_table,
      DBI::dbQuoteString(con, temp_col)
    )
  )

  if (!nrow(tmax_rows)) {
    message(sprintf("Skipping computed KBDI for %s because no Tmax daily stats are available.", stats_table))
    return(invisible(NULL))
  }

  tmax_rows <- tmax_rows %>%
    mutate(
      station_id = as.character(.data$station_id),
      local_date = as.Date(as.numeric(.data$local_date), origin = "1970-01-01"),
      tmax_f = as.numeric(.data$tmax_f)
    )

  if (precip_mode == "synoptic") {
    station_ids_for_filter <- unique(dplyr::coalesce(
      station_meta$stid,
      station_meta$station_id,
      station_meta$station_key
    ))
    station_ids_for_filter <- station_ids_for_filter[!is.na(station_ids_for_filter) & nzchar(station_ids_for_filter)]

    if (!length(station_ids_for_filter)) {
      message(sprintf("Skipping computed KBDI for %s because no station ids were available after metadata matching.", stats_table))
      return(invisible(NULL))
    }
    precip_rows <- build_synoptic_daily_precip(
      con = con,
      raw_table = raw_table,
      station_id_col = station_id_col,
      station_ids = station_ids_for_filter,
      wims_col = wims_col,
      only_non_wims = only_non_wims
    )
  } else {
    precip_query <- paste(
      "SELECT",
      sprintf("CAST(%s AS TEXT) AS station_id,", DBI::dbQuoteIdentifier(con, station_id_col)),
      "CAST(date(date, 'unixepoch', 'localtime') AS TEXT) AS local_date,",
      sprintf("SUM(COALESCE(%s, 0)) AS precip_in", DBI::dbQuoteIdentifier(con, "port_1_atmos_41_in_precipitation")),
      "FROM", raw_table,
      "GROUP BY 1, 2"
    )

    precip_rows <- DBI::dbGetQuery(
      con,
      precip_query
    )
  }

  if (!nrow(precip_rows)) {
    message(sprintf("Skipping computed KBDI for %s because no daily precipitation rows were available.", stats_table))
    return(invisible(NULL))
  }

  precip_rows <- precip_rows %>%
    mutate(
      station_id = as.character(.data$station_id),
      local_date = as.Date(.data$local_date),
      precip_in = as.numeric(.data$precip_in),
      has_precip_support = if ("has_precip_support" %in% names(.)) as.logical(.data$has_precip_support) else TRUE
    )

  kbdi_input <- tmax_rows %>%
    inner_join(
      station_meta %>%
        transmute(
          station_id = coalesce(.data$stid, .data$station_id, .data$station_key),
          annual_ppt_in = .data$annual_ppt_in
        ) %>%
        distinct(.data$station_id, .keep_all = TRUE),
      by = "station_id"
    ) %>%
    left_join(precip_rows, by = c("station_id", "local_date")) %>%
    mutate(
      precip_in = dplyr::coalesce(.data$precip_in, 0)
    ) %>%
    arrange(.data$station_id, .data$local_date)

  if (precip_mode == "synoptic") {
    supported_stations <- precip_rows %>%
      group_by(.data$station_id) %>%
      summarise(has_precip_support = any(.data$has_precip_support, na.rm = TRUE), .groups = "drop") %>%
      filter(.data$has_precip_support) %>%
      pull(.data$station_id)

    kbdi_input <- kbdi_input %>%
      filter(.data$station_id %in% supported_stations)
  }

  if (!nrow(kbdi_input)) {
    message(sprintf("Skipping computed KBDI for %s because no eligible station/day inputs remained after joins.", stats_table))
    return(invisible(NULL))
  }

  kbdi_rows <- kbdi_input %>%
    group_by(.data$station_id) %>%
    group_modify(~{
      .x %>%
        mutate(
          kbdi = compute_kbdi_series(
            local_date = .data$local_date,
            tmax_f = .data$tmax_f,
            precip_in = .data$precip_in,
            annual_ppt_in = .data$annual_ppt_in
          )
        )
    }) %>%
    ungroup() %>%
    filter(!is.na(.data$kbdi)) %>%
    transmute(
      station_id = .data$station_id,
      local_date = .data$local_date,
      metric_name = "kbdi",
      value_min = .data$kbdi,
      value_max = .data$kbdi,
      value_mean = .data$kbdi,
      n_obs = 1L,
      first_timestamp_utc = as.numeric(as.POSIXct(.data$local_date, tz = timezone)),
      last_timestamp_utc = as.numeric(as.POSIXct(.data$local_date, tz = timezone)),
      source = paste0(source_name, " computed"),
      is_complete_day = .data$local_date < Sys.Date(),
      updated_at_utc = as.numeric(Sys.time())
    ) %>%
    arrange(.data$station_id, .data$local_date, desc(.data$last_timestamp_utc)) %>%
    distinct(.data$station_id, .data$local_date, .data$metric_name, .keep_all = TRUE)

  if (!nrow(kbdi_rows)) {
    message(sprintf("Skipping computed KBDI for %s because no KBDI values could be computed.", stats_table))
    return(invisible(NULL))
  }

  DBI::dbWriteTable(con, stats_table, kbdi_rows, append = TRUE)
  dedupe_table_on_keys(con, stats_table, c("station_id", "local_date", "metric_name"))

  message(sprintf(
    "Rebuilt %d computed KBDI rows into %s.",
    nrow(kbdi_rows),
    stats_table
  ))

  invisible(kbdi_rows)
}

refresh_daily_stats_table <- function(
  con,
  raw_table,
  stats_table,
  source_name,
  metric_cols,
  sum_metric_cols = character(),
  lookback_days = 10,
  timezone = "America/Denver",
  station_ids = NULL,
  force_full_backfill = FALSE
) {
  local_today <- as.Date(format(Sys.time(), tz = timezone, usetz = FALSE))

  if (!dbExistsTable(con, raw_table)) {
    message(sprintf("Skipping %s because %s does not exist yet.", stats_table, raw_table))
    return(invisible(NULL))
  }

  stats_exists <- dbExistsTable(con, stats_table)
  metric_cols <- intersect(metric_cols, DBI::dbListFields(con, raw_table))
  station_ids <- unique(as.character(station_ids))
  station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
  station_filter_sql <- if (length(station_ids)) {
    sprintf(
      " AND CAST(station_id AS TEXT) IN (%s)",
      paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    )
  } else {
    ""
  }
  cutoff_date <- Sys.Date() - lookback_days
  cutoff_date_num <- as.numeric(cutoff_date)
  cutoff_epoch <- as.numeric(as.POSIXct(
    paste(cutoff_date, "00:00:00"),
    tz = timezone
  ))
  raw_min_date <- query_scalar_value(DBI::dbGetQuery(
    con,
    sprintf("SELECT MIN(date) AS min_date FROM %s WHERE 1 = 1%s", raw_table, station_filter_sql)
  ), "min_date")

  if (is.na(raw_min_date)) {
    message(sprintf("Skipping %s because %s has no rows.", stats_table, raw_table))
    return(invisible(NULL))
  }

  stats_min_date <- if (stats_exists) {
    query_scalar_value(DBI::dbGetQuery(
      con,
      sprintf("SELECT MIN(local_date) AS min_date FROM %s WHERE 1 = 1%s", stats_table, station_filter_sql)
    ), "min_date")
  } else {
    NA
  }

  raw_min_local_date <- as.Date(as.POSIXct(raw_min_date, origin = "1970-01-01", tz = timezone), tz = timezone)
  needs_backfill <- !stats_exists || is.na(stats_min_date) || as.Date(stats_min_date) > raw_min_local_date
  allow_full_stats_backfill <- force_full_backfill || env_flag("SOFU_FULL_STATS_BACKFILL", "0")
  existing_metric_dates <- if (stats_exists && length(metric_cols)) {
    metric_sql <- paste(DBI::dbQuoteString(con, metric_cols), collapse = ", ")
    DBI::dbGetQuery(
      con,
      sprintf(
        paste(
          "SELECT metric_name, MIN(local_date) AS min_date",
          "FROM %s",
          "WHERE metric_name IN (%s)%s",
          "GROUP BY metric_name"
        ),
        stats_table,
        metric_sql,
        station_filter_sql
      )
    )
  } else {
    data.frame()
  }

  full_backfill_metrics <- if (!stats_exists || (needs_backfill && allow_full_stats_backfill)) {
    metric_cols
  } else {
    existing_lookup <- setNames(existing_metric_dates$min_date, existing_metric_dates$metric_name)
    Filter(function(metric_col) {
      min_date <- if (metric_col %in% names(existing_lookup)) existing_lookup[[metric_col]] else NA
      allow_full_stats_backfill && (is.null(min_date) || is.na(min_date) || as.Date(min_date) > raw_min_local_date)
    }, metric_cols)
  }

  full_backfill_metrics <- unname(full_backfill_metrics)
  recent_metrics <- setdiff(metric_cols, full_backfill_metrics)
  query_start_date <- if (length(full_backfill_metrics)) raw_min_local_date else cutoff_date

  message(sprintf(
    "Preparing %s from %s with %d metric columns since %s.",
    stats_table,
    raw_table,
    length(metric_cols),
    as.character(query_start_date)
  ))

  stats_list <- lapply(metric_cols, function(metric_col) {
    metric_query_start_epoch <- if (metric_col %in% full_backfill_metrics) raw_min_date else cutoff_epoch
    metric_identifier <- DBI::dbQuoteIdentifier(con, metric_col)
    is_increment_metric <- metric_col %in% sum_metric_cols
    value_min_sql <- if (is_increment_metric) sprintf("SUM(%s)", metric_identifier) else sprintf("MIN(%s)", metric_identifier)
    value_max_sql <- if (is_increment_metric) sprintf("SUM(%s)", metric_identifier) else sprintf("MAX(%s)", metric_identifier)
    value_mean_sql <- if (is_increment_metric) sprintf("SUM(%s)", metric_identifier) else sprintf("AVG(%s)", metric_identifier)
    sql <- sprintf(
      paste(
        "SELECT",
        "CAST(station_id AS TEXT) AS station_id,",
        "DATE(date, 'unixepoch', 'localtime') AS local_date,",
        "%1$s AS value_min,",
        "%2$s AS value_max,",
        "%3$s AS value_mean,",
        "COUNT(%4$s) AS n_obs,",
        "MIN(date) AS first_timestamp_utc,",
        "MAX(date) AS last_timestamp_utc",
        "FROM %5$s",
        "WHERE date >= %6$s",
        "AND %4$s IS NOT NULL%7$s",
        "GROUP BY station_id, DATE(date, 'unixepoch', 'localtime')"
      ),
      value_min_sql,
      value_max_sql,
      value_mean_sql,
      metric_identifier,
      raw_table,
      metric_query_start_epoch,
      station_filter_sql
    )

    metric_df <- DBI::dbGetQuery(con, sql)

    if (!nrow(metric_df)) {
      return(NULL)
    }

    metric_df$metric_name <- metric_col
    metric_df$source <- source_name
    metric_df$local_date <- as.Date(metric_df$local_date)
    metric_df$is_complete_day <- metric_df$local_date < local_today
    metric_df$updated_at_utc <- as.numeric(Sys.time())
    metric_df
  })

  stats_df <- data.table::rbindlist(stats_list, use.names = TRUE, fill = TRUE)

  if (!nrow(stats_df)) {
    message(sprintf("No stats rows produced for %s.", stats_table))
    return(invisible(data.frame()))
  }

  if (!stats_exists) {
    DBI::dbWriteTable(con, stats_table, stats_df, overwrite = TRUE)
  } else {
    DBI::dbWithTransaction(con, {
      if (length(full_backfill_metrics)) {
        metrics_sql <- paste(DBI::dbQuoteString(con, full_backfill_metrics), collapse = ", ")
        DBI::dbExecute(
          con,
          sprintf(
            "DELETE FROM %s WHERE metric_name IN (%s)%s",
            stats_table,
            metrics_sql,
            station_filter_sql
          )
        )
      }

      if (length(recent_metrics)) {
        metrics_sql <- paste(DBI::dbQuoteString(con, recent_metrics), collapse = ", ")
        DBI::dbExecute(
          con,
          sprintf(
            "DELETE FROM %s WHERE metric_name IN (%s) AND local_date >= %s%s",
            stats_table,
            metrics_sql,
            cutoff_date_num,
            station_filter_sql
          )
        )
      }

      if (nrow(stats_df)) {
        DBI::dbWriteTable(con, stats_table, stats_df, append = TRUE)
      }
    })
  }

  dedupe_table_on_keys(con, stats_table, c("station_id", "local_date", "metric_name"))

  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_station_date_metric ON %s (station_id, local_date, metric_name)",
      stats_table,
      stats_table
    )
  )

  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_metric_date ON %s (metric_name, local_date)",
      stats_table,
      stats_table
    )
  )

  ensure_unique_index(
    con,
    stats_table,
    sprintf("uidx_%s_station_date_metric", stats_table),
    c("station_id", "local_date", "metric_name")
  )

  message(sprintf(
    "Refreshed %s with %d station-day-metric rows.",
    stats_table,
    nrow(stats_df)
  ))

  invisible(stats_df)
}

refresh_daily_percentiles_table <- function(
  con,
  stats_table,
  percentiles_table,
  source_name,
  lookback_days = 10,
  station_ids = NULL,
  force_full_backfill = FALSE
) {
  if (!dbExistsTable(con, stats_table)) {
    message(sprintf("Skipping %s because %s does not exist yet.", percentiles_table, stats_table))
    return(invisible(NULL))
  }

  percentiles_exists <- dbExistsTable(con, percentiles_table)
  station_ids <- unique(as.character(station_ids))
  station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
  station_filter_sql <- if (length(station_ids)) {
    sprintf(
      " AND CAST(station_id AS TEXT) IN (%s)",
      paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    )
  } else {
    ""
  }
  cutoff_date <- Sys.Date() - lookback_days

  stats_min_date <- query_scalar_value(DBI::dbGetQuery(
    con,
    sprintf("SELECT MIN(local_date) AS min_date FROM %s WHERE 1 = 1%s", stats_table, station_filter_sql)
  ), "min_date")

  if (is.na(stats_min_date)) {
    message(sprintf("Skipping %s because %s has no rows.", percentiles_table, stats_table))
    return(invisible(NULL))
  }

  pct_min_date <- if (percentiles_exists) {
    query_scalar_value(DBI::dbGetQuery(
      con,
      sprintf("SELECT MIN(source_min_local_date) AS min_date FROM %s WHERE 1 = 1%s", percentiles_table, station_filter_sql)
    ), "min_date")
  } else {
    NA
  }

  stats_min_date <- as.Date(stats_min_date, origin = "1970-01-01")
  pct_min_date <- if (!is.na(pct_min_date)) as.Date(pct_min_date, origin = "1970-01-01") else pct_min_date
  needs_backfill <- !percentiles_exists || is.na(pct_min_date) || pct_min_date > stats_min_date
  stats_metric_dates <- DBI::dbGetQuery(
    con,
    sprintf(
      "SELECT metric_name, MIN(local_date) AS min_date FROM %s WHERE 1 = 1%s GROUP BY metric_name",
      stats_table,
      station_filter_sql
    )
  )
  pct_metric_dates <- if (percentiles_exists) {
    DBI::dbGetQuery(
      con,
      sprintf(
        "SELECT metric_name, MIN(source_min_local_date) AS min_date FROM %s WHERE 1 = 1%s GROUP BY metric_name",
        percentiles_table,
        station_filter_sql
      )
    )
  } else {
    data.frame()
  }

  pct_metric_lookup <- if (nrow(pct_metric_dates)) setNames(pct_metric_dates$min_date, pct_metric_dates$metric_name) else c()
  full_backfill_metrics <- if (force_full_backfill || needs_backfill || !percentiles_exists) {
    stats_metric_dates$metric_name
  } else {
    Filter(function(metric_name) {
      pct_min <- if (metric_name %in% names(pct_metric_lookup)) pct_metric_lookup[[metric_name]] else NA
      stats_min_matches <- stats_metric_dates$min_date[stats_metric_dates$metric_name == metric_name]
      stats_min <- if (length(stats_min_matches)) stats_min_matches[[1]] else NA
      is.null(pct_min) || is.na(pct_min) || as.Date(pct_min, origin = "1970-01-01") > as.Date(stats_min, origin = "1970-01-01")
    }, stats_metric_dates$metric_name)
  }

  impacted_start_date <- if (length(full_backfill_metrics)) stats_min_date else cutoff_date
  impacted_start_numeric <- as.numeric(impacted_start_date)

  message(sprintf(
    "Preparing %s from %s since %s.",
    percentiles_table,
    stats_table,
    as.character(impacted_start_date)
  ))

  if (length(full_backfill_metrics)) {
    metrics_sql <- paste(DBI::dbQuoteString(con, full_backfill_metrics), collapse = ", ")
    impacted_keys_sql <- sprintf(
      paste(
        "SELECT DISTINCT station_id, metric_name,",
        "strftime('%%m-%%d', local_date) AS day_of_year_key",
        "FROM %s",
        "WHERE metric_name IN (%s)%s",
        "UNION",
        "SELECT DISTINCT station_id, metric_name,",
        "strftime('%%m-%%d', local_date) AS day_of_year_key",
        "FROM %s",
        "WHERE local_date >= %s%s",
        "AND metric_name NOT IN (%s)"
      ),
      stats_table,
      metrics_sql,
      station_filter_sql,
      stats_table,
      impacted_start_numeric,
      station_filter_sql,
      metrics_sql
    )
  } else {
    impacted_keys_sql <- sprintf(
      paste(
        "SELECT DISTINCT station_id, metric_name,",
        "strftime('%%m-%%d', local_date) AS day_of_year_key",
        "FROM %s",
        "WHERE local_date >= %s%s"
      ),
      stats_table,
      impacted_start_numeric,
      station_filter_sql
    )
  }

  impacted_keys <- DBI::dbGetQuery(con, impacted_keys_sql)

  if (!nrow(impacted_keys)) {
    message(sprintf("No impacted percentile keys found for %s.", percentiles_table))
    return(invisible(data.frame()))
  }

  stats_df <- DBI::dbReadTable(con, stats_table) %>%
    dplyr::filter(
      is_complete_day == 1 | is_complete_day == TRUE
    ) %>%
    dplyr::mutate(
      local_date = as.Date(local_date, origin = "1970-01-01"),
      day_of_year_key = format(local_date, "%m-%d")
    ) %>%
    dplyr::distinct(
      station_id,
      metric_name,
      local_date,
      value_min,
      value_mean,
      value_max,
      .keep_all = TRUE
    ) %>%
    dplyr::inner_join(
      impacted_keys,
      by = c("station_id", "metric_name", "day_of_year_key")
    )

  if (!nrow(stats_df)) {
    message(sprintf("No complete-day rows available to build %s.", percentiles_table))
    return(invisible(data.frame()))
  }

  percentile_rows <- dplyr::bind_rows(lapply(
    c("value_min", "value_mean", "value_max"),
    function(stat_field) {
      stats_df %>%
        dplyr::filter(!is.na(.data[[stat_field]])) %>%
        dplyr::group_by(station_id, metric_name, day_of_year_key) %>%
        dplyr::summarise(
          stat_type = stat_field,
          n_years = dplyr::n_distinct(format(local_date, "%Y")),
          source_min_local_date = min(local_date),
          source_max_local_date = max(local_date),
          p05 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.05, na.rm = TRUE, type = 7)),
          p10 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.10, na.rm = TRUE, type = 7)),
          p25 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.25, na.rm = TRUE, type = 7)),
          p50 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.50, na.rm = TRUE, type = 7)),
          p75 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.75, na.rm = TRUE, type = 7)),
          p90 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.90, na.rm = TRUE, type = 7)),
          p95 = as.numeric(stats::quantile(.data[[stat_field]], probs = 0.95, na.rm = TRUE, type = 7)),
          .groups = "drop"
        )
    }
  )) %>%
    dplyr::mutate(
      source = source_name,
      updated_at_utc = as.numeric(Sys.time())
    )

  if (!nrow(percentile_rows)) {
    message(sprintf("No percentile rows produced for %s.", percentiles_table))
    return(invisible(data.frame()))
  }

  if (!percentiles_exists) {
    DBI::dbWriteTable(con, percentiles_table, percentile_rows, overwrite = TRUE)
  } else {
    impacted_delete <- unique(percentile_rows[, c("station_id", "metric_name", "day_of_year_key")])

    DBI::dbWithTransaction(con, {
      apply(impacted_delete, 1, function(row) {
        DBI::dbExecute(
          con,
          sprintf(
            paste(
              "DELETE FROM %s",
              "WHERE station_id = %s",
              "AND metric_name = %s",
              "AND day_of_year_key = %s"
            ),
            percentiles_table,
            DBI::dbQuoteString(con, row[["station_id"]]),
            DBI::dbQuoteString(con, row[["metric_name"]]),
            DBI::dbQuoteString(con, row[["day_of_year_key"]])
          )
        )
      })

      DBI::dbWriteTable(con, percentiles_table, percentile_rows, append = TRUE)
    })
  }

  dedupe_table_on_keys(con, percentiles_table, c("station_id", "metric_name", "day_of_year_key", "stat_type"))

  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_station_metric_doy ON %s (station_id, metric_name, day_of_year_key, stat_type)",
      percentiles_table,
      percentiles_table
    )
  )

  DBI::dbExecute(
    con,
    sprintf(
      "CREATE INDEX IF NOT EXISTS idx_%s_metric_doy ON %s (metric_name, day_of_year_key, stat_type)",
      percentiles_table,
      percentiles_table
    )
  )

  ensure_unique_index(
    con,
    percentiles_table,
    sprintf("uidx_%s_station_metric_doy_stat", percentiles_table),
    c("station_id", "metric_name", "day_of_year_key", "stat_type")
  )

  message(sprintf(
    "Refreshed %s with %d percentile rows.",
    percentiles_table,
    nrow(percentile_rows)
  ))

  invisible(percentile_rows)
}

# -------------------------------------------------------------------
# 2. Ensure target table exists
# -------------------------------------------------------------------
get_source_max_date <- function(con, station_ids, value_cols = NULL) {
  if (!dbExistsTable(con, "synoptic_fems_data") || !length(station_ids)) {
    return(as.POSIXct(NA, tz = "UTC"))
  }

  station_ids <- unique(as.character(station_ids))
  station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]

  if (!length(station_ids)) {
    return(as.POSIXct(NA, tz = "UTC"))
  }

  station_sql <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
  available_value_cols <- intersect(
    if (is.null(value_cols)) character() else value_cols,
    DBI::dbListFields(con, "synoptic_fems_data")
  )
  value_sql <- if (length(available_value_cols)) {
    paste(
      "AND (",
      paste(sprintf("%s IS NOT NULL", DBI::dbQuoteIdentifier(con, available_value_cols)), collapse = " OR "),
      ")"
    )
  } else {
    ""
  }

  max_date <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT MAX(date) AS max_date",
        "FROM synoptic_fems_data",
        "WHERE CAST(station_id AS TEXT) IN (%s)",
        "AND date <= %s %s"
      ),
      station_sql,
      as.numeric(Sys.time()),
      value_sql
    )
  )$max_date

  as.POSIXct(max_date, origin = "1970-01-01", tz = "UTC")
}

get_source_latest_by_station <- function(con, station_ids, value_cols) {
  if (!dbExistsTable(con, "synoptic_fems_data") || !length(station_ids)) {
    return(tibble::tibble(
      station_id = character(),
      raw_max_date = as.numeric(character()),
      value_max_date = as.numeric(character())
    ))
  }

  station_ids <- unique(as.character(station_ids))
  station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
  value_cols <- intersect(value_cols, DBI::dbListFields(con, "synoptic_fems_data"))
  if (!length(station_ids) || !length(value_cols)) {
    return(tibble::tibble(
      station_id = character(),
      raw_max_date = as.numeric(character()),
      value_max_date = as.numeric(character())
    ))
  }

  station_sql <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
  value_condition <- paste(sprintf("%s IS NOT NULL", DBI::dbQuoteIdentifier(con, value_cols)), collapse = " OR ")

  DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id,",
        "MAX(date) AS raw_max_date,",
        "MAX(CASE WHEN %s THEN date END) AS value_max_date",
        "FROM synoptic_fems_data",
        "WHERE CAST(station_id AS TEXT) IN (%s)",
        "GROUP BY CAST(station_id AS TEXT)"
      ),
      value_condition,
      station_sql
    )
  ) %>%
    mutate(
      station_id = as.character(.data$station_id),
      raw_max_date = as.numeric(.data$raw_max_date),
      value_max_date = as.numeric(.data$value_max_date)
    )
}

get_synoptic_recent_gap_start <- function(con, station_ids, value_cols, lookback_days, timezone = "America/Denver") {
  if (!dbExistsTable(con, "synoptic_fems_data") || !length(station_ids)) {
    return(as.POSIXct(NA, tz = timezone))
  }

  station_ids <- unique(as.character(station_ids))
  station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
  value_cols <- intersect(value_cols, DBI::dbListFields(con, "synoptic_fems_data"))
  if (!length(station_ids) || !length(value_cols)) {
    return(as.POSIXct(NA, tz = timezone))
  }

  local_today <- as.Date(format(Sys.time(), tz = timezone, usetz = FALSE))
  first_date <- local_today - max(as.integer(lookback_days) - 1L, 0L)
  station_sql <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
  value_condition <- paste(sprintf("%s IS NOT NULL", DBI::dbQuoteIdentifier(con, value_cols)), collapse = " OR ")
  observed <- DBI::dbGetQuery(
    con,
    sprintf(
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id,",
        "DATE(date, 'unixepoch', 'localtime') AS local_date",
        "FROM synoptic_fems_data",
        "WHERE CAST(station_id AS TEXT) IN (%s)",
        "AND date >= %s",
        "AND (%s)",
        "GROUP BY CAST(station_id AS TEXT), DATE(date, 'unixepoch', 'localtime')"
      ),
      station_sql,
      as.numeric(as.POSIXct(paste(first_date, "00:00:00"), tz = timezone)),
      value_condition
    )
  ) %>%
    mutate(
      station_id = as.character(.data$station_id),
      local_date = as.Date(.data$local_date)
    )

  expected <- tidyr::expand_grid(
    station_id = station_ids,
    local_date = seq.Date(first_date, local_today, by = "day")
  )
  missing <- expected %>%
    anti_join(observed, by = c("station_id", "local_date"))

  if (!nrow(missing)) {
    return(as.POSIXct(NA, tz = timezone))
  }

  as.POSIXct(paste(min(as.Date(missing$local_date)), "00:00:00"), tz = timezone)
}

synoptic_recent_cutoff <- Sys.time() - synoptic_backfill_days * 24 * 3600
synoptic_source_value_cols <- c(
  "air_temp_set_1.mean",
  "relative_humidity_set_1.mean",
  "wind_speed_set_1.mean",
  "soil_moisture_set_1.mean",
  "soil_moisture_set_2.mean",
  "precip_accum_set_1.mean",
  "precip_accum_one_hour_set_1.mean"
)
synoptic_latest_by_station <- get_source_latest_by_station(
  con,
  synoptic_fetch_station_ids,
  synoptic_source_value_cols
)
synoptic_existing_max <- if (nrow(synoptic_latest_by_station)) {
  as.POSIXct(max(synoptic_latest_by_station$value_max_date, na.rm = TRUE), origin = "1970-01-01", tz = "UTC")
} else {
  as.POSIXct(NA, tz = "UTC")
}
if (synoptic_targeted_repair) {
  # A targeted repair must honor its explicit bounded lookback even when the
  # same canonical station has newer FEMS weather rows. Those rows cannot
  # prove that the mapped Synoptic soil series is complete.
  synoptic_fetch_start <- synoptic_recent_cutoff
} else if (is.na(synoptic_existing_max) || !is.finite(as.numeric(synoptic_existing_max))) {
  message("Creating synoptic_fems_data table on first run or no Synoptic rows found; starting Synoptic pull from recent cutoff.")
  synoptic_fetch_start <- synoptic_recent_cutoff
} else {
  # Normally the one-day overlap wins. When an operator explicitly widens
  # SOFU_SYNOPTIC_BACKFILL_DAYS, however, honor that earlier bounded cutoff
  # instead of letting a recent existing maximum collapse the repair window.
  synoptic_regular_start <- max(
    as.POSIXct(paste(water_year_start_boot, "00:00:00"), tz = "America/Denver"),
    min(synoptic_existing_max - 24 * 3600, synoptic_recent_cutoff)
  )

  # Repair only short trailing spans where keyed rows exist but every source
  # measurement is blank. This avoids turning an inactive station into an
  # unbounded historical fetch while ensuring a bad merge cannot hide a gap.
  tail_repair_days <- suppressWarnings(as.integer(Sys.getenv("SOFU_SYNOPTIC_TAIL_REPAIR_DAYS", "14")))
  if (is.na(tail_repair_days) || tail_repair_days < 1L) tail_repair_days <- 14L
  tail_repair <- synoptic_latest_by_station %>%
    filter(
      !is.na(.data$raw_max_date),
      !is.na(.data$value_max_date),
      .data$raw_max_date > .data$value_max_date,
      (.data$raw_max_date - .data$value_max_date) <= tail_repair_days * 24 * 3600
    )
  tail_repair_start <- if (nrow(tail_repair)) {
    as.POSIXct(min(tail_repair$value_max_date) - 24 * 3600, origin = "1970-01-01", tz = "UTC")
  } else {
    synoptic_regular_start
  }
  gap_repair_days <- suppressWarnings(as.integer(Sys.getenv("SOFU_SYNOPTIC_GAP_REPAIR_DAYS", "14")))
  if (is.na(gap_repair_days) || gap_repair_days < 1L) gap_repair_days <- 14L
  gap_repair_start <- get_synoptic_recent_gap_start(
    con,
    synoptic_fetch_station_ids,
    synoptic_source_value_cols,
    gap_repair_days
  )
  synoptic_fetch_start <- min(
    synoptic_regular_start,
    tail_repair_start,
    if (is.na(gap_repair_start)) synoptic_regular_start else gap_repair_start
  )
}

fems_recent_start_date <- local_today_boot - max(fems_backfill_days - 1L, 0L)
if (fems_recent_start_date < water_year_start_boot) {
  fems_recent_start_date <- water_year_start_boot
}
fems_existing_max <- get_source_max_date(
  con,
  zone_site_roster %>%
    filter(.data$api == "FEMS") %>%
    pull(.data$station_id)
)
fems_existing_start_date <- if (is.na(fems_existing_max)) {
  as.Date(NA)
} else {
  as.Date(fems_existing_max - 24 * 3600, tz = "UTC")
}
fems_fetch_start_date <- fems_recent_start_date
if (!is.na(fems_existing_start_date)) {
  fems_fetch_start_date <- min(fems_fetch_start_date, fems_existing_start_date)
}
if (fems_fetch_start_date < water_year_start_boot) {
  fems_fetch_start_date <- water_year_start_boot
}

message(sprintf(
  "Fetching Synoptic data since: %s | Fetching FEMS data since: %s",
  as.character(synoptic_fetch_start),
  as.character(fems_fetch_start_date)
))

# Canonical roster IDs supplied here receive a one-time period-of-record
# establishment without widening the normal daily FEMS overlap.
establish_station_ids <- trimws(unlist(strsplit(
  Sys.getenv("SOFU_FEMS_ESTABLISH_STATIONS", ""),
  ",",
  fixed = TRUE
)))
establish_station_ids <- unique(establish_station_ids[nzchar(establish_station_ids)])

if (run_stage_ingest) {
  # -------------------------------------------------------------------
  # 3. Fetch only new data
  # -------------------------------------------------------------------
  synoptic_data <- get_synoptic_timeseries_long(
    station_ids = synoptic_fetch_station_ids,
    start_time = synoptic_fetch_start,
    end_time = Sys.time(),
    ob_timezone = "local",
    parallel = TRUE,
    chunk_by = "14 days"
  )

  fems_fetch_lookup <- zone_site_roster %>%
    filter(.data$api == "FEMS") %>%
    transmute(
      fems_fetch_id = as.character(.data$fems_fetch_id),
      station_id_roster = as.character(.data$station_id),
      WIMS_ID = as.character(.data$WIMS_ID),
      station_name_roster = as.character(.data$station_name)
    ) %>%
    filter(!is.na(.data$fems_fetch_id), nzchar(.data$fems_fetch_id)) %>%
    distinct(.data$fems_fetch_id, .keep_all = TRUE)

  # A new FEMS station needs a one-time period-of-record fetch. Keep this
  # separate from the short daily overlap used for established stations.
  establish_lookup <- fems_fetch_lookup %>%
    filter(.data$station_id_roster %in% establish_station_ids)
  unresolved_establish_ids <- setdiff(establish_station_ids, establish_lookup$station_id_roster)
  if (length(unresolved_establish_ids)) {
    warning(
      sprintf(
        "Skipping FEMS establishment for station(s) without a numeric fems_fetch_id: %s",
        paste(unresolved_establish_ids, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  establish_start_date <- suppressWarnings(as.Date(Sys.getenv(
    "SOFU_FEMS_ESTABLISH_START_DATE",
    "2000-01-01"
  )))
  if (is.na(establish_start_date)) {
    stop("SOFU_FEMS_ESTABLISH_START_DATE must use YYYY-MM-DD.", call. = FALSE)
  }
  establish_chunk_by <- Sys.getenv("SOFU_FEMS_ESTABLISH_CHUNK_BY", "90 days")
  establish_fetch_ids <- unique(as.character(establish_lookup$fems_fetch_id))
  regular_fems_ids <- setdiff(roster_fems_ids, establish_fetch_ids)

  if (skip_fems_nfdrs) {
    message("Skipping FEMS NFDRS retrieval; retaining existing NFDRS rows during this weather/Synoptic repair.")
    fems_data <- tibble::tibble(
      station_id = character(),
      observation_time = as.POSIXct(character(), tz = "America/Denver"),
      observation_time_lst = as.POSIXct(character(), tz = "America/Denver")
    )
  } else {
    fems_data <- get_nfdrs_long(
      station_ids = regular_fems_ids,
      start_date = fems_fetch_start_date,
      end_date = Sys.Date(),
      fuel_model = "Y",
      chunk_by = "14 days",
      parallel = FALSE,
      date_time_format = "LocalStationTime"
    )

    if (length(establish_fetch_ids)) {
      message(sprintf(
        "Establishing %d FEMS station(s) from %s through %s.",
        length(establish_fetch_ids),
        as.character(establish_start_date),
        as.character(Sys.Date())
      ))
      fems_data <- dplyr::bind_rows(
        fems_data,
        get_nfdrs_long(
          station_ids = establish_fetch_ids,
          start_date = establish_start_date,
          end_date = Sys.Date(),
          fuel_model = "Y",
          chunk_by = establish_chunk_by,
          parallel = FALSE,
          date_time_format = "LocalStationTime"
        )
      )
    }
  }

  if (!"station_id" %in% names(fems_data)) {
    if ("STATION_ID" %in% names(fems_data)) {
      fems_data$station_id <- fems_data$STATION_ID
    } else if ("wims_id" %in% names(fems_data)) {
      fems_data$station_id <- fems_data$wims_id
    } else if ("WIMS_ID" %in% names(fems_data)) {
      fems_data$station_id <- fems_data$WIMS_ID
    } else {
      fems_data$station_id <- character(nrow(fems_data))
    }
  }

  fetch_fems_weather_with_fallback <- function(station_ids, start_date, end_date) {
    station_ids <- unique(as.character(station_ids))
    station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]

    if (!length(station_ids)) {
      return(tibble::tibble())
    }

    bulk_weather <- tryCatch(
      get_weather(
        station_ids = station_ids,
        start_date = start_date,
        end_date = end_date
      ),
      error = function(err) {
        warning(
          sprintf("Bulk FEMS weather request failed, falling back to station-by-station pulls: %s", conditionMessage(err)),
          call. = FALSE
        )
        tibble::tibble()
      }
    )

    if (!"station_id" %in% names(bulk_weather)) {
      bulk_weather$station_id <- character(nrow(bulk_weather))
    }

    bulk_weather$station_id <- as.character(bulk_weather$station_id)
    fetched_ids <- unique(bulk_weather$station_id[!is.na(bulk_weather$station_id) & nzchar(bulk_weather$station_id)])

    weather_value_cols <- intersect(
      c("temperature", "relative_humidity", "wind_speed", "hourly_precip"),
      names(bulk_weather)
    )
    latest_weather_by_station <- if (
      nrow(bulk_weather) &&
      "observation_time_lst" %in% names(bulk_weather) &&
      length(weather_value_cols)
    ) {
      bulk_weather %>%
        mutate(
          observation_time_lst = if (inherits(.data$observation_time_lst, "POSIXt")) {
            .data$observation_time_lst
          } else {
            parse_api_datetime(.data$observation_time_lst, default_tz = "America/Denver", output_tz = "America/Denver")
          },
          has_weather_value = if_any(all_of(weather_value_cols), ~ !is.na(.x))
        ) %>%
        filter(.data$has_weather_value, !is.na(.data$observation_time_lst)) %>%
        group_by(.data$station_id) %>%
        summarise(
          latest_weather_date = max(as.Date(.data$observation_time_lst)),
          .groups = "drop"
        )
    } else {
      tibble::tibble(
        station_id = character(),
        latest_weather_date = as.Date(character())
      )
    }

    weather_lookup <- setNames(latest_weather_by_station$latest_weather_date, latest_weather_by_station$station_id)
    target_weather_date <- as.Date(end_date)
    stale_ids <- Filter(function(station_id_value) {
      latest_date <- if (station_id_value %in% names(weather_lookup)) {
        unname(weather_lookup[station_id_value][[1]])
      } else {
        NA
      }
      is.null(latest_date) || is.na(latest_date) || latest_date < target_weather_date
    }, station_ids)
    missing_ids <- unique(c(setdiff(station_ids, fetched_ids), stale_ids))

    message(sprintf(
      "FEMS weather bulk fetch returned %d row(s) across %d/%d station ids; %d station(s) need fallback.",
      nrow(bulk_weather),
      length(fetched_ids),
      length(station_ids),
      length(missing_ids)
    ))

    if (!length(missing_ids)) {
      return(bulk_weather)
    }

    message(sprintf(
      "FEMS weather fallback: fetching %d missing station(s) individually.",
      length(missing_ids)
    ))

    fallback_weather <- purrr::map_dfr(
      missing_ids,
      function(station_id_value) {
        tryCatch(
          {
            result <- get_weather(
              station_ids = station_id_value,
              start_date = start_date,
              end_date = end_date
            )

            if (!"station_id" %in% names(result)) {
              result$station_id <- rep(as.character(station_id_value), nrow(result))
            }

            result %>%
              mutate(station_id = as.character(.data$station_id))
          },
          error = function(err) {
            warning(
              sprintf("FEMS weather fallback failed for %s: %s", station_id_value, conditionMessage(err)),
              call. = FALSE
            )
            tibble::tibble(
              station_id = character(),
              observation_time = as.POSIXct(character()),
              observation_time_lst = as.POSIXct(character())
            )
          }
        )
      }
    )

    if (!"station_id" %in% names(fallback_weather)) {
      fallback_weather$station_id <- character(nrow(fallback_weather))
    }

    dplyr::bind_rows(bulk_weather, fallback_weather) %>%
      mutate(station_id = as.character(.data$station_id)) %>%
      distinct(.data$station_id, .data$observation_time_lst, .keep_all = TRUE)
  }

  fems_weather <- fetch_fems_weather_with_fallback(
    station_ids = regular_fems_ids,
    start_date = fems_fetch_start_date,
    end_date = Sys.Date()
  )

  if (length(establish_fetch_ids)) {
    fems_weather <- dplyr::bind_rows(
      fems_weather,
      fetch_fems_weather_with_fallback(
        station_ids = establish_fetch_ids,
        start_date = establish_start_date,
        end_date = Sys.Date()
      )
    ) %>%
      distinct(.data$station_id, .data$observation_time_lst, .keep_all = TRUE)
  }

  # FEMS can return a complete local day whose reported timestamps lead the
  # actual station clock by six hours. Keep observations through that known
  # lead, but never ingest values that would still be future after correction.
  fems_future_cutoff <- Sys.time() + fems_future_guard_hours * 60 * 60
  filter_fems_future_rows <- function(data, timestamp_col) {
    if (!nrow(data) || !timestamp_col %in% names(data)) return(data)

    timestamps <- data[[timestamp_col]]
    if (!inherits(timestamps, "POSIXt")) {
      timestamps <- parse_api_datetime(
        timestamps,
        default_tz = "America/Denver",
        output_tz = "UTC"
      )
    }
    data[is.na(timestamps) | as.numeric(timestamps) <= as.numeric(fems_future_cutoff), , drop = FALSE]
  }

  fems_weather <- filter_fems_future_rows(fems_weather, "observation_time_lst")
  fems_data <- filter_fems_future_rows(fems_data, "observation_time_lst")

  ensure_fems_datetime_column <- function(data, column_name, timezone = "America/Denver") {
    if (!column_name %in% names(data)) {
      data[[column_name]] <- as.POSIXct(
        rep(NA_real_, nrow(data)),
        origin = "1970-01-01",
        tz = timezone
      )
    }
    data
  }
  fems_data <- ensure_fems_datetime_column(fems_data, "observation_time_lst")
  fems_data <- ensure_fems_datetime_column(fems_data, "observation_time")
  fems_weather <- ensure_fems_datetime_column(fems_weather, "observation_time_lst")
  fems_weather <- ensure_fems_datetime_column(fems_weather, "observation_time")

  if (!nrow(fems_data)) {
    warning(
      "FEMS NFDRS returned no rows for this bounded pull; retaining existing NFDRS values.",
      call. = FALSE
    )
  }

  # The raw FEMS clock is ahead of the actual observation time. Filter its
  # full-day response against that raw clock first, then make the canonical
  # timestamp used by hourly keys, cross-source joins, and the frontend.
  fems_weather <- normalize_fems_observation_times(
    fems_weather,
    offset_hours = fems_clock_offset_hours
  )
  # NFDRS has its own documented `dateTimeFormat` contract. The request above
  # uses LocalStationTime, so retain those timestamps without the weather-clock
  # correction used for `weatherObs`.
  fems_data <- normalize_fems_observation_times(fems_data, offset_hours = 0)

  fems_nfdrs_hourly <- fems_data %>%
    mutate(
      fems_fetch_id = as.character(.data$station_id),
      observation_time_lst = if (inherits(.data$observation_time_lst, "POSIXt")) {
        .data$observation_time_lst
      } else {
        lubridate::as_datetime(.data$observation_time_lst, tz = "America/Denver")
      },
      date = lubridate::floor_date(.data$observation_time_lst, "hour")
    ) %>%
    select(-any_of(c("station_id", "station_name"))) %>%
    left_join(
      fems_fetch_lookup,
      by = "fems_fetch_id"
    ) %>%
    mutate(
      WIMS_ID = as.character(.data$WIMS_ID),
      station_id = as.character(.data$station_id_roster),
      station_name = as.character(.data$station_name_roster)
    )

  fems_weather_hourly <- fems_weather %>%
    mutate(
      station_id = as.character(.data$station_id),
      date_time = if (inherits(.data$observation_time_lst, "POSIXt")) {
        .data$observation_time_lst
      } else {
        lubridate::as_datetime(.data$observation_time_lst, tz = "America/Denver")
      },
      `air_temp_set_1.mean` = suppressWarnings(as.numeric(.data$temperature)),
      `relative_humidity_set_1.mean` = suppressWarnings(as.numeric(.data$relative_humidity)),
      `wind_speed_set_1.mean` = suppressWarnings(as.numeric(.data$wind_speed)),
      `precip_accum_one_hour_set_1.mean` = suppressWarnings(as.numeric(.data$hourly_precip))
    ) %>%
    select(
      "station_id",
      "date_time",
      `air_temp_set_1.mean`,
      `relative_humidity_set_1.mean`,
      `wind_speed_set_1.mean`,
      `precip_accum_one_hour_set_1.mean`
    ) %>%
    wrangle_to_hourly(type = "synoptic") %>%
    rename(fems_fetch_id = .data$station_id) %>%
    left_join(
      fems_fetch_lookup,
      by = "fems_fetch_id"
    ) %>%
    mutate(
      station_id = as.character(.data$station_id_roster),
      WIMS_ID = as.character(.data$WIMS_ID),
      station_name = as.character(.data$station_name_roster)
    )

  if ("date_time" %in% names(synoptic_data) && !inherits(synoptic_data$date_time, "POSIXt")) {
    synoptic_data$date_time <- suppressWarnings(
      lubridate::ymd_hms(synoptic_data$date_time, tz = "America/Denver")
    )
    if (all(is.na(synoptic_data$date_time))) {
      synoptic_data$date_time <- lubridate::as_datetime(synoptic_data$date_time)
    }
  }

  synoptic_data_hourly <- synoptic_data %>%
    wrangle_to_hourly(type = "synoptic") %>%
    normalize_synoptic_hourly_metric_names()

  synoptic_component <- synoptic_data_hourly %>%
    left_join(
      synoptic_wims_lookup,
      by = c("station_id" = "STID")
    ) %>%
    mutate(
      station_id = dplyr::coalesce(
        as.character(.data$canonical_station_id),
        as.character(.data$station_id)
      ),
      WIMS_ID = dplyr::coalesce(as.character(.data$WIMS_ID), as.character(.data$station_id)),
      is_fems_soil_overlay = dplyr::coalesce(.data$is_fems_soil_overlay, FALSE)
    )

  synoptic_measurement_cols <- setdiff(names(synoptic_component), c("station_id", "date", "WIMS_ID", "station_name"))
  if (length(synoptic_measurement_cols)) {
    synoptic_component <- synoptic_component %>%
      filter(if_any(all_of(synoptic_measurement_cols), ~ !is.na(.x)))
  }

  fems_component <- fems_weather_hourly %>%
    full_join(
      fems_nfdrs_hourly,
      by = c("fems_fetch_id", "station_id", "station_name", "WIMS_ID", "date")
    ) %>%
    mutate(
      station_id = as.character(.data$station_id),
      WIMS_ID = as.character(.data$WIMS_ID)
    )

  # Some FEMS stations expose soil moisture only through Synoptic. Overlay
  # those explicitly mapped soil fields without replacing FEMS weather or
  # NFDRS values for the same canonical station and hour.
  soil_metric_cols <- c("soil_moisture_set_1.mean", "soil_moisture_set_2.mean")
  for (metric_name in soil_metric_cols) {
    if (!metric_name %in% names(synoptic_component)) synoptic_component[[metric_name]] <- NA_real_
    if (!metric_name %in% names(fems_component)) fems_component[[metric_name]] <- NA_real_
  }

  synoptic_soil_overlay <- synoptic_component %>%
    filter(.data$is_fems_soil_overlay) %>%
    select(.data$station_id, .data$WIMS_ID, .data$date, all_of(soil_metric_cols)) %>%
    distinct(.data$station_id, .data$WIMS_ID, .data$date, .keep_all = TRUE)

  # Keep mapped Synoptic soil records even when FEMS has no row at the same
  # hour. The previous exact-time overlay updated only coincident timestamps,
  # then dropped the remaining valid Synoptic observations.
  synoptic_soil_only_component <- synoptic_soil_overlay %>%
    left_join(
      zone_site_roster %>%
        select(.data$station_id, .data$station_name) %>%
        distinct(.data$station_id, .keep_all = TRUE),
      by = "station_id"
    )

  if (nrow(synoptic_soil_overlay)) {
    fems_component <- fems_component %>%
      left_join(
        synoptic_soil_overlay,
        by = c("station_id", "WIMS_ID", "date"),
        suffix = c("", "__synoptic_soil")
      )

    for (metric_name in soil_metric_cols) {
      overlay_name <- paste0(metric_name, "__synoptic_soil")
      fems_component[[metric_name]] <- dplyr::coalesce(
        suppressWarnings(as.numeric(fems_component[[overlay_name]])),
        suppressWarnings(as.numeric(fems_component[[metric_name]]))
      )
    }
    fems_component <- fems_component %>%
      select(-all_of(paste0(soil_metric_cols, "__synoptic_soil")))
  }

  synoptic_component <- synoptic_component %>%
    filter(!.data$is_fems_soil_overlay) %>%
    select(-any_of(c("canonical_station_id", "is_fems_soil_overlay")))

  new_synoptic_fems_data <- bind_rows(
    # FEMS stays first so it remains authoritative where timestamps coincide;
    # its soil fields already received the explicit same-hour overlay above.
    fems_component,
    synoptic_component,
    synoptic_soil_only_component
  ) %>%
    mutate(
      station_id = dplyr::coalesce(as.character(.data$station_id), as.character(.data$WIMS_ID))
    ) %>%
    distinct(station_id, WIMS_ID, date, .keep_all = TRUE)


  # -------------------------------------------------------------------
  # 4. First run: create table
  # -------------------------------------------------------------------
  if (!dbExistsTable(con, "synoptic_fems_data")) {
    dbWriteTable(con, "synoptic_fems_data", new_synoptic_fems_data, overwrite = TRUE)

    dbExecute(con, "
      CREATE UNIQUE INDEX IF NOT EXISTS idx_syn_fems_unique
      ON synoptic_fems_data (station_id, WIMS_ID, date)
    ")
    ensure_index(
      con,
      "synoptic_fems_data",
      "idx_syn_fems_station_date",
      c("station_id", "date")
    )

    message(sprintf("Created synoptic_fems_data with %d rows.", nrow(new_synoptic_fems_data)))

  } else if (nrow(new_synoptic_fems_data) > 0) {

    # -----------------------------------------------------------------
    # 5. Incremental load via staging table
    # -----------------------------------------------------------------

    db_cols <- dbListFields(con, "synoptic_fems_data")
    new_cols <- names(new_synoptic_fems_data)

    missing_in_new <- setdiff(db_cols, new_cols)

    for (col in missing_in_new) {
      new_synoptic_fems_data[[col]] <- NA
    }

    new_synoptic_fems_data <- new_synoptic_fems_data[, db_cols, drop = FALSE]

    identity_cols <- c(
      "station_id", "date", "WIMS_ID", "station_name", "observation_time",
      "observation_time_lst", "display_hour_lst", "nfdr_type", "fuel_model"
    )
    value_cols <- setdiff(db_cols, identity_cols)
    if (length(value_cols)) {
      has_observation <- rowSums(!is.na(new_synoptic_fems_data[, value_cols, drop = FALSE])) > 0L
      dropped_blank_rows <- sum(!has_observation)
      if (dropped_blank_rows) {
        warning(
          sprintf("Dropping %d empty Synoptic/FEMS candidate row(s) before merge.", dropped_blank_rows),
          call. = FALSE
        )
        new_synoptic_fems_data <- new_synoptic_fems_data[has_observation, , drop = FALSE]
      }
    }

    if (!nrow(new_synoptic_fems_data)) {
      message("No non-empty Synoptic/FEMS candidate rows remain after validation.")
    } else {

    message(sprintf(
      "Preparing Synoptic/FEMS merge with %d candidate rows into existing table.",
      nrow(new_synoptic_fems_data)
    ))

    dbWriteTable(con, "synoptic_fems_stage", new_synoptic_fems_data, overwrite = TRUE, temporary = TRUE)
    dbExecute(
      con,
      "CREATE INDEX IF NOT EXISTS idx_synoptic_fems_stage_station_date ON synoptic_fems_stage (station_id, date)"
    )
    ensure_index(
      con,
      "synoptic_fems_data",
      "idx_syn_fems_station_date",
      c("station_id", "date")
    )

    common_cols <- colnames(new_synoptic_fems_data)
    col_sql <- paste(DBI::dbQuoteIdentifier(con, common_cols), collapse = ", ")

    # Weather and NFDRS arrive through independent FEMS endpoints. Preserve
    # an existing non-missing field when the current staged source payload
    # does not carry that field, rather than deleting a valid companion value
    # at the same canonical station/hour during the exact-key replacement.
    preservable_cols <- setdiff(value_cols, c("quality_code"))
    for (column_name in preservable_cols) {
      quoted_column <- DBI::dbQuoteIdentifier(con, column_name)
      DBI::dbExecute(
        con,
        sprintf(
          paste(
            "UPDATE synoptic_fems_stage AS stage",
            "SET %1$s = COALESCE(stage.%1$s, (",
            "SELECT existing.%1$s FROM synoptic_fems_data AS existing",
            "WHERE existing.station_id = stage.station_id",
            "AND existing.date = stage.date",
            "))",
            "WHERE stage.%1$s IS NULL"
          ),
          quoted_column
        )
      )
    }

    merge_started_at <- Sys.time()

    dbWithTransaction(con, {
      # The source clock adjustment changes FEMS hourly keys. Remove only
      # legacy FEMS weather rows in this staged interval; soil-only Synoptic
      # records remain intact and are rejoined on their canonical hour.
      fems_weather_columns <- intersect(
        c(
          "air_temp_set_1.mean", "relative_humidity_set_1.mean",
          "wind_speed_set_1.mean", "precip_accum_one_hour_set_1.mean"
        ),
        db_cols
      )
      if (fems_clock_offset_hours != 0 && length(fems_weather_columns) && nrow(fems_component)) {
        fems_migration_windows <- fems_component %>%
          filter(!is.na(.data$station_id), !is.na(.data$date)) %>%
          group_by(.data$station_id) %>%
          summarise(
            start_date = min(as.numeric(.data$date)),
            end_date = max(as.numeric(.data$date)) - fems_clock_offset_hours * 60 * 60,
            .groups = "drop"
          )
        weather_predicate <- paste(
          sprintf("%s IS NOT NULL", DBI::dbQuoteIdentifier(con, fems_weather_columns)),
          collapse = " OR "
        )
        for (window_index in seq_len(nrow(fems_migration_windows))) {
          window <- fems_migration_windows[window_index, , drop = FALSE]
          dbExecute(
            con,
            sprintf(
              paste(
                "DELETE FROM synoptic_fems_data",
                "WHERE station_id = %s AND date >= %s AND date <= %s",
                "AND (%s)"
              ),
              DBI::dbQuoteString(con, window$station_id[[1]]),
              window$start_date[[1]],
              window$end_date[[1]],
              weather_predicate
            )
          )
        }
      }

      fems_station_ids <- zone_site_roster %>%
        filter(.data$api == "FEMS") %>%
        pull(.data$station_id) %>%
        unique()
      fems_station_ids_sql <- paste(
        DBI::dbQuoteString(con, fems_station_ids),
        collapse = ", "
      )
      if (nzchar(fems_station_ids_sql)) {
        # Remove prior future-looking FEMS rows before loading the corrected
        # bounded window. Synoptic soil is not expected to have future rows.
        dbExecute(
          con,
          sprintf(
            "DELETE FROM synoptic_fems_data WHERE station_id IN (%s) AND date > %s",
            fems_station_ids_sql,
            as.numeric(fems_future_cutoff)
          )
        )
      }

      # Replace only exact staged observation keys. A single global date
      # window across FEMS and Synoptic can otherwise erase one source's
      # valid rows when the other source has a longer recovery window.
      message("Replacing existing Synoptic/FEMS rows at exact staged station/timestamp keys...")
      dbExecute(
        con,
        paste(
          "DELETE FROM synoptic_fems_data",
          "WHERE (station_id, date) IN (",
          "SELECT station_id, date FROM synoptic_fems_stage",
          ")"
        )
      )

      message("Inserting refreshed Synoptic/FEMS rows from stage table...")
      dbExecute(
        con,
        sprintf(
          paste(
            "INSERT INTO synoptic_fems_data (%s)",
            "SELECT %s FROM synoptic_fems_stage"
          ),
          col_sql,
          col_sql
        )
      )
    })

      message(sprintf(
        "Merged %d Synoptic/FEMS candidate rows in %.1f seconds.",
        nrow(new_synoptic_fems_data),
        as.numeric(difftime(Sys.time(), merge_started_at, units = "secs"))
      ))
    }

  } else {
    message("No new rows to append.")
  }

  # The physical grain is one normalized hourly observation per station and
  # timestamp. Earlier indexes included nullable WIMS_ID and therefore let
  # duplicate NULL-key rows through.
  dedupe_table_on_keys(con, "synoptic_fems_data", c("station_id", "date"))
  ensure_unique_index(
    con,
    "synoptic_fems_data",
    "uidx_synoptic_fems_station_date",
    c("station_id", "date")
  )

  fems_station_sql <- paste(
    DBI::dbQuoteString(
      con,
      zone_site_roster %>%
        filter(.data$api == "FEMS") %>%
        pull(.data$station_id) %>%
        unique()
    ),
    collapse = ", "
  )

  if (nzchar(fems_station_sql)) {
    repaired_rows <- DBI::dbExecute(
      con,
      sprintf(
        paste(
          "UPDATE synoptic_fems_data",
          "SET station_id = WIMS_ID",
          "WHERE (station_id IS NULL OR TRIM(CAST(station_id AS TEXT)) = '')",
          "AND CAST(WIMS_ID AS TEXT) IN (%s)"
        ),
        fems_station_sql
      )
    )

    if (repaired_rows > 0) {
      message(sprintf("Normalized %d FEMS row(s) with blank station_id using WIMS_ID.", repaired_rows))
    }
  }


  # =============================================================================
  # 4. INCREMENTAL DATA FETCH FOR ZENTRACLOUD
  # =============================================================================
  if (!skip_zentra_ingest) {

  usfs_soil_sites <- c(
    "z6-28071",
    "z6-32392",
    "z6-32393",
    "z6-32483",
    "z6-28073"
  )

  now_utc <- as.POSIXct(Sys.time(), tz = "UTC")
  local_today_zentra <- as.Date(format(Sys.time(), tz = "America/Denver", usetz = FALSE))
  water_year_start_zentra <- as.Date(sprintf(
    "%d-10-01",
    ifelse(
      as.integer(format(local_today_zentra, "%m")) >= 10L,
      as.integer(format(local_today_zentra, "%Y")),
      as.integer(format(local_today_zentra, "%Y")) - 1L
    )
  ))
  water_year_backfill_days <- as.integer(local_today_zentra - water_year_start_zentra) + 1L
  if (force_water_year_backfill) {
    zentra_backfill_days <- max(zentra_backfill_days, water_year_backfill_days)
  }
  zentra_backfill_start_date <- max(
    water_year_start_zentra,
    local_today_zentra - max(zentra_backfill_days - 1L, 0L)
  )

  zentra_station_windows <- tibble::tibble(
    station_id = character(),
    start_datetime = as.POSIXct(character(), tz = "America/Denver"),
    end_datetime = as.POSIXct(character(), tz = "America/Denver"),
    window_type = character()
  )

  collapse_station_windows <- function(windows_df) {
    if (!nrow(windows_df)) {
      return(windows_df)
    }

    windows_df <- windows_df %>%
      arrange(.data$station_id, .data$start_datetime, .data$end_datetime)

    collapsed <- lapply(split(windows_df, windows_df$station_id), function(station_windows) {
      station_windows <- station_windows[order(station_windows$start_datetime, station_windows$end_datetime), , drop = FALSE]
      merged_rows <- list()

      current_start <- station_windows$start_datetime[[1]]
      current_end <- station_windows$end_datetime[[1]]
      current_types <- station_windows$window_type[[1]]

      if (nrow(station_windows) > 1) {
        for (idx in 2:nrow(station_windows)) {
          next_start <- station_windows$start_datetime[[idx]]
          next_end <- station_windows$end_datetime[[idx]]
          next_type <- station_windows$window_type[[idx]]

          if (!is.na(next_start) && !is.na(current_end) && next_start <= (current_end + 1)) {
            current_end <- max(current_end, next_end)
            current_types <- unique(c(current_types, next_type))
          } else {
            merged_rows[[length(merged_rows) + 1L]] <- tibble::tibble(
              station_id = as.character(station_windows$station_id[[1]]),
              start_datetime = current_start,
              end_datetime = current_end,
              window_type = paste(sort(unique(current_types)), collapse = "+")
            )
            current_start <- next_start
            current_end <- next_end
            current_types <- next_type
          }
        }
      }

      merged_rows[[length(merged_rows) + 1L]] <- tibble::tibble(
        station_id = as.character(station_windows$station_id[[1]]),
        start_datetime = current_start,
        end_datetime = current_end,
        window_type = paste(sort(unique(current_types)), collapse = "+")
      )

      dplyr::bind_rows(merged_rows)
    })

    dplyr::bind_rows(collapsed) %>%
      arrange(.data$station_id, .data$start_datetime, .data$end_datetime)
  }

  fetch_zentracloud_window <- function(device_id, start_datetime, end_datetime, chunk_days = NULL) {
  start_utc <- as.POSIXct(start_datetime, tz = "UTC")
  end_utc <- as.POSIXct(end_datetime, tz = "UTC")

  if (is.na(start_utc) || is.na(end_utc) || end_utc < start_utc) {
    return(tibble::tibble())
  }

  if (is.null(chunk_days) || length(chunk_days) == 0) {
    window_days <- max(1L, ceiling(as.numeric(difftime(end_utc, start_utc, units = "days"))))
    default_chunk_days <- suppressWarnings(as.integer(Sys.getenv("SOFU_ZENTRA_CHUNK_DAYS", "30")))
    if (is.na(default_chunk_days) || default_chunk_days < 1L) {
      default_chunk_days <- 30L
    }
    chunk_days <- min(default_chunk_days, window_days)
  }

  chunk_days <- suppressWarnings(as.integer(chunk_days[[1]]))
  if (is.na(chunk_days) || chunk_days < 1L) {
    chunk_days <- 30L
  }

  message(sprintf(
    "  Using Zentra chunk size of %d day%s for %s.",
    chunk_days,
    ifelse(chunk_days == 1L, "", "s"),
    device_id
  ))

  max_rate_limit_retries <- suppressWarnings(as.integer(Sys.getenv("SOFU_ZENTRA_429_RETRIES", "4")))
  if (is.na(max_rate_limit_retries) || max_rate_limit_retries < 0L) {
    max_rate_limit_retries <- 4L
  }
  base_retry_sleep <- suppressWarnings(as.numeric(Sys.getenv("SOFU_ZENTRA_429_SLEEP_SECONDS", "20")))
  if (!is.finite(base_retry_sleep) || base_retry_sleep <= 0) {
    base_retry_sleep <- 20
  }

  fetch_one_window <- function(window_start, window_end) {
    attempt <- 1L

    repeat {
      result <- tryCatch(
        get_zentracloud_v5_data(
          device_id = device_id,
          start_datetime = format(window_start, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
          end_datetime = format(window_end, "%Y-%m-%d %H:%M:%S", tz = "UTC")
        ),
        error = identity
      )

      if (!inherits(result, "error")) {
        return(result)
      }

      is_rate_limited <- grepl("429", conditionMessage(result), fixed = TRUE)
      if (!is_rate_limited || attempt > max_rate_limit_retries) {
        return(result)
      }

      sleep_seconds <- base_retry_sleep * (2^(attempt - 1L))
      message(sprintf(
        "    Rate limited fetching %s from %s to %s; retry %d/%d after %.0f sec.",
        device_id,
        format(window_start, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
        format(window_end, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
        attempt,
        max_rate_limit_retries,
        sleep_seconds
      ))
      Sys.sleep(sleep_seconds)
      attempt <- attempt + 1L
    }
  }

  chunk_starts <- seq(from = start_utc, to = end_utc, by = sprintf("%d days", chunk_days))
  chunk_results <- purrr::map(
    chunk_starts,
    function(chunk_start) {
      chunk_end <- min(chunk_start + chunk_days * 24 * 3600 - 1, end_utc)

      message(sprintf(
        "  - Fetching %s from %s to %s (chunk size %d day%s)",
        device_id,
        format(chunk_start, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
        format(chunk_end, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
        chunk_days,
        ifelse(chunk_days == 1L, "", "s")
      ))

      chunk_result <- fetch_one_window(chunk_start, chunk_end)
      if (inherits(chunk_result, "error")) {
        warning(
          sprintf(
            "Zentra fetch failed for %s from %s to %s: %s",
            device_id,
            format(chunk_start, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
            format(chunk_end, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
            conditionMessage(chunk_result)
          ),
          call. = FALSE
        )
        return(tibble::tibble())
      }

      chunk_result
    }
  )

  if (!length(chunk_results)) {
    return(tibble::tibble())
  }

  dplyr::bind_rows(chunk_results)
  }

  zentra_missing_windows <- tibble::tibble(
    station_id = character(),
    start_datetime = as.POSIXct(character(), tz = "America/Denver"),
    end_datetime = as.POSIXct(character(), tz = "America/Denver"),
    window_type = character()
  )
  zentra_missing_days <- tibble::tibble(
    station_id = character(),
    local_date = as.Date(character())
  )

  if (dbExistsTable(con, "zentracloud_data")) {
    latest_target_local_date <- local_today_zentra

    if (!is.na(latest_target_local_date) && latest_target_local_date >= zentra_backfill_start_date) {
      observed_station_rows <- DBI::dbGetQuery(
        con,
        sprintf(
          paste(
            "SELECT CAST(station_id AS TEXT) AS station_id,",
            "date",
            "FROM zentracloud_data",
            "WHERE CAST(station_id AS TEXT) IN (%s)",
            "AND date >= %s",
            "AND date <= %s"
          ),
          paste(DBI::dbQuoteString(con, usfs_soil_sites), collapse = ", "),
          as.numeric(as.POSIXct(paste(zentra_backfill_start_date, "00:00:00"), tz = "America/Denver")),
          as.numeric(now_utc)
        )
      )

      observed_station_days <- if (nrow(observed_station_rows)) {
        observed_station_rows %>%
          mutate(
            station_id = as.character(.data$station_id),
            date = suppressWarnings(as.numeric(.data$date)),
            local_date = as.Date(
              format(
                as.POSIXct(.data$date, origin = "1970-01-01", tz = "UTC"),
                tz = "America/Denver",
                usetz = FALSE
              )
            )
          ) %>%
          filter(!is.na(.data$local_date)) %>%
          distinct(.data$station_id, .data$local_date)
      } else {
        tibble::tibble(
          station_id = character(),
          local_date = as.Date(character())
        )
      }

      expected_station_days <- tidyr::expand_grid(
        station_id = usfs_soil_sites,
        local_date = seq.Date(zentra_backfill_start_date, latest_target_local_date, by = "day")
      )

      zentra_missing_days <- expected_station_days %>%
        anti_join(
          observed_station_days,
          by = c("station_id", "local_date")
        ) %>%
        arrange(.data$station_id, .data$local_date)

      if (nrow(zentra_missing_days)) {
        zentra_missing_windows <- zentra_missing_days %>%
          group_by(.data$station_id) %>%
          mutate(
            run_group = cumsum(dplyr::coalesce(.data$local_date != dplyr::lag(.data$local_date) + 1L, TRUE))
          ) %>%
          group_by(.data$station_id, .data$run_group) %>%
          summarise(
            start_date = min(.data$local_date),
            end_date = max(.data$local_date),
            missing_day_count = dplyr::n(),
            .groups = "drop"
          ) %>%
          transmute(
            station_id = as.character(.data$station_id),
            start_datetime = as.POSIXct(paste(.data$start_date, "00:00:00"), tz = "America/Denver"),
            end_datetime = as.POSIXct(paste(.data$end_date, "23:59:59"), tz = "America/Denver"),
            window_type = "missing_days"
          )
      }
    }
  }

  zentra_refresh_windows <- tibble::tibble(
    station_id = usfs_soil_sites,
    start_datetime = as.POSIXct(
      paste(pmax(local_today_zentra - 1L, water_year_start_zentra), "00:00:00"),
      tz = "America/Denver"
    ),
    end_datetime = now_utc,
    window_type = "recent_refresh"
  )

  zentra_station_windows <- bind_rows(
    zentra_missing_windows,
    zentra_refresh_windows
  ) %>%
    distinct(.data$station_id, .data$start_datetime, .data$end_datetime, .keep_all = TRUE) %>%
    collapse_station_windows()

  if (nrow(zentra_missing_days)) {
    utils::write.csv(
      zentra_missing_days,
      file.path(output_dir, "zentra_missing_days.csv"),
      row.names = FALSE
    )
  }

  if (nrow(zentra_station_windows)) {
    utils::write.csv(
      zentra_station_windows,
      file.path(output_dir, "zentra_station_windows.csv"),
      row.names = FALSE
    )
  }

  message(sprintf(
    "Fetching ZentraCloud data from %s through %s (%d-day effective backfill window; force_wy_backfill=%s)",
    zentra_backfill_start_date,
    now_utc,
    zentra_backfill_days,
    force_water_year_backfill
  ))

  new_zentra_data <- purrr::map(
    seq_len(nrow(zentra_station_windows)),
    purrr::safely(function(i) {
      station_id <- zentra_station_windows$station_id[[i]]
      station_start <- zentra_station_windows$start_datetime[[i]]
      station_end <- zentra_station_windows$end_datetime[[i]]
      window_type <- zentra_station_windows$window_type[[i]]
      message(sprintf(
        "Fetching ZentraCloud data for %s from %s to %s [%s]",
        station_id,
        station_start,
        station_end,
        window_type
      ))

      fetch_zentracloud_window(
        device_id = station_id,
        start_datetime = station_start,
        end_datetime = station_end
      ) %>%
        mutate(station_id = station_id)
    })
  )

  # Keep only successful pulls
  new_zentra_data_bind <- bind_rows(purrr::map(new_zentra_data, "result"))
  zentra_existing_max_before <- if (dbExistsTable(con, "zentracloud_data")) {
    DBI::dbGetQuery(
      con,
      paste(
        "SELECT CAST(station_id AS TEXT) AS station_id,",
        "MAX(date) AS existing_max_date",
        "FROM zentracloud_data",
        "GROUP BY station_id"
      )
    ) %>%
      mutate(existing_max_date = as.POSIXct(.data$existing_max_date, origin = "1970-01-01", tz = "UTC"))
  } else {
    tibble::tibble(station_id = character(), existing_max_date = as.POSIXct(character()))
  }

  zentra_fetch_qc <- purrr::map_dfr(
    seq_len(nrow(zentra_station_windows)),
    function(i) {
      station_id <- zentra_station_windows$station_id[[i]]
      result_tbl <- new_zentra_data[[i]]$result
      result_err <- new_zentra_data[[i]]$error

      if (!is.null(result_err)) {
        return(tibble::tibble(
          station_id = station_id,
          requested_start_utc = as.character(zentra_station_windows$start_datetime[[i]]),
          requested_end_utc = as.character(zentra_station_windows$end_datetime[[i]]),
          fetched_raw_rows = 0L,
          fetched_raw_min_utc = NA_character_,
          fetched_raw_max_utc = NA_character_,
          transformed_hourly_rows = 0L,
          transformed_min_utc = NA_character_,
          transformed_max_utc = NA_character_,
          existing_max_before_utc = as.character(zentra_existing_max_before$existing_max_date[match(station_id, zentra_existing_max_before$station_id)]),
          candidate_new_rows = 0L,
          note = sprintf("Fetch error: %s", conditionMessage(result_err))
        ))
      }

      if (is.null(result_tbl) || !nrow(result_tbl)) {
        return(tibble::tibble(
          station_id = station_id,
          requested_start_utc = as.character(zentra_station_windows$start_datetime[[i]]),
          requested_end_utc = as.character(zentra_station_windows$end_datetime[[i]]),
          fetched_raw_rows = 0L,
          fetched_raw_min_utc = NA_character_,
          fetched_raw_max_utc = NA_character_,
          transformed_hourly_rows = 0L,
          transformed_min_utc = NA_character_,
          transformed_max_utc = NA_character_,
          existing_max_before_utc = as.character(zentra_existing_max_before$existing_max_date[match(station_id, zentra_existing_max_before$station_id)]),
          candidate_new_rows = 0L,
          note = "No rows returned from API"
        ))
      }

      raw_timestamps <- as.POSIXct(result_tbl$timestamp, origin = "1970-01-01", tz = "UTC")

      tibble::tibble(
        station_id = station_id,
        requested_start_utc = as.character(zentra_station_windows$start_datetime[[i]]),
        requested_end_utc = as.character(zentra_station_windows$end_datetime[[i]]),
        fetched_raw_rows = nrow(result_tbl),
        fetched_raw_min_utc = as.character(min(raw_timestamps, na.rm = TRUE)),
        fetched_raw_max_utc = as.character(max(raw_timestamps, na.rm = TRUE)),
        transformed_hourly_rows = NA_integer_,
        transformed_min_utc = NA_character_,
        transformed_max_utc = NA_character_,
        existing_max_before_utc = as.character(zentra_existing_max_before$existing_max_date[match(station_id, zentra_existing_max_before$station_id)]),
        candidate_new_rows = NA_integer_,
        note = "Fetched raw rows"
      )
    }
  )

  if (nrow(new_zentra_data_bind) > 0) {

  # ----------------------------------------------------------
  # Transform only the new batch
  # ----------------------------------------------------------
  new_zentra_data_bind <- new_zentra_data_bind %>%
    mutate(sensor_name = stringr::str_remove(sensor_name, ' G2')) %>%
    distinct(
      .data$timestamp,
      .data$datetime,
      .data$error_code,
      .data$station_id,
      .data$port_num,
      .data$sensor_name,
      .data$unit,
      .data$measurement,
      .keep_all = TRUE
    ) %>%
    group_by(station_id) %>%
    tidyr::pivot_wider(
      names_from = c("port_num", "sensor_name", "unit", "measurement"),
      values_from = c("value"),
      names_glue = "port_{port_num}_{sensor_name}_{unit}_{measurement}",
      values_fn = \(x) x[[1]]
    ) %>%
    ungroup() %>%
    janitor::clean_names() %>%
    mutate(
      timestamp = as.numeric(.data$timestamp),
      date_time = lubridate::with_tz(
        as.POSIXct(.data$timestamp, origin = "1970-01-01", tz = "UTC"),
        "America/Denver"
      )
    ) %>%
    select(-any_of(c(
      "datetime",
      "timestamp",
      "error_code",
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
    filter(!is.na(.data$date)) %>%
    distinct(station_id, date, .keep_all = TRUE)

  if (!nrow(new_zentra_data_bind)) {
    message("Zentra transform produced no valid hourly rows after dedupe/time conversion.")
    zentra_fetch_qc <- zentra_fetch_qc %>%
      mutate(
        transformed_hourly_rows = 0L,
        candidate_new_rows = 0L,
        note = ifelse(
          .data$fetched_raw_rows > 0,
          "Fetched raw rows but no valid hourly rows after transform",
          .data$note
        )
      )
  } else {

    db_cols <- dbListFields(con, "zentracloud_data")
    new_cols <- names(new_zentra_data_bind)

    missing_in_new <- setdiff(db_cols, new_cols)

    for (col in missing_in_new) {
      new_zentra_data_bind[[col]] <- NA
    }

    new_zentra_data_bind <- new_zentra_data_bind[, intersect(names(new_zentra_data_bind), db_cols), drop = FALSE]
    new_zentra_data_bind <- new_zentra_data_bind %>%
      mutate(
        station_id = as.character(.data$station_id),
        date = as.numeric(.data$date)
      )

    existing_recent_rows <- if (dbExistsTable(con, "zentracloud_data")) {
      min_candidate_date <- suppressWarnings(min(as.numeric(new_zentra_data_bind$date), na.rm = TRUE))
      if (!is.finite(min_candidate_date)) {
        tibble::as_tibble(stats::setNames(
          replicate(length(db_cols), logical(0), simplify = FALSE),
          db_cols
        ))
      } else {
        DBI::dbGetQuery(
          con,
          DBI::sqlInterpolate(
            con,
            sprintf(
              "SELECT %s FROM zentracloud_data WHERE date >= ?min_candidate_date",
              paste(DBI::dbQuoteIdentifier(con, db_cols), collapse = ", ")
            ),
            min_candidate_date = min_candidate_date
          )
        ) %>%
          tibble::as_tibble() %>%
          mutate(
            station_id = as.character(.data$station_id),
            date = as.numeric(.data$date)
          )
      }
    } else {
      tibble::as_tibble(stats::setNames(
        replicate(length(db_cols), logical(0), simplify = FALSE),
        db_cols
      ))
    }

    existing_recent_keys <- existing_recent_rows %>%
      transmute(
        station_id = as.character(.data$station_id),
        date = as.numeric(.data$date)
      ) %>%
      distinct(.data$station_id, .data$date)

    candidate_new_rows <- if (nrow(existing_recent_keys)) {
      dplyr::anti_join(
        new_zentra_data_bind %>%
          transmute(
            station_id = as.character(.data$station_id),
            date = as.numeric(.data$date)
          ),
        existing_recent_keys,
        by = c("station_id", "date")
      )
    } else {
      new_zentra_data_bind %>%
        transmute(
          station_id = as.character(.data$station_id),
          date = as.numeric(.data$date)
      )
    }

    merged_zentra_rows <- if (nrow(existing_recent_rows)) {
      join_cols <- c("station_id", "date")
      value_cols <- setdiff(db_cols, join_cols)

      merged <- new_zentra_data_bind %>%
        rename_with(~ paste0(.x, "__new"), all_of(value_cols)) %>%
        full_join(
          existing_recent_rows %>%
            rename_with(~ paste0(.x, "__old"), all_of(value_cols)),
          by = join_cols
        )

      for (col in value_cols) {
        merged[[col]] <- dplyr::coalesce(
          merged[[paste0(col, "__new")]],
          merged[[paste0(col, "__old")]]
        )
      }

      merged %>%
        select(all_of(db_cols)) %>%
        distinct(.data$station_id, .data$date, .keep_all = TRUE)
    } else {
      new_zentra_data_bind %>%
        select(all_of(db_cols)) %>%
        distinct(.data$station_id, .data$date, .keep_all = TRUE)
    }

    transformed_summary <- new_zentra_data_bind %>%
      group_by(.data$station_id) %>%
      summarise(
        transformed_hourly_rows = dplyr::n(),
        transformed_min_utc = as.character(as.POSIXct(min(.data$date), origin = "1970-01-01", tz = "UTC")),
        transformed_max_utc = as.character(as.POSIXct(max(.data$date), origin = "1970-01-01", tz = "UTC")),
        .groups = "drop"
      )

    candidate_summary <- candidate_new_rows %>%
      group_by(.data$station_id) %>%
      summarise(
        candidate_new_rows = dplyr::n(),
        .groups = "drop"
      )

    zentra_fetch_qc <- zentra_fetch_qc %>%
      select(-.data$transformed_hourly_rows, -.data$transformed_min_utc, -.data$transformed_max_utc, -.data$candidate_new_rows, -.data$note) %>%
      left_join(transformed_summary, by = "station_id") %>%
      left_join(candidate_summary, by = "station_id") %>%
      mutate(
        transformed_hourly_rows = dplyr::coalesce(.data$transformed_hourly_rows, 0L),
        candidate_new_rows = dplyr::coalesce(.data$candidate_new_rows, 0L),
        note = dplyr::case_when(
          .data$fetched_raw_rows == 0 ~ "No rows returned from API",
          .data$candidate_new_rows == 0 ~ "Fetched rows duplicate existing station/date keys",
          TRUE ~ "New candidate rows available"
        )
      )

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
      # Incremental merge via staging table
      # --------------------------------------------------------
      dbWriteTable(
        con,
        "zentracloud_stage",
        merged_zentra_rows,
        overwrite = TRUE,
        temporary = TRUE
      )

      common_cols <- colnames(merged_zentra_rows)
      col_sql <- paste(DBI::dbQuoteIdentifier(con, common_cols), collapse = ", ")

      sql <- sprintf("
        INSERT INTO zentracloud_data (%s)
        SELECT %s
        FROM zentracloud_stage
      ", col_sql, col_sql)

      dbWithTransaction(con, {
        dbExecute(con, "
          DELETE FROM zentracloud_data
          WHERE EXISTS (
            SELECT 1
            FROM zentracloud_stage
            WHERE zentracloud_stage.station_id = zentracloud_data.station_id
              AND zentracloud_stage.date = zentracloud_data.date
          )
        ")
        dbExecute(con, sql)
      })

      message(sprintf(
        "Merged %d ZentraCloud rows into 'zentracloud_data'.",
        nrow(merged_zentra_rows)
      ))
    }
    }

  } else {
    message("No new ZentraCloud rows returned.")
  }

  if (nrow(zentra_fetch_qc)) {
    utils::write.csv(
      zentra_fetch_qc,
      file.path(output_dir, "zentra_fetch_qc.csv"),
      row.names = FALSE
    )
  }

  if (dbExistsTable(con, "zentracloud_data")) {
    zentra_qc <- DBI::dbGetQuery(
      con,
      paste(
        "WITH ordered AS (",
        "  SELECT",
        "    CAST(station_id AS TEXT) AS station_id,",
        "    date,",
        "    LEAD(date) OVER (PARTITION BY station_id ORDER BY date) AS next_date",
        "  FROM zentracloud_data",
        ")",
        "SELECT",
        "  station_id,",
        "  MAX((next_date - date) / 3600.0) AS max_gap_hours,",
        "  SUM(CASE WHEN (next_date - date) > 7200 THEN 1 ELSE 0 END) AS gap_count_over_2h,",
        "  datetime(MIN(date), 'unixepoch') AS min_utc,",
        "  datetime(MAX(date), 'unixepoch') AS max_utc",
        "FROM ordered",
        "WHERE next_date IS NOT NULL",
        "GROUP BY station_id",
        "ORDER BY station_id"
      )
    )

    if (nrow(zentra_qc)) {
      utils::write.csv(
        zentra_qc,
        file.path(output_dir, "zentra_qc_gaps.csv"),
        row.names = FALSE
      )
    }
  }
  } else {
    message("Skipping ZentraCloud ingest for this source-isolated repair.")
  }
} else {
  message("Skipping raw ingest stage because SOFU_STAGE_INGEST=0.")
}

if (run_stage_daily_stats) {
  refresh_daily_stats_table(
    con = con,
    raw_table = "synoptic_fems_data",
    stats_table = "synoptic_fems_daily_stats",
    source_name = "Synoptic/FEMS",
    metric_cols = c(
      "air_temp_set_1.mean",
      "relative_humidity_set_1.mean",
      "wind_speed_set_1.mean",
      "precip_accum_set_1.mean",
      "precip_accum_24_hour_set_1.mean",
      "precip_accum_one_hour_set_1.mean",
      "soil_moisture_set_1.mean",
      "soil_moisture_set_2.mean",
      "gsi",
      "kbdi"
    ),
    sum_metric_cols = c("precip_accum_one_hour_set_1.mean"),
    lookback_days = synoptic_stats_lookback_days
  )

  if (length(establish_station_ids)) {
    refresh_daily_stats_table(
      con = con,
      raw_table = "synoptic_fems_data",
      stats_table = "synoptic_fems_daily_stats",
      source_name = "Synoptic/FEMS",
      metric_cols = c(
        "air_temp_set_1.mean",
        "relative_humidity_set_1.mean",
        "wind_speed_set_1.mean",
        "precip_accum_set_1.mean",
        "precip_accum_24_hour_set_1.mean",
        "precip_accum_one_hour_set_1.mean",
        "soil_moisture_set_1.mean",
        "soil_moisture_set_2.mean",
        "gsi",
        "kbdi"
      ),
      sum_metric_cols = c("precip_accum_one_hour_set_1.mean"),
      lookback_days = synoptic_stats_lookback_days,
      station_ids = establish_station_ids,
      force_full_backfill = TRUE
    )
  }

  refresh_daily_stats_table(
    con = con,
    raw_table = "zentracloud_data",
    stats_table = "zentracloud_daily_stats",
    source_name = "Zentra",
    metric_cols = c(
      "port_1_atmos_41_f_air_temperature",
      "port_1_atmos_41_percent_relative_humidity",
      "port_1_atmos_41_mph_wind_speed",
      "port_1_atmos_41_in_precipitation",
      "port_1_atmos_41_k_pa_vpd",
      "port_2_teros_11_percent_water_content",
      "port_3_teros_11_percent_water_content"
    ),
    lookback_days = zentra_stats_lookback_days
  )
} else {
  message("Skipping daily stats refresh because SOFU_STAGE_DAILY_STATS=0.")
}

if (run_stage_computed_indices) {
  append_computed_gsi_stats(
    con = con,
    raw_table = "synoptic_fems_data",
    stats_table = "synoptic_fems_daily_stats",
    source_name = "Synoptic/FEMS",
    temp_col = "air_temp_set_1.mean",
    station_lookup = sites_in_cg,
    station_id_col = "station_id",
    wims_col = "WIMS_ID",
    only_non_wims = TRUE,
    rh_col = "relative_humidity_set_1.mean"
  )

  append_computed_gsi_stats(
    con = con,
    raw_table = "zentracloud_data",
    stats_table = "zentracloud_daily_stats",
    source_name = "Zentra",
    temp_col = "port_1_atmos_41_f_air_temperature",
    station_lookup = sites_in_cg,
    station_id_col = "station_id",
    vpd_col = "port_1_atmos_41_k_pa_vpd"
  )

  append_computed_kbdi_stats(
    con = con,
    raw_table = "synoptic_fems_data",
    stats_table = "synoptic_fems_daily_stats",
    source_name = "Synoptic/FEMS",
    temp_col = "air_temp_set_1.mean",
    station_lookup = sites_in_cg,
    prism_raster_path = prism_annual_ppt_path,
    station_id_col = "station_id",
    wims_col = "WIMS_ID",
    only_non_wims = TRUE,
    precip_mode = "synoptic"
  )

  append_computed_kbdi_stats(
    con = con,
    raw_table = "zentracloud_data",
    stats_table = "zentracloud_daily_stats",
    source_name = "Zentra",
    temp_col = "port_1_atmos_41_f_air_temperature",
    station_lookup = sites_in_cg,
    prism_raster_path = prism_annual_ppt_path,
    station_id_col = "station_id",
    precip_mode = "zentra"
  )
} else {
  message("Skipping computed GSI/KBDI refresh because SOFU_STAGE_COMPUTED_INDICES=0.")
}

if (run_stage_percentiles) {
  if (!only_establish_derived || !length(establish_station_ids)) {
    refresh_daily_percentiles_table(
      con = con,
      stats_table = "synoptic_fems_daily_stats",
      percentiles_table = "synoptic_fems_daily_percentiles",
      source_name = "Synoptic/FEMS",
      lookback_days = percentiles_lookback_days
    )
  }

  if (length(establish_station_ids)) {
    refresh_daily_percentiles_table(
      con = con,
      stats_table = "synoptic_fems_daily_stats",
      percentiles_table = "synoptic_fems_daily_percentiles",
      source_name = "Synoptic/FEMS",
      lookback_days = percentiles_lookback_days,
      station_ids = establish_station_ids,
      force_full_backfill = TRUE
    )
  }

  if (!only_establish_derived || !length(establish_station_ids)) {
    refresh_daily_percentiles_table(
      con = con,
      stats_table = "zentracloud_daily_stats",
      percentiles_table = "zentracloud_daily_percentiles",
      source_name = "Zentra",
      lookback_days = percentiles_lookback_days
    )
  }
} else {
  message("Skipping percentile refresh because SOFU_STAGE_PERCENTILES=0.")
}



# copy file for backup

file.copy("data/sofu.sqlite", paste0("data/sofu_", Sys.Date(), ".sqlite"))

# =============================================================================
# 5. DISCONNECT
# =============================================================================

if (DBI::dbIsValid(con)) DBI::dbDisconnect(con)
message("Script finished and database connection closed.")

writeLines(
  paste("Workflow completed at", Sys.time()),
  file.path(output_dir, "workflow_status.txt")
)

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
  delete_source_rows <- function(station_ids) {
    station_ids <- unique(as.character(station_ids))
    station_ids <- station_ids[!is.na(station_ids) & nzchar(station_ids)]
    if (!length(station_ids)) {
      return(invisible(NULL))
    }

    station_filter <- paste(DBI::dbQuoteString(con, station_ids), collapse = ", ")
    DBI::dbExecute(
      con,
      sprintf(
        paste(
          "DELETE FROM %s",
          "WHERE metric_name = 'gsi'",
          "AND source = %s",
          "AND CAST(station_id AS TEXT) IN (%s)"
        ),
        DBI::dbQuoteIdentifier(con, stats_table),
        DBI::dbQuoteString(con, paste0(source_name, " computed")),
        station_filter
      )
    )
  }
