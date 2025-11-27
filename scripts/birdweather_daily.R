# scripts/birdweather_daily.R

library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)

# ---- CONFIG / SECRETS ----
api_key   <- Sys.getenv("BW_API_KEY")
device_id <- Sys.getenv("BW_DEVICE_ID")

if (api_key == "" || device_id == "") {
  stop("Missing BW_API_KEY or BW_DEVICE_ID environment variables.")
}

base_dir <- "data"

# ---- TIME RANGE: previous UTC day ----
yesterday <- Sys.Date() - 1

start_time <- paste0(yesterday, "T00:00:00Z")
end_time   <- paste0(yesterday, "T23:59:59Z")

day_dir  <- file.path(base_dir, as.character(yesterday))
dir.create(day_dir, recursive = TRUE, showWarnings = FALSE)

daily_file   <- file.path(day_dir, "detections.csv")
master_file  <- file.path(base_dir, "master_detections.csv")

# ---- FETCH DATA FROM API ----
url <- paste0(
  "https://api.birdweather.com/v1/devices/", device_id,
  "/detections?start=", start_time,
  "&end=", end_time
)

cat("Requesting detections for", yesterday, "...\n")
cat("URL:", url, "\n")

res <- GET(url, add_headers(Authorization = paste("Bearer", api_key)))

if (http_error(res)) {
  stop("API request failed with status: ", status_code(res))
}

json_txt  <- content(res, as = "text", encoding = "UTF-8")
json_data <- fromJSON(json_txt, flatten = TRUE)

# ---- WRITE DAILY FILE ----
if (is.null(json_data$data) || length(json_data$data) == 0) {
  cat("No detections returned for that day. Writing empty daily CSV.\n")
  daily_df <- tibble()
  write.csv(daily_df, daily_file, row.names = FALSE)
} else {
  daily_df <- as_tibble(json_data$data)
  write.csv(daily_df, daily_file, row.names = FALSE)
  cat("Saved", nrow(daily_df), "detections to", daily_file, "\n")
}

# ---- UPDATE MASTER CSV ----

# 1. Load existing master if present
if (file.exists(master_file)) {
  master_df <- read.csv(master_file, stringsAsFactors = FALSE)
  master_df <- as_tibble(master_df)
} else {
  master_df <- tibble()
}

# 2. Bind new data
# If you might re-run workflows for the same day,
# this removes any existing rows from that date first.
if (nrow(daily_df) > 0) {
  # Try to detect the time column; adapt if your field has a different name
  time_col <- intersect(names(daily_df), c("timestamp", "created_at", "time"))[1]

  if (!is.null(time_col)) {
    # Convert to Date if possible
    daily_df <- daily_df %>%
      mutate(.day = as.Date(.data[[time_col]]))

    if (nrow(master_df) > 0 && time_col %in% names(master_df)) {
      master_df <- master_df %>%
        mutate(.day = as.Date(.data[[time_col]])) %>%
        filter(.day != yesterday) %>%
        select(-.day)
    }

    daily_df <- daily_df %>% select(-.day)
  }

  combined_df <- bind_rows(master_df, daily_df)
} else {
  combined_df <- master_df
}

# 3. Remove duplicates if there is a unique detection id
id_col <- intersect(names(combined_df), c("id", "detection_id"))[1]
if (!is.null(id_col)) {
  combined_df <- combined_df %>%
    distinct(.data[[id_col]], .keep_all = TRUE)
}

# 4. Write master file
write.csv(combined_df, master_file, row.names = FALSE)
cat("Master file updated:", master_file, " (", nrow(combined_df), "rows )\n")
