# scripts/birdweather_daily.R

library(httr)
library(jsonlite)
library(dplyr)
library(lubridate)

# Read secrets from environment
api_key   <- Sys.getenv("BW_API_KEY")
device_id <- Sys.getenv("BW_DEVICE_ID")

if (api_key == "" || device_id == "") {
  stop("Missing BW_API_KEY or BW_DEVICE_ID environment variables.")
}

# We always fetch the previous UTC day
yesterday <- Sys.Date() - 1

start_time <- paste0(yesterday, "T00:00:00Z")
end_time   <- paste0(yesterday, "T23:59:59Z")

base_dir <- "data"
day_dir  <- file.path(base_dir, as.character(yesterday))
dir.create(day_dir, recursive = TRUE, showWarnings = FALSE)

out_file <- file.path(day_dir, "detections.csv")

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

json_txt <- content(res, as = "text", encoding = "UTF-8")
json_data <- fromJSON(json_txt, flatten = TRUE)

# json_data$data is expected to contain detections
if (is.null(json_data$data) || length(json_data$data) == 0) {
  cat("No detections returned for that day. Writing empty CSV.\n")
  write.csv(data.frame(), out_file, row.names = FALSE)
} else {
  detections <- as_tibble(json_data$data)
  write.csv(detections, out_file, row.names = FALSE)
  cat("Saved", nrow(detections), "detections to", out_file, "\n")
}
