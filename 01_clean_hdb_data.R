library(data.table); library(tidyverse); library(ggplot2)
library(lubridate); library(scales); library(jsonlite)

### Download HDB Data ###
# Function to Download HDB Data
download_hdb_data <- function(dest) {
  
  # Pull data
  base_url = "https://data.gov.sg/api/action/datastore_search?resource_id="
  resource_ids = c("1b702208-44bf-4829-b620-4615ee19b57c",
                   "83b2fc37-ce8c-4df4-968b-370fd818138b",
                   "8c00bf08-9124-479e-aeca-7cc411d884c4")
  list_results = list()
  
  # Iterate through resource_ids
  for (i in 1:length(resource_ids)) {
    id = resource_ids[i]
    url = paste0(base_url, id)
    
    # Download Data
    result_limit = fromJSON(url)$result$total
    message("Downloading ", result_limit, " records...")
    url_amended = paste0(url, "&limit=", result_limit)
    list_results[[i]] = fromJSON(url_amended)$result$records
  }
  
  # Combine Results and Sort
  hdb = rbindlist(list_results, use.names=T, fill=T)
  setorder(hdb, month)
  
  # Write raw data
  fwrite(hdb, dest, na="NA")
}

# Download Data
download_hdb_data("data/hdb_raw.csv")

### Clean data ###
hdb <- fread("data/hdb_raw.csv")

# Clean date
hdb[, date := ymd(paste0(month, "-01"))]
hdb[, year := year(date)]

# Clean flat_type
hdb[, flat_type := case_when(flat_type == "1 ROOM" ~ "1R",
                             flat_type == "2 ROOM" ~ "2R",
                             flat_type == "3 ROOM" ~ "3R",
                             flat_type == "4 ROOM" ~ "4R",
                             flat_type == "5 ROOM" ~ "5R",
                             flat_type == "EXECUTIVE" ~ "Executive",
                             flat_type == "MULTI-GENERATION" ~ "Multi-Gen")]


### Create address_mapping ###
# Clean address
hdb[, address := paste0(block, " ", street_name)]
hdb[, address := gsub("TEBAN GDNS", "TEBAN GARDENS", address)]
hdb[, address := gsub("C'WEALTH", "COMMONWEALTH", address)]
hdb[, address := gsub("BEDOK NTH", "BEDOK NORTH", address)]
hdb[, address := gsub(" LOR ", " LORONG ", address)]
hdb[, address := gsub(" RD", " ROAD", address)]
hdb[, address := gsub("ST. GEORGE'S", "ST GEORGES", address)]
hdb[, address := gsub(" AVE ", " AVENUE ", address)]
hdb[, address := gsub("KG ", "KAMPONG ", address)]
hdb[, address := gsub(" CTRL ", " CENTRAL ", address)]

unique(grep(" CTRL ", hdb$street_name, value=T))

# Get list of unique addresses
address_mapping <- hdb[, unique(.SD[, c("address", "town"), with=F])]
setorder(address_mapping, town, address)

# Function to Geocode address
geocode_address <- function(list_address) {
  list_query = gsub(" ", "+", list_address)
  n = length(list_address)
  results = list("returned_address" = vector("character", n),
                 "lon" = vector("numeric", n),
                 "lat" = vector("numeric", n),
                 "postal" = vector("character", n))
  
  # Iterate through addresses
  for (i in 1:length(list_address)) {
    address = list_address[i]
    query = list_query[i]
    geom = "Y"
    add_details = "Y"
    page = 1
    base_url = "https://developers.onemap.sg/commonapi/search?"
    final_url = paste0(base_url, "searchVal=", query,
                       "&returnGeom=", geom,
                       "&getAddrDetails=", add_details,
                       "&pageNum=", page)
    
    # Try fetching geocoded details up to 5 times
    success = 0
    num_attempts = 0
    while (success == 0 & num_attempts <= 5) {
      num_attempts = num_attempts + 1
      tryCatch({
        
        # Fetch geocoding
        geocoded = fromJSON(final_url, flatten=TRUE)
        if (geocoded$found != 0) {
          results[["returned_address"]][i] = geocoded$results$ADDRESS[1]
          results[["lon"]][i] = geocoded$results$LONGITUDE[1]
          results[["lat"]][i] = geocoded$results$LATITUDE[1]
          results[["postal"]][i] = geocoded$results$POSTAL[1]
          success = 1
        } 
      }, error = function(err) {
        message("Error occurred: ", err)
        message("The row is: ", counter, " and the query is ", address)
        message("Trying again, attempt ", num_attempts)
      }) 
    }
    
    # Now append the results
    if (success == 0) {
      results[["returned_address"]][i] = NA
      results[["lon"]][i] = NA
      results[["lat"]][i] = NA
      results[["postal"]][i] = NA
    }
    
    if(i %% 50 == 0) message("The counter is ", i)
  }
  
  return(results)
}

# Geocode Addresses
address_mapping[, c("returned_address", "lon", "lat", "postal") := geocode_address(address)]
fwrite(address_mapping, "data/hdb_address_mapping.csv", na="NA")


### Create hdb_median_price_by_town ###
hdb_median_price_by_town <- hdb[, .(median_price = median(resale_price, na.rm=T)), 
                                by=.(year, town, flat_type)]
setorder(hdb_median_price_by_town, year, town, flat_type)
fwrite(hdb_median_price_by_town, "data/hdb_median_price_by_town.csv", na="NA")
