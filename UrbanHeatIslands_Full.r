# Databricks notebook source
# DBTITLE 1,Part I: Generate Urban Heat Island Indices from Raw Data
# The following code blocks take raw weather station data from NOAA and compute daily cooling indices for each of about 300 US cities, starting on January 1, 2008 and ending on the present day.
# Notes:
# 1: This section takes 3 - 4 minutes to run
# 2: Urban heat island indices are daily values that compare the amount of cooling in a city to the average amount of cooling in reference weather stations, which generally are those stations that are close to but not in the city. These values depend heavily on the choice of weather stations for each city, both those in the city and the reference stations for the city. The way these are computed can be modified, and relevant parameters are noted. The heat island indices are very likley underestimates, primarly because many in-city weather stations are not in the city core, and often are at the city periphery, e.g., at airports. 
# 2: As noted inline, precomputed weather stations for cities and their references can be loaded to save time. Or if desired, they can be re-computed, e.g., using different values for some of the parameters used to determine reference weather stations.

# COMMAND ----------

# DBTITLE 1,Libraries
install.packages("geosphere")
library(geosphere)
library(magrittr)
library(SparkR)

# COMMAND ----------

# DBTITLE 1,Initialize Session, Load Data

# takes ~30 sec

# initialize session
sparkR.session(
  sparkConfig = list(
    'fs.azure.account.key.azureopendatastorage.blob.core.windows.net' = 'H5xcOyRSlehC/HHqJQ6qMiHnCnSyjVbhPie2zE7GE5fG2umrG2fMS7l3NX7R3sh7zCIiIv30ZaUZXKDdiARrsw=='
  )
)
wasbs_path <- 'wasbs://isdweatherdatacontainer@azureopendatastorage.blob.core.windows.net/ISDWeather/'


# full data
isdDF <- read.df(wasbs_path, source = "parquet")

# select relevant columns, remove rows with missing temperature and/or longitude data
weather.all = select(isdDF, "usaf","wban","datetime","latitude","longitude","temperature","station_name","year","day","month")
weather.all = weather.all %>%
  filter(., isNotNull(weather.all$temperature)) %>%
  filter(., weather.all$Longitude < 999)

dim(weather.all)


# COMMAND ----------

# DBTITLE 1,Calculate Daily Cooling: All Stations, All Days
# compute daily cooling as daily max - daily min
station.daily.cooling = summarize(groupBy(weather.all, weather.all$year, weather.all$month, weather.all$day, weather.all$wban), cooling = max(weather.all$temperature) - min(weather.all$temperature))

#head(station.daily.cooling)
  

# COMMAND ----------

# DBTITLE 1,Get Unique Stations/Locations
# Note: this is necessary because some stations have slight variants in lat/long over time. 
# These lat/long differences are quite minor, so simple averaging is used to compute a single location for every station

stations.unique = summarize(groupBy(weather.all, weather.all$wban), Latitude = mean(weather.all$Latitude), Longitude = mean(weather.all$Longitude))

#head(stations.unique)

# COMMAND ----------

# DBTITLE 1,Load Bounding Boxes for All Cities
# load bounding boxes
bb = read.df("/FileStore/tables/us_cities_bounding_boxes_full_clean.csv", source = "csv", header="true", inferSchema = "true")

# COMMAND ----------

# DBTITLE 1,Load City and Reference Weather Stations

# optional: a set of precomputed, as of 2/5/19, city and city reference weather stations that can be loaded from file to save time.

# If you wish to change and/or recompute which weather stations to use, use the two 'Find Weather Stations' code blocks immediately below. Otherwise skip those two code blocks.

# Stations are computed using the functions below as follows:
# City stations are weather stations within X km from the city center, where city center is defined as the center of a bounding box for each city and the distance X is the distance from the city center to the corner of the bounding box.
# Reference stations are the n weather stations closest to but outside the city stations circle plus distance d. In this case n = 5 and d = 25km. That is, reference weather stations are the 5 closest stations that are at least 25k outside the city.


# city stations
city.stations = read.df("FileStore/tables/us_cities_weather_stations.csv", source = "csv", header = "true", inferSchema = "true")

# city reference stations
reference.stations = read.df("FileStore/tables/us_cities_reference_weather_stations.csv", source = "csv", header = "true", inferSchema = "true")


# COMMAND ----------

# DBTITLE 1,Find Weather Stations for Each City

# takes ~1.5 min

# For each city, find stations that are within Xkm from the center of the city's bounding box. Here X is definied as the distance from the center of the bounding box to one of it's corners. 
# This creates a radius of distance Xkm based on the size of the bounding box, and thus is slightly larger than the bounding box. 

# get unique stations and lat/longs
stations.df = collect(stations.unique)

# cities
cities.df = collect(bb)


# find reference stations for each city
for(c in 1:nrow(cities.df)) {
  # find midpoint of bounding box and distance from corner to midpoint
  bb.point.one = c(cities.df$West[c], cities.df$North[c])
  bb.point.two = c(cities.df$East[c], cities.df$South[c])
  bb.midpoint = midPoint(bb.point.one, bb.point.two)
  bb.midpoint.dist = distHaversine(bb.point.one, bb.midpoint)

  # get distance from bounding box midpoint to all stations, order by proximity
  hav.dist = distHaversine(bb.midpoint, stations.df[,3:2])
  city.stations.temp = data.frame(CityWBAN = stations.df$wban, CityDist = hav.dist) 
  city.stations = as.DataFrame(city.stations.temp)
  city.stations = city.stations %>%
    filter(., city.stations$CityDist < bb.midpoint.dist) %>%
    distinct() %>%
    withColumn(., "City", cities.df$City[c]) %>%
    withColumn(., "State", cities.df$State[c])
  
  city.stations.df.temp = collect(city.stations)
  if(c == 1) {
    city.stations.all.df = city.stations.df.temp
  } else {
    city.stations.all.df = rbind(city.stations.all.df, city.stations.df.temp)
  }
  

}

city.stations = as.DataFrame(city.stations.all.df)

head(city.stations)

# COMMAND ----------

# DBTITLE 1,Find Reference Weather Stations for Each City

# takes ~30 sec

# For each city, this computes a set of reference weather stations for each city. The reference stations are used to compute how much each city's daily cooling 
# deviates from cooling in geographically similar, but less urban environments
# reference stations are defined as follows:
# select the number of references stations desired. Reference cooling will the average daily cooling of these stations.


# get unique stations and lat/longs
stations.df = collect(stations.unique)

# cities
cities.df = collect(bb)

# set number of reference weather stations and distance for reference stations from bounding box center
num.ref.stations = 5
dist.from.bb = 25000

# remove city stations from set of possible reference stations
stations.to.remove = which(stations.df$wban %in% city.stations.all.df$CityWBAN)
stations.df = stations.df[-stations.to.remove,]

# find reference stations for each city
for(c in 1:nrow(cities.df)) {
  # find midpoint of bounding box and distance from corner to midpoint
  bb.point.one = c(cities.df$West[c], cities.df$North[c])
  bb.point.two = c(cities.df$East[c], cities.df$South[c])
  bb.midpoint = midPoint(bb.point.one, bb.point.two)
  bb.midpoint.dist = distHaversine(bb.point.one, bb.midpoint)

  # get distance from bounding box midpoint to all stations, order by proximity
  hav.dist = distHaversine(bb.midpoint, stations.df[,3:2])
  reference.station.temp = data.frame(RefWBAN = stations.df$wban, RefDist = hav.dist) 
  reference.station.temp = reference.station.temp[order(reference.station.temp$RefDist),]
  
  # remove dupes in possible references stations due to slight differences in lat/long for same stations at different points in time
  unique_rows = !duplicated(reference.station.temp[c("RefWBAN")]) 
  reference.station.temp = reference.station.temp[unique_rows,]
  
  # select top n stations greater than min distance from bounding box center
  reference.station.temp = reference.station.temp[reference.station.temp$RefDist > (dist.from.bb + bb.midpoint.dist),]
  reference.station.temp = head(reference.station.temp, num.ref.stations)
  if(c == 1) {
    reference.stations.df = reference.station.temp
  } else {
    reference.stations.df = rbind(reference.stations.df, reference.station.temp)
  }
}

city.names.str = rep(cities.df$City[1],num.ref.stations)
for(i in 2:nrow(cities.df)) {
  city.names.str = c(city.names.str, rep(cities.df$City[i],num.ref.stations))
}

state.names.str = rep(cities.df$State[1],num.ref.stations)
for(i in 2:nrow(cities.df)) {
  state.names.str = c(state.names.str, rep(cities.df$State[i],num.ref.stations))
}

reference.stations.df = transform(reference.stations.df, RefCity = city.names.str, RefState = state.names.str)

reference.stations = as.DataFrame(reference.stations.df)

head(reference.stations)

# COMMAND ----------

# DBTITLE 1,Merge City and Reference Stations with Daily Cooling Data

# create single data frame of city and reference stations
city.stations.df = collect(city.stations)
reference.stations.df = collect(reference.stations)

names(city.stations.df) = c("wban","Distance","City","State")
names(reference.stations.df) = c("wban","Distance","City","State")

city.stations.df = transform(city.stations.df, Type = rep("city",nrow(city.stations.df)))
reference.stations.df = transform(reference.stations.df, Type = rep("reference",nrow(reference.stations.df)))

city.reference.stations.df = rbind(city.stations.df, reference.stations.df)
city.reference.stations = as.DataFrame(city.reference.stations.df)


# merge with full weather data
cooling.cities = merge(station.daily.cooling, city.reference.stations)

head(cooling.cities)


# COMMAND ----------

# DBTITLE 1,Generate Daily Cooling Summaries per City
# average each day's cooling over each reference station for specified city
  cooling.cities.summary = summarize(groupBy(cooling.cities, cooling.cities$City, cooling.cities$State, cooling.cities$year, cooling.cities$month, cooling.cities$day, cooling.cities$Type),CoolingMean = mean(cooling.cities$cooling)) %>%
    arrange(., cooling.cities$City, cooling.cities$State, cooling.cities$year, cooling.cities$month, cooling.cities$day, cooling.cities$Type) 

# restructure data frame
cooling.cities.summary.city = filter(cooling.cities.summary, cooling.cities.summary$Type == "city") %>%
  withColumnRenamed(., "CoolingMean", "CoolingMeanCity")
cooling.cities.summary.reference = filter(cooling.cities.summary, cooling.cities.summary$Type == "reference") %>%
  withColumnRenamed(., "CoolingMean", "CoolingMeanReference")

# create final data frame
cooling.cities.summary.all = merge(cooling.cities.summary.city, cooling.cities.summary.reference, by = c("City","State","year","month","day")) %>%
  drop(., c("City_y","State_y","year_y","month_y","day_y","Type")) %>%
  withColumn(., "CoolingMeanDiff", .$CoolingMeanCity - .$CoolingMeanReference)

colnames(cooling.cities.summary.all) = c("City","State","Year","Month","Day","CoolingMeanCity","CoolingMeanReference","CoolingMeanDiff")



head(cooling.cities.summary.all)

# COMMAND ----------

# DBTITLE 1,Save Daily Cooling Summaries per City
# Optional: save daily per-city cooling summaries for future use

# write.df(cooling.cities.summary.all, "/FileStore/tables/us_cities_daily_cooling_full.parquet", mode = "overwrite", source = "parquet", header = "true")

# COMMAND ----------

# DBTITLE 1,Part II: Example Analytics - Compare Heat Island Effects
# This section shows what can be done with the urban heat island indices computed above. Average effects over all cities, and per city are shown, and cities can be compared to one another and over time. In each case difference years and months can be specified. Finally, population density data are incorporated to show the relationship between density, but not really population, and urban heat islands.

# COMMAND ----------

# DBTITLE 1,R Libraries
# note: tidyverse may not be installed on all clusters. If not, run the install line, which takes a few miniutes.

#install.packages("tidyverse") # run if not installed on cluster
library(tidyverse)


# COMMAND ----------

# DBTITLE 1,Create R Data Frame
# convert to R data frame 
urban.heat.islands.df = SparkR::collect(cooling.cities.summary.all) %>%
  mutate(., Date = as.Date(paste(Year,Month,Day, sep = "-")))

head(urban.heat.islands.df)

# COMMAND ----------

# DBTITLE 1,Plot: All Cities, Urban Heat Island Effect Over Time
urban.heat.islands.yearly.mean = urban.heat.islands.df %>%
  dplyr::filter(., Year < 2019) %>%
  #dplyr::filter(., Month == 2) %>% # optional: filter to specific month of year
  mutate(., YearDate = as.Date(paste0(Year,"-01-01"))) %>%
  dplyr::group_by(YearDate) %>%
  summarise(MeanCooling = mean(CoolingMeanDiff))


ggplot(urban.heat.islands.yearly.mean, aes(x = YearDate, y = MeanCooling)) + geom_line(color = "light blue", size = 1) + geom_point(size = 3) +
  labs(x = "Year", y = "Mean City Cooling vs Reference, Degrees C, All Cities", title = "Average Daily Urban Heat Island Effect, All Cities") +
  theme(axis.title = element_text(size = 16), axis.text = element_text(size = 14), title = element_text(size = 16))


# COMMAND ----------

# DBTITLE 1,Plot: Single City Urban Heat Island Effect Over Time
urban.heat.islands.yearly = urban.heat.islands.df %>%
  dplyr::filter(., Year < 2019) %>%
  #dplyr::filter(., Month == 8) %>% # optional: filter to specific month of year
  mutate(., YearDate = as.Date(paste0(Year,"-01-01"))) %>%
  dplyr::group_by(City, YearDate) %>%
  summarise(MeanCooling = mean(CoolingMeanDiff))


## single city plot over time ##
city.name = "Los Angeles"
plot.df = urban.heat.islands.yearly %>%
  dplyr::filter(City == city.name)

ggplot(plot.df, aes(x = YearDate, y = MeanCooling, color = City)) + geom_line(size = 1) + geom_point(size = 4) +
  labs(title = paste0(city.name,": Cooling vs Reference"), y = "Mean City vs Reference Cooling", x = "Year") +
  theme(axis.text = element_text(size = 14), axis.title = element_text(size = 16), 
        title = element_text(size = 16), legend.position = "none")


# COMMAND ----------

# DBTITLE 1,Plot: Best and Worst Urban Heat Island Cities
## bar plot of 50 worst urban heat island cities for a given year ##

year.use = 2018

urban.heat.islands.year = urban.heat.islands.df %>%
  dplyr::filter(., Year == year.use) %>%
  #dplyr::filter(., Month == 8) %>% # optional: filter to specific month of year
  dplyr::group_by(City) %>%
  summarise(MeanCooling = mean(CoolingMeanDiff))

levels = urban.heat.islands.year$City[order(urban.heat.islands.year$MeanCooling)]
urban.heat.islands.year = urban.heat.islands.year %>%
  mutate(City = factor(urban.heat.islands.year$City, levels = levels)) %>%
  arrange(., MeanCooling) # sort descending to get the best 50 cities

rows.to.plot = c(1:50,(nrow(urban.heat.islands.year) - 50):nrow(urban.heat.islands.year))

ggplot(urban.heat.islands.year[rows.to.plot,], aes(x = City, y = MeanCooling, fill = MeanCooling)) + geom_col() +
  labs(y = "Cooling Index, Degrees Celsius", title = paste0("Cooling vs Reference, ", year.use)) +
  theme(axis.title = element_text(size = 16), title = element_text(size = 16), axis.text.y = element_text(size = 6), legend.position = "none") +
  scale_fill_gradient(low = "red", high = "blue") +
  coord_flip()

# COMMAND ----------

# DBTITLE 1,Plot: Urban Heat Island Effects and Population
city.populations = read.df("/FileStore/tables/us_cities_populations.csv", source = "csv", header = "true")

city.populations.df = SparkR::collect(city.populations)

year.use = 2018

urban.heat.islands.year.pop = urban.heat.islands.df %>%
  dplyr::filter(., Year == year.use) %>%
  #dplyr::filter(., Month == 8) %>% # optional: filter to specific month of year
  dplyr::group_by(City) %>%
  summarise(MeanCooling = mean(CoolingMeanDiff)) %>%
  inner_join(city.populations.df) %>%
  mutate(Population2016 = as.numeric(Population2016))

cool.pop.cor = round(cor(urban.heat.islands.year.pop$MeanCooling, as.numeric(urban.heat.islands.year.pop$Population2016)),2)

ggplot(urban.heat.islands.year.pop, aes(x = MeanCooling, y = log10(Population2016))) + geom_point() + geom_smooth(method = "lm") +
  labs(y = "Population, Log", x = "Mean Daily Cooling", title = paste0("Cooling x Population, r = ",cool.pop.cor)) +
  theme(axis.text = element_text(size = 14), axis.title = element_text(size = 16), title = element_text(size = 16))

# COMMAND ----------

# DBTITLE 1,Plot: Urban Heat Island Effect and Density
city.populations.df = SparkR::collect(city.populations)

year.use = 2018

urban.heat.islands.year.pop = urban.heat.islands.df %>%
  dplyr::filter(., Year == year.use) %>%
  #dplyr::filter(., Month == 8) %>% # optional: filter to specific month of year
  dplyr::group_by(City) %>%
  summarise(MeanCooling = mean(CoolingMeanDiff)) %>%
  inner_join(city.populations.df) %>%
  mutate(DensityKM = as.numeric(DensityKM))

cool.den.cor = round(cor(urban.heat.islands.year.pop$MeanCooling, urban.heat.islands.year.pop$DensityKM),2)

ggplot(urban.heat.islands.year.pop, aes(x = MeanCooling, y = DensityKM)) + geom_point() + geom_smooth(method = "lm") +
  labs(y = "Population Density", x = "Mean Daily Cooling", title = paste0("Cooling x Density, r = ",cool.den.cor)) +
  theme(axis.text = element_text(size = 14), axis.title = element_text(size = 16), title = element_text(size = 16))
