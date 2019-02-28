# Databricks notebook source
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


#sparkR.session(
#  sparkConfig = list(
#    'fs.azure.sas.isdweathertestcontainer.datazenforpublictest.blob.core.windows.net' = '?sv=2018-03-28&ss=b&srt=sco&sp=rl&se=2099-01-01T11:19:28Z&st=2018-12-#27T03:19:28Z&spr=https&sig=E%2FHt9cjV7JyozvByU6tFD3kUtN8v%2Fn3JpLQRTmvwu%2Fw%3D'
#  )
#)
#wasbs_path <- 'wasbs://isdweathertestcontainer@datazenforpublictest.blob.core.windows.net/ISDWeather'

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
write.df(cooling.cities.summary.all, "/FileStore/tables/us_cities_daily_cooling_full.parquet", mode = "overwrite", source = "parquet", header = "true")

# COMMAND ----------


