# UrbanHeatIslands
Data processing and analysis notebooks for urban heat islands analysis with NOAA data via Azure IoT. 


## Overview

These notebooks compute daily urban heat island effects for most cities in the U.S., with historical data going back to January 1, 2008. The historical data set is also kept current and stored as a CSV file. The analytics examine trends over time, compare cities, and align heat island effects with population demographic data. Most of the code is written in R.


## Usage

Option #1: If you want to work with raw weather data (e.g., to change how the urban heat island effects are computed), load the UrbanHeatIsland_Full.r file into Databricks on Azure. Note the dependency files, which are included in the repo and must be in your databricks filestore.

Option #2: If you want to start with the urban heat island indices themselves (again, kept current to present day), use urban-heat-island-analytics.ipynb.
