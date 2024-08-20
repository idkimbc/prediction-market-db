import openmeteo_requests
import sqlite3
import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://climate-api.open-meteo.com/v1/climate"
params = {
	"latitude": 51.5,
	"longitude": 10.5,
	"start_date": "1950-01-01",
	"end_date": "2050-12-31",
	"models": ["CMCC_CM2_VHR4", "FGOALS_f3_H", "HiRAM_SIT_HR", "MRI_AGCM3_2_S", "EC_Earth3P_HR", "MPI_ESM1_2_XR", "NICAM16_8S"],
	"daily": ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "wind_speed_10m_mean", "wind_speed_10m_max", "cloud_cover_mean", "shortwave_radiation_sum", "relative_humidity_2m_mean", "relative_humidity_2m_max", "relative_humidity_2m_min", "dew_point_2m_mean", "dew_point_2m_min", "dew_point_2m_max", "precipitation_sum", "rain_sum", "snowfall_sum", "pressure_msl_mean", "soil_moisture_0_to_10cm_mean", "et0_fao_evapotranspiration_sum"]
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
daily_wind_speed_10m_mean = daily.Variables(3).ValuesAsNumpy()
daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()
daily_cloud_cover_mean = daily.Variables(5).ValuesAsNumpy()
daily_shortwave_radiation_sum = daily.Variables(6).ValuesAsNumpy()
daily_relative_humidity_2m_mean = daily.Variables(7).ValuesAsNumpy()
daily_relative_humidity_2m_max = daily.Variables(8).ValuesAsNumpy()
daily_relative_humidity_2m_min = daily.Variables(9).ValuesAsNumpy()
daily_dew_point_2m_mean = daily.Variables(10).ValuesAsNumpy()
daily_dew_point_2m_min = daily.Variables(11).ValuesAsNumpy()
daily_dew_point_2m_max = daily.Variables(12).ValuesAsNumpy()
daily_precipitation_sum = daily.Variables(13).ValuesAsNumpy()
daily_rain_sum = daily.Variables(14).ValuesAsNumpy()
daily_snowfall_sum = daily.Variables(15).ValuesAsNumpy()
daily_pressure_msl_mean = daily.Variables(16).ValuesAsNumpy()
daily_soil_moisture_0_to_10cm_mean = daily.Variables(17).ValuesAsNumpy()
daily_et0_fao_evapotranspiration_sum = daily.Variables(18).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}
daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
daily_data["temperature_2m_max"] = daily_temperature_2m_max
daily_data["temperature_2m_min"] = daily_temperature_2m_min
daily_data["wind_speed_10m_mean"] = daily_wind_speed_10m_mean
daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
daily_data["cloud_cover_mean"] = daily_cloud_cover_mean
daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
daily_data["relative_humidity_2m_mean"] = daily_relative_humidity_2m_mean
daily_data["relative_humidity_2m_max"] = daily_relative_humidity_2m_max
daily_data["relative_humidity_2m_min"] = daily_relative_humidity_2m_min
daily_data["dew_point_2m_mean"] = daily_dew_point_2m_mean
daily_data["dew_point_2m_min"] = daily_dew_point_2m_min
daily_data["dew_point_2m_max"] = daily_dew_point_2m_max
daily_data["precipitation_sum"] = daily_precipitation_sum
daily_data["rain_sum"] = daily_rain_sum
daily_data["snowfall_sum"] = daily_snowfall_sum
daily_data["pressure_msl_mean"] = daily_pressure_msl_mean
daily_data["soil_moisture_0_to_10cm_mean"] = daily_soil_moisture_0_to_10cm_mean
daily_data["et0_fao_evapotranspiration_sum"] = daily_et0_fao_evapotranspiration_sum

daily_dataframe = pd.DataFrame(data = daily_data)
daily_dataframe.to_csv('dailyDataframe.csv', index=False)
df = pd.read_csv('/Users/aarshochatterjee/Documents/models/prediction_market/dailyDataframe.csv')
df_cleaned = df.dropna(axis=1, how='all')
df_cleaned.to_csv('dailyDataframe.csv', index=False)

# converting the csv to sqlite db
csv_file = '/Users/aarshochatterjee/Documents/models/prediction_market/dailyDataframe.csv'
sqlite_db = 'testDatabase.db'
table_name = 'Climate'

df = pd.read_csv(csv_file)
conn = sqlite3.connect(sqlite_db)
df.to_sql(table_name, conn, if_exists='replace', index=False)