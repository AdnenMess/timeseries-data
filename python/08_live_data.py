from numpy import float64, int32, string_
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt

# Import the new influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# load the configuration from the json file
# with open("api_config.json") as json_data_file:
#     config = json.load(json_data_file)
#
#
# payload = {'Key': config['Key'], 'q': 'Berlin', 'aqi': 'no'}
payload = {'Key': '9999b5e5e428888f85c142633230000', 'q': 'Paris', 'aqi': 'no'}
r = requests.get("https://api.weatherapi.com/v1/current.json", params=payload)

# Get the json
r_string = r.json()
print(r_string)
print("*" * 50)

# normalize the nested json
normalized = json_normalize(r_string)

# you only get the localized time that's why timestamp format with +02.00 is very important (for Berlin) 
# otherwise TS will be in UTC and therefore in the future -> it will not get shown on the board
normalized['TimeStamp'] = normalized['location.localtime_epoch'].apply(
    lambda s: dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+01:00'))

# rename the columns
normalized.rename(columns={'location.name': 'location',
                           'location.region': 'region',
                           'current.temp_c': 'temp_c',
                           'current.wind_kph': 'wind_kph'
                           }, inplace=True)
print(normalized)
print("*" * 50)
print(normalized.dtypes)

# set the index to the new timestamp
normalized.set_index('TimeStamp', inplace=True)

# filter out just the temp and wind for export
ex_df = normalized.filter(['temp_c', 'wind_kph'])

print(ex_df)
print(ex_df.dtypes)

client = influxdb_client.InfluxDBClient(
    url='http://localhost:8086',
    token='4dBQ5ZpeBq65N7EPfegmLOxwAwmWc6P_E5R1joJ_d8wFW2tpxMr2DzBEbLKsf_s4wqe1mG4uyobDmSw2cSe_5g==',
    org='my-org',
    timeout=50000
)

# write the test data into measurement
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='live_weather', org='my-org', record=ex_df, data_frame_measurement_name='api')
write_api.flush()
print(message)
