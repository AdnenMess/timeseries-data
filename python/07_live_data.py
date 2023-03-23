from numpy import float64, int32, string_
from pandas.core.reshape.pivot import pivot
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

normalized = json_normalize(r_string)

# timestamp format with +2.00 is very important otherwise it will not get shown on the board API returns local board
normalized['TimeStamp'] = normalized['location.localtime_epoch']. \
    apply(lambda s: dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+01:00'))
normalized.rename(columns={'location.name': 'location',
                           'location.region': 'region',
                           'current.temp_c': 'temp_c',
                           'current.wind_kph': 'wind_kph'
                           }, inplace=True)
print(normalized)
print(normalized.dtypes)

normalized.set_index('TimeStamp', inplace=True)
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
message = write_api.write(bucket='test', org='my-org', record=ex_df, data_frame_measurement_name='api')
write_api.flush()
print(message)
