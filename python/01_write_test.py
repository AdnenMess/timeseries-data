import pandas as pd
import datetime as dt
from pathlib import Path

# Import the new influxdb API client (pip install influxdb_client)
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# Read the csv into a dataframe
#df = pd.read_csv("data/test.csv", )

folder = Path.cwd().parent / "Data" / "PRSA"

df = pd.read_csv(folder/"PRSA_Data_Aotizhongxin_20130301-20170228.csv", )


# create a timestamp out of the four columns
# needed for influx 2020-01-01T00:00:00.00Z
# lambda s : dt.datetime(*s) takes every row and parses it -> *s
# strftime to reformat the string into influxdb format
df['TimeStamp'] = df[['year', 'month', 'day', 'hour']].apply(lambda s : dt.datetime(*s).strftime('%Y-%m-%dT%H:%M:%SZ'),axis = 1)

# set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace = True)
# drop the year, month, day, hour, No from the dataframe
ex_df = df.drop(['year', 'month', 'day', 'hour','No'], axis=1)
print(ex_df)


Fields = ['PM2.5', 'PM10', 'SO2', 'NO2', 'CO', 'O3', 'TEMP', 'PRES', 'DEWP', 'RAIN', 'wd', 'WSPM']
# Define tag fields
datatags = ['station', 'wd']


client = influxdb_client.InfluxDBClient(
   url='http://localhost:8086',
   token='4dBQ5ZpeBq65N7EPfegmLOxwAwmWc6P_E5R1joJ_d8wFW2tpxMr2DzBEbLKsf_s4wqe1mG4uyobDmSw2cSe_5g==',
   org='my-org'
)

#write the test data into measurement
write_api = client.write_api(write_options=SYNCHRONOUS, timeout=50000)
#message = write_api.write(bucket='air_quality',org='my-org',record = ex_df, data_frame_measurement_name = 'test', data_frame_tag_columns=['station','wd'])
message = write_api.write(bucket='air-quality', org='my-org', record=ex_df,
                          data_frame_measurement_name='test', data_frame_tag_columns=['station', 'wd'])
print(message)


write_api.flush()

