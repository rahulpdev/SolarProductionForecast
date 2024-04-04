from datetime import datetime
import os
import requests
import pandas as pd


# Define environment constants
OCTOPUS_API_KEY = os.environ["API_KEY"]
ELECTRICITY_MPAN = os.environ["ELECTRICITY_MPAN"]
ELECTRICITY_SERIAL = os.environ["ELECTRICITY_SERIAL"]


# Define constants
OCTOPUS_URI = "https://api.octopus.energy/v1"
PVGIS_URI = "https://re.jrc.ec.europa.eu/api/v5_2"
WALLBOX_CSV = 'SessionsReport.csv'
HOURLY_PV_ENDPOINT = "/seriescalc"
TIMEZONE = 'Europe/London'
TODAY = datetime.today()
START_DATE = TODAY.replace(
   year=TODAY.year - 1, day=1, hour=0, minute=0, second=0, microsecond=0
)
END_DATE = TODAY.replace(
   month=TODAY.month, day=1, hour=0, minute=0, second=0, microsecond=0
)
LAT = 51.789115
LON = -1.532447
SOLAR_OUTPUT_START_YEAR = 2020
SOLAR_OUTPUT_END_YEAR = 2020
PEAK_POWER_KW = 3.44
# Mounting location of solar array, either free or building
MOUNTING = 'building'
# Sum of system losses, in percent. Default value is 14 percent
SYSTEM_LOSS = 14.0
ANGLE = 35.0
# Orientation (azimuth) angle of the (fixed) plane, 0=south, 90=west, -90=east
ASPECT = -35.0
METER_PARAMETERS = {
   'period_from': START_DATE,
   'period_to': END_DATE,
   'page_size': 25000,
   'group_by': 'hour',
}
HOURLY_PV_PARAMETERS = {
   'lat': LAT,
   'lon': LON,
   'startyear': SOLAR_OUTPUT_START_YEAR,
   'endyear': SOLAR_OUTPUT_END_YEAR,
   'pvcalculation': 1,
   'peakpower': PEAK_POWER_KW,
   'mountingplace': MOUNTING,
   'loss': SYSTEM_LOSS,
   'angle': ANGLE,
   'aspect': ASPECT,
   'outputformat': 'json'
}
QUANTUM_VALUE_DICT = {'energy_consumption': 'kWh'}
INTERVAL_SECONDS_DICT = {'h': 3600}
TRANSFORM_OCTOPUS_GROUP_DICT = {'hour': 'h', 'day': 'd', 'month': 'm'}
OCTOPUS_INTERVAL_GROUP = 'hour'
WALLBOX_LABELS = {
   'en': {'start': 'Start', 'duration': 'Charging time (h:m:s)', 'quantity': 'Energy (kWh)'}
}
ENERGY_UNIT = 'kWh'




# Use PVGIS API to get solar production history
response = requests.get(
   url=f"{PVGIS_URI}{HOURLY_PV_ENDPOINT}",
   params=HOURLY_PV_PARAMETERS
)
response.raise_for_status()
data = response.json()['outputs']['hourly']


# Create and format df
df_production = pd.DataFrame(data)
df_production['time'] = pd.to_datetime(
   df_production['time'].str[:-2], format='%Y%m%d:%H', utc=True
)
df_production.set_index('time', inplace=True)




# Convert session based quantities to fixed time interval quantities
def convert_sessions_to_fixed_intervals(sessions_df, sessions_quantity_label, sessions_duration_label, sessions_start_label, fixed_interval='h'):
   calculated_column = 'calculated_session_end'


   # Convert session duration to timedelta
   sessions_df[sessions_duration_label] = pd.to_timedelta(sessions_df[sessions_duration_label])


   # Calculate actual session end time with duration
   sessions_df[calculated_column] = sessions_df[sessions_start_label] + sessions_df[sessions_duration_label]


   # Create empty dataframe to store fixed interval quantities
   fixed_interval_df = pd.DataFrame(
       columns=[
           sessions_start_label,
           sessions_quantity_label
       ]
   )


   # Iterate through each session
   for _, row in sessions_df.iterrows():
       total_quantity = row[sessions_quantity_label]
       # Generate interval timestamps between the session start time and calculated end time
       fixed_timestamps = pd.date_range(
           start=row[sessions_start_label].floor(fixed_interval),
           end=row[calculated_column],
           freq=fixed_interval
       )


       # Calculate the quantity in each interval and append to new dataframe
       if len(fixed_timestamps) == 1:
           fixed_interval_df = fixed_interval_df._append(
               pd.DataFrame(
                   {
                       sessions_start_label: fixed_timestamps,
                       sessions_quantity_label: total_quantity
                   }
               )
           )
       else:
           # Calculate the average quantity per interval
           total_intervals = row[sessions_duration_label].total_seconds() / INTERVAL_SECONDS_DICT[fixed_interval]
           average_quantity = total_quantity / total_intervals


           # Calculate duration of the start interval and end interval
           start_interval_duration = (row[sessions_start_label].ceil(fixed_interval) - row[sessions_start_label]).total_seconds()
           start_interval_duration = start_interval_duration / INTERVAL_SECONDS_DICT[fixed_interval]
           end_interval_duration = (row[calculated_column] - row[calculated_column].floor(fixed_interval)).total_seconds()
           end_interval_duration = end_interval_duration / INTERVAL_SECONDS_DICT[fixed_interval]


           # Allocate proportionate quantities to the start interval and end interval
           start_interval_quantity = average_quantity * start_interval_duration
           end_interval_quantity = average_quantity * end_interval_duration


           # Append quantities for the start interval and end interval to new dataframe
           fixed_interval_df = fixed_interval_df._append(
               pd.DataFrame(
                   {
                       sessions_start_label: [fixed_timestamps[0], fixed_timestamps[-1]],
                       sessions_quantity_label: [start_interval_quantity, end_interval_quantity]
                   }
               )
           )


           if len(fixed_timestamps) > 2:
               # Allocate remaining quantity equally to the other intervals
               full_interval_quantity = (
                       (total_quantity - start_interval_quantity - end_interval_quantity) / (len(fixed_timestamps) - 2)
               )


               # Append quantities for the full intervals
               fixed_interval_df = fixed_interval_df._append(
                   pd.DataFrame(
                       {
                           sessions_start_label: fixed_timestamps[1:-1],
                           sessions_quantity_label: full_interval_quantity
                       }
                   )
               )


   # Aggregate quantity data in dataframe for each fixed interval
   fixed_interval_df = fixed_interval_df.groupby(sessions_start_label).sum().reset_index()


   return {'fixed_interval_dataframe': fixed_interval_df, 'interval': fixed_interval}




# Create Octopus energy timeseries with fixed time intervals
def create_octopus_energy_fixed_interval_df(electricity_mpan: int, electricity_serial_num, api_key, group_by_interval):
   # Declare required constants
   period_start_date = PERIOD_START_DATE
   period_end_date = PERIOD_END_DATE
   octopus_uri = OCTOPUS_URI


   # Call Octopus API to get electricity meter history
   response = requests.get(
       url=f"{octopus_uri}/electricity-meter-points/{electricity_mpan}/meters/{electricity_serial_num}/consumption/",
       params={
           'period_from': period_start_date,
           'period_to': period_end_date,
           'page_size': 25000,
           'group_by': group_by_interval,
       },
       auth=(
           api_key, ""
       )
   )
   response.raise_for_status()
   data = response.json()['results'][1:]


   # Create dataframe from raw data
   octopus_df = pd.DataFrame(data)


   # Convert start time column to datetime and convert to UTC
   octopus_df['interval_start'] = pd.to_datetime(
       octopus_df['interval_start'],
       utc=True,
       format='ISO8601'
   )
   octopus_df['interval_start'] = octopus_df['interval_start'].dt.tz_convert('UTC')


   return {
       'dataframe': octopus_df,
       'start_label': 'interval_start',
       'quantity_label': 'consumption',
       'fixed_interval': TRANSFORM_OCTOPUS_GROUP_DICT[group_by_interval]
   }




# Create Wallbox timeseries with fixed time intervals
def create_wallbox_fixed_interval_df(csv_file, timezone, time_interval='h', language_iso639='en'):
   # Declare required constants
   wallbox_quantity_label = WALLBOX_LABELS[language_iso639]['quantity']
   wallbox_duration_label = WALLBOX_LABELS[language_iso639]['duration']
   wallbox_start_label = WALLBOX_LABELS[language_iso639]['start']


   # Create raw dataframe from CSV file
   wallbox_df = pd.read_csv(csv_file)


   # Replace weird commas with periods in floating point data
   wallbox_df[wallbox_quantity_label] = wallbox_df[wallbox_quantity_label].str.replace(',', '.').astype(float)


   # Convert start time column to datetime and adjust for weird timestamp
   wallbox_df[wallbox_start_label] = pd.to_datetime(
       wallbox_df[wallbox_start_label],
       utc=False,
       format='ISO8601'
   )
   wallbox_df[wallbox_start_label] = wallbox_df[wallbox_start_label] - pd.Timedelta(hours=1)


   # Create new dataframe with energy consumption in fixed time intervals
   wallbox_new_df = convert_sessions_to_fixed_intervals(
       wallbox_df,
       wallbox_quantity_label,
       wallbox_duration_label,
       wallbox_start_label,
       time_interval
   )
   wallbox_new_df = wallbox_new_df['fixed_interval_dataframe']


   # Convert start time to localized UTC
   wallbox_new_df[wallbox_start_label] = wallbox_new_df[wallbox_start_label].dt.tz_localize(timezone)
   wallbox_new_df[wallbox_start_label] = wallbox_new_df[wallbox_start_label].dt.tz_convert('UTC')


   return {
       'dataframe': wallbox_new_df,
       'start_label': wallbox_start_label,
       'quantity_label': wallbox_quantity_label,
       'fixed_interval': time_interval
   }




def create_fixed_interval_meter(fixed_interval_df, meter_quantum, meter_interval='h'):
   # Add interval start time to dataframe index
   fixed_interval_df['dataframe'].set_index(
       fixed_interval_df['start_label'],
       inplace=True
   )


   # Remove all other columns from dataframe
   fixed_interval_df['dataframe'] = fixed_interval_df['dataframe'][[fixed_interval_df['quantity_label']]]


   # Rename quantity column to standard convention
   fixed_interval_df['dataframe'].rename(
       columns={fixed_interval_df['quantity_label']: meter_quantum},
       inplace=True
   )


   return fixed_interval_df




# Create list of dataframes
fixed_interval_df_list = []
wallbox_df = create_wallbox_fixed_interval_df(WALLBOX_CSV, TIMEZONE, 'h', 'en')
fixed_interval_df_list.append(wallbox_df)
octopus_df = create_octopus_energy_fixed_interval_df(ELECTRICITY_MPAN, ELECTRICITY_SERIAL, OCTOPUS_API_KEY, 'hour')
fixed_interval_df_list.append(octopus_df)


# Create list of meters from list of dataframes
fixed_interval_meters_list = []
for fixed_interval_df in fixed_interval_df_list:
   meter = create_fixed_interval_meter(fixed_interval_df, QUANTUM_VALUE_DICT['energy_consumption'])
   fixed_interval_meters_list.append(meter)




fixed_interval_meter_comp = pd.concat(
   [
       fixed_interval_meters_list[0]['dataframe'][QUANTUM_VALUE_DICT['energy_consumption']],
       fixed_interval_meters_list[1]['dataframe'][QUANTUM_VALUE_DICT['energy_consumption']]
   ],
   axis=1,
   keys=[
       QUANTUM_VALUE_DICT['energy_consumption'],
       QUANTUM_VALUE_DICT['energy_consumption']
   ]
)


fixed_interval_meter_comp.to_csv('consumption_comp.csv', sep=',', index=True, encoding='utf-8')




# Create meters side by side comparison
def create_side_by_side_meter_comp(meters_list):
   pass


# Calculate utility meter net of car charger meter


# Calculate utility meter net of off-peak meters


# Calculate solar meter minus utility meter net of car charger meter


# Calculate solar meter minus utility meter net of off-peak meters


# abc.to_csv('consumption_comp.csv', sep=',', index=True, encoding='utf-8')




df_hourly_production = df_production.groupby(
   by=[df_production.index.month, df_production.index.hour]
).mean()
df_hourly_production.index.names = ['month', 'hour']
print(df_hourly_production.info())


df_hourly_consumption = df_consumption.groupby(
   by=[df_consumption.index.month, df_consumption.index.hour]
).mean()
df_hourly_consumption.index.names = ['month', 'hour']
print(df_hourly_consumption.info())
print(df_hourly_consumption)


df_hourly_charger = hourly_data.groupby(
   by=[hourly_data.index.month, hourly_data.index.hour]
).mean()
df_hourly_charger.index.names = ['month', 'hour']
print(df_hourly_charger.info())
print(df_hourly_charger)


df_consumption_net_of_charger = pd.concat(
   [
       df_consumption['consumption'],
       hourly_data['quantity']
   ],
   axis=1,
   keys=['consumption kWh', 'charger kWh']
)
df_consumption_net_of_charger.index.name = 'datetime'




df_hourly_net = pd.concat(
   [
       df_hourly_production['P'].div(1000),
       df_hourly_consumption['consumption'],
       df_hourly_charger['quantity']
   ],
   axis=1,
   keys=['production kWh', 'consumption kWh', 'charger kWh']
)


# df_hourly_net.to_csv('hourlyenergy.csv', sep=',', index=True, encoding='utf-8')


# TODO Calculate hourly meter consumption net of car charger consumption
# TODO Review use of UTC in solar production data and code
# TODO Review hour of day code for only production and net consumption
# TODO Create CSV of hour of day by month with production and net consumption
# TODO Split code between files, one for Wallbox API, manipulating session based consumption, netting car charger consumption
# TODO Calculate daily peak rate time period consumption
# TODO Calculate daily battery charge
# TODO Calculate daily export
# TODO Calculate daily battery discharge net of peak rate consumption requirement
# TODO Calculate daily import
# TODO Connect to Wallbox API and retrieve car charging sessions
