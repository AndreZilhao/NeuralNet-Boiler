import json
import os
import sys
import time
import pandas as pd
import numpy as np
from darksky import forecast
from datetime import datetime as dt

FIELDS = "time,dia,hora,temperatura,ligado,outTemperature,dewPoint,cloudCover,precipProbability,precipIntensity," \
         "visibility,uvIndex"
save_interval = 10000


def main():
    # Obtain user info and data
    with open("user.json") as userfile:
        user_data = json.load(userfile)
    with open(sys.argv[1]) as datafile:
        dfa = pd.read_csv(datafile)

    # Request params
    request_info = user_data['key'], user_data['location'][0], user_data['location'][1]

    # We traverse the data from start to finish.
    # Every time the day changes as we traverse the dataset, we make a new fetch, create and populate a dataframe, and
    # interpolate the data. Finally, we add the interpolated data to a data_set to later join with the original csv
    current_day = ''
    data_set = None
    time_series = []
    save_count = 0
    for i in range(dfa.shape[0]):
        save_count += 1
        timeslice, unix_time = format_time(pd.to_datetime(dfa['dia'][i] + dfa['hora'][i]), user_data['timestep'])
        time_series.append(unix_time)
        if not current_day == dfa['dia'][i]:
            current_day = dfa['dia'][i]
            print("Do work with", dfa['dia'][i])

            # HTTP Request and data interpolation
            result = forecast(*request_info, time=timeslice)
            day_data = interpolate_data(result, user_data['timestep'])
            if data_set is None:
                data_set = day_data
            else:
                data_set = data_set.append(day_data)
        if save_interval > 0 and save_count % save_interval == 0:
            print("Saving Checkpoint")
            save(dfa, data_set, time_series)
    save(dfa, data_set, time_series)


def interpolate_data(data, step):
    """ Parses and interpolates a data result from the API.
     Returns a pandas dataframe for a single day, with step interval in minutes """

    # Create empty dataframe with 288 intervals, named 'edf'
    index = np.arange(0, 288)
    columns = ['outTemperature',
               'dewPoint',
               'cloudCover',
               'precipProbability',
               'precipIntensity',
               'visibility',
               'uvIndex']
    edf = pd.DataFrame(np.nan, index=index, columns=columns)
    start_time = data['hourly']['data'][0]['time']
    finish_time = data['hourly']['data'][23]['time'] + 3600
    interval_step = step*60
    time_series = np.arange(start_time, finish_time, interval_step)
    edf.insert(loc=0, column='time', value=time_series)
    edf.set_index('time', inplace=True, drop=True)

    # Create dataframe from fetched data with 24 intervals, named 'rdf'
    index = np.arange(0, 24)
    columns = ['time',
               'outTemperature',
               'dewPoint',
               'cloudCover',
               'precipProbability',
               'precipIntensity',
               'visibility',
               'uvIndex']
    rdf = pd.DataFrame(np.nan, index=index, columns=columns)
    # Not all data exists in all data points. We have to check each individually. Interpolation helps with missing data
    for i in range(0, 24):
        d = data['hourly']['data'][i]
        if 'time' in d:
            rdf.at[i, 'time'] = d['time']
        if 'temperature' in d:
            rdf.at[i, 'outTemperature'] = to_celsius(d['temperature'])
        if 'dewPoint' in d:
            rdf.at[i, 'dewPoint'] = d['dewPoint']
        if 'cloudCover' in d:
            rdf.at[i, 'cloudCover'] = d['cloudCover']
        if 'precipProbability' in d:
            rdf.at[i, 'precipProbability'] = d['precipProbability']
        if 'precipIntensity' in d:
            rdf.at[i, 'precipIntensity'] = d['precipIntensity']
        if 'visibility' in d:
            rdf.at[i, 'visibility'] = d['visibility']
        if 'uvIndex' in d:
            rdf.at[i, 'uvIndex'] = d['uvIndex']
    rdf.set_index('time', inplace=True, drop=True)

    # Merge the 2 dataframes, using time as key
    df = pd.DataFrame.combine_first(edf, rdf)
    # Interpolate the data
    df = df.interpolate()
    return df


def save(dfa, data_set, time_series):
    """merges the databases, loads a database file and appends the last reads to it"""

    # Add the formated time series to be used as common index. Truncation when saving
    ndfa = dfa.truncate(after=len(time_series)-1)
    ndfa.insert(loc=0, column='time', value=time_series)
    ndfa.set_index('time', inplace=True, drop=True)

    # Join both data sets and export to file
    final_df = ndfa.join(data_set)

    if not os.path.isfile("darkskyDB.csv"):
        with open("darkskyDB.csv", 'a') as f:
            f.write(FIELDS)
    with open("darkskyDB.csv") as datafile:
        db = pd.read_csv(datafile, index_col=0)
        db = db.append(final_df)
        db.sort_index(inplace=True)
        db.drop_duplicates(['hora'], inplace=True)
        db.to_csv("darkskyDB.csv")


def to_celsius(temperature):
    """Converts fahrenheit temperature to celsius"""
    return (temperature - 32) * 5 / 9


def format_time(datetime, step_interval):
    """Rounds the received time to the nearest 'step_interval' in minutes"""
    step = step_interval*60
    t = time.mktime(datetime.timetuple())
    timestamp = (t+step/2)//step*step
    return dt.fromtimestamp(timestamp).isoformat(), int(timestamp)


if __name__ == '__main__':
    main()
