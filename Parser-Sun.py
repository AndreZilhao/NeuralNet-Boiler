import json
import os
import sys
import time
import pandas as pd
from pysolar.solar import *
from datetime import datetime as dt
from datetime import timezone as tz

FIELDS = "time,dia,hora,temperatura,ligado,outTemperature,dewPoint,cloudCover,precipProbability,precipIntensity," \
         "visibility,uvIndex"
save_interval = 10000
dbName = "out-sun.csv"


def main():
    # Obtain user info and data
    with open("user.json") as userfile:
        user_data = json.load(userfile)
    with open(sys.argv[1]) as datafile:
        dfa = pd.read_csv(datafile)

    current_day = ''
    sun_series = []
    save_count = 0
    for i in range(dfa.shape[0]):
        save_count += 1
        timeslice, unix_time = format_time(pd.to_datetime(dfa['dia'][i] + dfa['hora'][i]), user_data['timestep'])
        sun_angle = get_altitude(user_data['location'][0], user_data['location'][0], dt.fromtimestamp(unix_time, tz=tz.utc))
        sun_series.append(sun_angle)
        if not current_day == dfa['dia'][i]:
            current_day = dfa['dia'][i]
            print("Do work with", dfa['dia'][i])

            # Request and data interpolation
        if save_interval > 0 and save_count % save_interval == 0:
            print("Saving Checkpoint")
            save(dfa, sun_series)
    save(dfa, sun_series)


def save(dfa, time_series):
    """merges the databases, loads a database file and appends the last reads to it"""

    # Add the formated time series to be used as common index. Truncation when saving
    ndfa = dfa.truncate(after=len(time_series)-1)
    ndfa.insert(loc=0, column='sun_angle', value=time_series)
    ndfa.set_index('time', inplace=True, drop=True)

    if not os.path.isfile(dbName):
        with open(dbName, 'a') as f:
            f.write(FIELDS)
    with open(dbName) as datafile:
        db = pd.read_csv(datafile, index_col=0)
        db = db.append(ndfa)
        db.sort_index(inplace=True)
        db.drop_duplicates(subset=['hora', 'dia'], inplace=True)
        db.to_csv(dbName)


def format_time(datetime, step_interval):
    """Rounds the received time to the nearest 'step_interval' in minutes"""
    step = step_interval*60
    t = time.mktime(datetime.timetuple())
    timestamp = (t+step/2)//step*step
    return dt.fromtimestamp(timestamp).isoformat(), int(timestamp)


if __name__ == '__main__':
    main()
