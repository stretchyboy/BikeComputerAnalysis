

# Tabs : [1,2,3] = [Training, Long Ride, Event]

'''CREATE TABLE sessions (_id integer primary key autoincrement, starttime long, sessiontime long, pauseTime long, distance float, topspeed float, average float, meters integer, overalltime long, title text, bike text, cat integer, calories integer, used_bike integer, heartavg integer, heartmax integer, pow_avg integer, pow_max integer, url text , hasTrack integer, hasUpdatedElevation integer, withBarometer )'''
# sessions : 


'''CREATE TABLE tracks (_id integer primary key autoincrement, lat integer, lon integer, elev integer, time integer, session_id integer, heartrate integer, power integer, temperature integer, cadence integer)'''
# tracks

import pandas as pd
import sqlite3

con = sqlite3.connect("data/backuped_sessions.db")
df = pd.read_sql_query("SELECT lat/1000000.0 as lat, lon/1000000.0 as lon, elev, time, temperature, distance, starttime, overalltime, meters as elevation_gain, weight as bike_weight FROM tracks JOIN sessions ON (tracks.session_id = sessions._id) JOIN bikes ON (used_bike=bikes._id) WHERE cat IN (1,2,3)", con)

# Verify that result of SQL query is stored in the dataframe
print(df)

con.close()