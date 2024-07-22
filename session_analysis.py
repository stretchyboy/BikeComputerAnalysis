

# Tabs : [1,2,3] = [Training, Long Ride, Event]

'''CREATE TABLE sessions (_id integer primary key autoincrement, starttime long, sessiontime long, pauseTime long, distance float, topspeed float, average float, meters integer, overalltime long, title text, bike text, cat integer, calories integer, used_bike integer, heartavg integer, heartmax integer, pow_avg integer, pow_max integer, url text , hasTrack integer, hasUpdatedElevation integer, withBarometer )'''
# sessions : 


'''CREATE TABLE tracks (_id integer primary key autoincrement, lat integer, lon integer, elev integer, time integer, session_id integer, heartrate integer, power integer, temperature integer, cadence integer)'''
# tracks

import pandas as pd
import sqlite3
import numpy as np
import os
from geopy.geocoders import Nominatim
from geopy.point import Point


con = sqlite3.connect("data/backuped_sessions.db")
#df = pd.read_sql_query("SELECT session_id, lat/1000000.0 as lat, lon/1000000.0 as lon, elev, time, temperature, distance, starttime, overalltime, meters as elevation_gain, weight as bike_weight FROM tracks JOIN sessions ON (tracks.session_id = sessions._id) JOIN bikes ON (used_bike=bikes._id) WHERE cat IN (1,2,3) AND sessions._id = 34", con)
#exclude comutting ang heavy bike days
df = pd.read_sql_query("SELECT session_id, lat/1000000.0 as lat, lon/1000000.0 as lon, elev, time, temperature, distance, starttime, overalltime, meters as elevation_gain, weight as bike_weight FROM tracks JOIN sessions ON (tracks.session_id = sessions._id) JOIN bikes ON (used_bike=bikes._id) WHERE cat IN (1,2,3) AND weight <16", con)


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371000  # Radius of earth in kilometers. Use 3956 for miles
    return c * r

df['len'] = haversine(df['lat'].shift(), df['lon'].shift(), df['lat'], df['lon'])
df['dist'] = df['len'].cumsum()
df['pre_mins'] = ((df['time'] - df['starttime']) / (1000*60) ).astype(int)
df['pre_hours'] = ((df['time'] - df['starttime']) / (1000*60*60) ).astype(int)

df['grad'] = 100 * (df['elev']- df['elev'].shift()) / df['len']
df['id'] = (df['elev'].diff(10) < 0).cumsum()
#df['id'] = (df['grad'] < 0).cumsum()

df['gained'] = (df['elev'].diff()>0).cumsum()

sessions = df.groupby(['session_id']).agg(
        dist_start=('dist', "min"),
        pre_gained=('gained', "min"),
        )


# Verify that result of SQL query is stored in the dataframe


df.fillna(0, inplace=True)

#print(df.columns)

groups = df.groupby(['session_id','id']).agg(
        dist_start=('dist', "min"),
        dist_end=('dist', "max"),
        #count = ("id","count"),
        #gradient_max=('grad', "max"),
        pre_gained=('gained', "min"),
        
        ele_start=('elev', "min"),
        ele_end=('elev', "max"),
        time_start=('time', "min"),
        time_end=('time', "max"),
        #mid_lat = ("lat","mean"),
        #mid_lon = ("lon","mean"),
        #elapsed_mins = ('elapsed_mins', "min"),
        pre_mins = ('pre_mins', "min"),
        pre_hours = ('pre_hours', "min"),
        temp = ('temperature', 'mean')
    )

groups.fillna(0, inplace=True)


groups['dist_start'] = (groups['dist_start']-sessions['dist_start']).astype(int)
groups['dist_end'] = (groups['dist_end']-sessions['dist_start']).astype(int)

groups['pre_gained'] = (groups['pre_gained']-sessions['pre_gained']).astype(int)

#groups['dist_start'] = groups['dist_start'].astype(int)
#groups['dist_end'] = groups['dist_end'].astype(int)


groups = groups[groups['dist_end']>groups['dist_start']]
groups = groups[groups['ele_end']>groups['ele_start']]


groups['grad_mean'] = (100 * (groups["ele_end"]- groups["ele_start"]) / (groups['dist_end'] - groups['dist_start'])).astype(int)

groups = groups[(groups['grad_mean'] < 30)] 

groups['dur'] = ((groups["time_end"]- groups["time_start"]) / 1000).astype(int)
groups['length'] = groups['dist_end'] - groups['dist_start']
groups = groups[groups['dur'] > 0] 
groups['vam'] = (((groups["ele_end"]- groups["ele_start"]) * 60) / (groups['dur']/60)).astype(int)
groups = groups[groups['dur'] >= 60] 

groups['kph'] = (((groups['length']/1000) / (groups['dur']/(60*60)))*10).astype(int)/10
groups = groups[groups['kph'] > 5] 

groups.drop(labels=['id','dist_start', 'dist_end', 'ele_start', 'ele_end', 'time_start','time_end'], axis=1, errors='ignore', inplace=True)

groups['score'] = ((groups['length']) * groups['grad_mean']).astype(int)
groups['pre_score'] = groups['score'].cumsum() - groups['score']

sess2 = groups.groupby(['session_id']).agg(
        min_pre_score=('pre_score', "min"),
)
print(sess2)

groups['pre_score'] = groups['pre_score'] - sess2['min_pre_score']
#The filter used for the bike computer
#groups = groups[(groups['score'] > 6500)] 

groups["cat"] = pd.cut(groups['score'], [-100000, 8000, 16000, 32000, 64000, 80000, 1000000], right=True, labels=["-","4", "3", "2", "1", "HC"])

print (groups)

data = groups.reset_index(drop=True)#


#.drop(labels=["session_id", "id"])
print (data)
con.close()
exit()

means = groups.groupby(['pre_hours','grad_mean']).agg(
        kph = ("kph","mean"),
        #kph_min = ("kph","min"),
        #kph_max = ("kph","max"),
)
print(means.unstack())

# Look at normalising againt each element and then multipling up

grads = groups.groupby(['grad_mean']).agg(
        kph = ("kph","mean"),
        #kph_min = ("kph","min"),
        #kph_max = ("kph","max"),
)
print(grads)


hours = groups.groupby(['pre_hours']).agg(
        kph = ("kph","mean"),
        #kph_min = ("kph","min"),
        #kph_max = ("kph","max"),
)
print(hours)