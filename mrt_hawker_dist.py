import geopandas
import pandas as pd
from geopy import distance
import logging


def clean_mrt_station(s):
    return s.lower().replace(" mrt station", "")


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logging.info("Reading SHP file")
df = geopandas.read_file("./mrt_station_data/MRTLRTStnPtt.shp")
df = df.to_crs(crs="WGS84")

df['lat'] = df['geometry'].apply(lambda p: p.y)
df['long'] = df['geometry'].apply(lambda p: p.x)

df = df[['STN_NAME', 'STN_NO', 'lat', 'long']].drop_duplicates(subset=["STN_NAME", "STN_NO"])
logging.info("Converting SHP file to CSV file")
df.to_csv("./mrt_data.csv")

# Read in Hawker Centre locations
logging.info("Reading in Hawker data file")
hawker_data_df = pd.read_json("hawker_data.json")[['hawker_name', 'hawker_coords']]

mrt_hawker_dist = []
for _, hawker_row in hawker_data_df.iterrows():
    hawker_name = hawker_row['hawker_name']
    h_coords = hawker_row['hawker_coords']

    for idx, row in df.iterrows():
        station_name = row['STN_NAME']
        station_long = row['long']
        station_lat = row['lat']
        station_num = row['STN_NO']

        dist = distance.distance(h_coords, (station_lat, station_long)).km
        mrt_hawker_dist.append({
            'hawker_name': hawker_name,
            'station_name': station_name,
            'station_name_cleaned': clean_mrt_station(station_name),
            'distance': dist,
            'station_num': station_num
        })

logging.info("Finished iterating through hawker and MRT information")
mrt_hawker_df = pd.DataFrame(mrt_hawker_dist)
mrt_hawker_df = mrt_hawker_df.sort_values(by=["hawker_name", "station_name", "station_num"])

if not mrt_hawker_df.index.is_unique:
    logging.info(mrt_hawker_df.index.value_counts())
    logging.warning("MRT/Hawker DataFrame is not unique")

logging.info("Number of Hawker/Station pairs: {}".format(len(mrt_hawker_df)))
mrt_hawker_df.to_json("./mrt_hawker_distances.json", orient='records')