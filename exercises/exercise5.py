from zipfile import ZipFile
import urllib.request
import pandas as pd
import sqlalchemy as sal
from sqlalchemy import TEXT, FLOAT, INTEGER, BOOLEAN
import os

SOURCE_URL="https://gtfs.rhoenenergie-bus.de/GTFS.zip"
ZIP_PATH="GTFS.zip"

DF_DTYPE = {
    'stop_id': INTEGER,
    'stop_name': TEXT,
    'stop_lat': FLOAT,
    'stop_lon': FLOAT,
    'zone_id': INTEGER
}

DTYPE = {
    'stop_id': 'INTEGER',
    'stop_name': 'TEXT',
    'stop_lat': 'FLOAT',
    'stop_lon': 'FLOAT',
    'zone_id': 'INTEGER'
}

def save_to_sql(df):
    engine = sal.create_engine("sqlite:///gtfs.sqlite")
    df.to_sql("stops", engine, index=False, if_exists="replace", dtype=DF_DTYPE)

def get_stops():
    df = pd.read_csv(ZipFile(ZIP_PATH).open("stops.txt"))
    return df

def prepare_data():
    urllib.request.urlretrieve(SOURCE_URL, ZIP_PATH)

def process_dataframes(df):
    df = df[["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]]
    df = df[df["zone_id"]==2001]
    df = df.query("-90 <= stop_lon <= 90")
    df = df.query("-90 <= stop_lat <= 90")
    df = df.dropna()

def main():
    prepare_data()
    stops_df = get_stops()
    process_dataframes(stops_df)
    save_to_sql(stops_df)
    os.remove(ZIP_PATH)


if __name__ == "__main__":
    main()