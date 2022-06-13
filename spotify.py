from email import header
from operator import imod
from wsgiref import headers
from numpy import RAISE
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
from datetime import datetime
import json
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Philipnkwam"
TOKEN = "BQDNeEPUJCxqaG12wgX0n39n3ok22nMbDQrb9AxCpd4cCGgCreNYN6Eu2SITHCVm6GsSNDnfEUe4xnaPtH0IprhU9tssPub6SqjYQz2XzOPB-JP2Zn_hzqZ0RjPZVUxCs4pF2doHgT8EKZRzITVexwNVX5r6QanBHK3_Rb20O_IoXjV3tCAlCynj7jb2hUp4H4xoM0X72w"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
     
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is Violated")

    # if df.isnull().values.any():
    #     raise Exception('Null values found')


    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned song doeas not come within the last 24 hours")

if __name__ == "__main__":

    headers= {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {TOKEN}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    r= requests.get(f"https://api.spotify.com/v1/me/player/recently-played?limit=3&after={yesterday_unix_timestamp}", headers=headers)
    data = r.json()


    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    
    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamps"])
    print(song_df)

    if check_if_valid_data(song_df):
        print("Data Valid, Proceed to load stage")


    song_df.to_csv('Data.csv')