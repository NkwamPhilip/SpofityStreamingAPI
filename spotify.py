from email import header
from operator import imod
from wsgiref import headers
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
TOKEN = "BQC7zyBoD35ZXzBlNjG9gpX6FR_LgUdP2hqQ-BrjY-36K1OiEbDXWmv21WZzth4lU_7d-wdZMexL_Zc6AL0ItW9da6374J7flbDqS67vP2vSkQHFWTsZNDa15IdZDNT71jtGMjEqQCpKc8KlHZduLzcrE3BupAicZljCoqd4wwMd_bJIESz0I1k-wQEyvHsD9z2k4o47kw"
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

    song_df.to_csv('Data.csv')