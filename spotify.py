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
TOKEN = "BQC3BHSbvU5tkBPPlN8hHIPyfst-wfNpIgv8GN4HIJkaYd4RylNjkz4QAvviRuNjh02TZUAWmuo4kQQR0FuvLcSLj-yIFAJmwr0BPjp2UdP1IONKuG9dJmq5KCgYOTVZXWV2LgRO_ZYSK0sBEB1MAC8Drt06gzPark1KciwIHTEUZxkyrerO2j5fdJyeyPyaSsrX9NPOEg"

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