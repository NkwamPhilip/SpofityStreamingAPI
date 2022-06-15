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
TOKEN = "BQAkuCMJYBLvQW768kbhwlmilhb674Nrp9EqYnQKjczmDp0mJa34g4N8i4NgkxdE5Grq_ES6kkSf_sH7eDoDm4IjyN5UZXWzRMsZrrchfcnNeIlffWVLSmzHDpxzOnCLGafeXfA1C7JkxbBzPTukGZcocnOhEuzjEqAEfyKs6kssTzEFdmJ7a3FwHH0Sof2b44vTPJiWaw"

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=60)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df["timestamp"].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
    #         print("At least one of the returned songs does not have a yesterday's timestamp")

    # return True
if __name__ == "__main__":

    headers= {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {TOKEN}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000


    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)
    data = r.json()


    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

#Extracting only the relevant piece of data from the json object

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

#Prepare a dictionary in other to turn it to a pandas dataframe below
    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }
    
    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    print(song_df)

#Validate the dataframe

    if check_if_valid_data(song_df):
        print("Data Valid, Proceed to load stage")

#Optionally saving data locally 
    song_df.to_csv('Data.csv')


#Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )

    """

    cursor.execute(sql_query)
    print("Opened Database Succesfully")

    try:
        song_df.to_sql('my_played_tracks', engine, index=False, if_exists='append')
    except:
        print('Data Already exists in database')

    conn.close()
    print('Closed Database Successfully')



    #Job Scheduling




