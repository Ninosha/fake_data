import requests
import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')


def get_songs(path, access_token):
    def fetch_data():

        headers = {"Accept": "application/json", "Content-Type": "application/json",
                   "Authorization": f"Bearer {access_token}",

       }

        try:
            response = requests.get(path)
            r = requests.get(path, data=response, headers=headers, params={"scope": ["user-read-recently-played"]})

            res = r.json()
            print(res)
            return res

        except IOError as e:
            raise e

    def create_dataframe():

        res = fetch_data()

        try:
            json_data = res["items"]
            res = pd.json_normalize(json_data)

            df = pd.DataFrame(res)

            df1 = pd.DataFrame()
            artist = df["track.album.artists"]
            df_artists = pd.DataFrame()
            df_artists["album_artists"] = [artist[i][0]["name"] for i in range(len(artist))]

            df1["date"] = df["played_at"]
            df1["album_id"] = df["track.album.id"]
            df1["album_name"] = df['track.album.name']
            df1["album_artist"] = df_artists["album_artists"]

            df = pd.DataFrame(res)
            df2 = pd.DataFrame()
            track_artist = df["track.artists"]
            track_artists = pd.DataFrame()
            track_artists["track_artists"] = [track_artist[i][0]["name"] for i in range(len(track_artist))]
            df2["date"] = df["played_at"]
            df2["track_id"] = df["track.id"]
            df2["track_name"] = df['track.name']
            df2["track_artist"] = track_artists["track_artists"]
            df2["track_popularity"] = df["track.popularity"]

            return df1, df2

        except IOError as e:
            raise e

    def partitioning():

        album, track = create_dataframe()

        album.to_sql("albums", con=ENGINE, if_exists="append", index=False)
        track.to_sql("tracks", con=ENGINE, if_exists="append", index=False)

    partitioning()
