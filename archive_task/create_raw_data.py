import pandas as pd
from fetch_data import fetch_data
from helper_functions.json_csv_read import read_json
from helper_functions.create_per_second import per_second
from vars import RAW_JSON_URL


fetched_data = fetch_data()
df = pd.DataFrame([fetched_data])


def create_raw_data():
    """
    :return: creates raw data json in PSA directory
    """

    df.to_json(RAW_JSON_URL, orient="records")

    def append_to_raw_json():

        """
        :return: joins present raw data with old raw data in pandas dataframe and creates raw_data
        """

        fetched_data = fetch_data()
        df = pd.DataFrame([fetched_data])
        df.reset_index(drop=True)
        data = read_json(RAW_JSON_URL)
        df1 = pd.DataFrame(data)
        df1.reset_index(drop=True)
        df3 = df1.append(df)
        df3.reset_index(drop=True)
        df3.to_json(RAW_JSON_URL, orient="records")

    per_second(append_to_raw_json)



