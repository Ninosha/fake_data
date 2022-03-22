import os
import pandas as pd
import redis
import pyarrow as pa
from Flapp.helper_functions.data_generator_funcs import *

URL = f"app/Data/"
codes = ["id", "password", "code"]
random_names = ["first name", "last name", "department", "location"]
my_columns = ["first name", "id", "email", "last name", "password", "location", "code", "department"]


def read_csv(url):
    """
    function reads csv file in pandas dataframe
    :param url: URL of directory file gets saved
    :return: pandas dataframe
    """

    for file in os.listdir(url):
        file = pd.read_csv(f"{url}/{file}", delimiter=";")
        return file


def generate_fake_data(num, filename):
    """
    function takes received data, fakes it depending on number user passes

    :param num: integer/multiply data number
    :param filename: str/uploaded filename
    :return: calls function that fakes data
    """

    file = read_csv(URL)
    file_columns = list(file.columns)
    df = pd.DataFrame(file)
    length = len(df.index)

    matching_cols, not_matching_cols = check_if_match(my_columns, file_columns)

    faked_data_dict = gen_fake_data(matching_cols, not_matching_cols, random_names, df, codes)

    ordered_dict = order(file, faked_data_dict)

    multiply_rows(num, filename, length, ordered_dict)


def check_in_redis(num, filename):
    """
    function checks if data with passed filename is saved in redis, if not writes in redis as a key of filename,
    value as a dataframe
    :param num: int/multiply number
    :param filename: str/filename
    :return: pandas dataframe
    """

    try:
        r = redis.Redis(host="localhost", port=6379)
    except ConnectionError:
        return ConnectionError

    r.flushdb()
    context = pa.default_serialization_context()

    try:
        getted = context.deserialize(r.get(filename))
        print("I'm getting")
        return getted

    except:
        generate_fake_data(num, filename)
        df_csv = pd.read_csv(URL + filename)
        print("i'm setting")
        r.set(filename, context.serialize(df_csv).to_buffer().to_pybytes())
        return df_csv
