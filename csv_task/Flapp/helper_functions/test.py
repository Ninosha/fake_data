import os
import pandas as pd
import csv
from Flapp.helper_functions.generate_values import *
import redis
import pyarrow as pa

URL = f"app/Data/"
ods = ["id", "password", "code"]
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

    def check_if_match(my_cols, data_cols):
        """
        checks if received columns are the columns function handles and adds to matching_dict,
        if not, columns will be added to not_matching list

        :param my_cols: list of columns the application can generate
        :param data_cols: list of columns received from csv file user uploaded
        :return: dictionary/list
        """
        matching_dict = {}
        for i in my_cols:
            for j in list(data_cols):
                if i in j.lower():
                    matching_dict[i] = j
                    data_cols.remove(j)
        not_matching = data_cols

        return matching_dict, not_matching

    matching_cols, not_matching_cols = check_if_match(my_columns, file_columns)

    def gen_fake_data(matched_cols):
        """
        function takes matched columns uses functions from generate_values module, adds generated values to matched
        columns, adds None as a value to non-matched columns, adds all the columns and values to dictionary

        :param matched_cols: list of matched columns
        :return: dict
        """
        fake_data_dict = {}
        for key, value in matched_cols.items():
            if key in random_names:
                fake_data_dict[value] = get_random_data(value, df)
            elif key in ods:
                fake_data_dict[value] = check_numeric(value, df)
            elif key == "email":
                if fake_data_dict["First name"]:
                    name = fake_data_dict["First name"].lower()
                    tail = get_random_data("Login email", df).split("@")[1]
                    fake_data_dict['Login email'] = f"{name}@{tail}"
        for column in not_matching_cols:
            fake_data_dict[column] = None
        return fake_data_dict

    def order():
        """
        function orders generated dictionary returned from gen_fake_data function according to original data columns
        :return: sorted dict
        """
        item = gen_fake_data(matching_cols)
        sorted = {}
        for column in file.columns:
            sorted[column] = item[column]
        return sorted

    def multiply_rows(number, file_name):
        """
        function reads how many line of data csv contains and multiplies newly generated rows and adds to csv file
        :param number: number user wants to multiply data
        :param file_name: str of filename
        :return: None
        """
        file_path = URL + file_name
        multiply = length * number - length

        with open(file_path, 'a') as csvfile:

            # checks if csv file ends with new line, if not adds new line
            csvfile.readline()
            lines = csvfile.readlines()
            if lines[-1][-1] != "\n":
                csvfile.write('\n')
            csvwriter = csv.writer(csvfile, delimiter=';')

            while multiply > 0:
                new_row = list(order().values())
                csvwriter.writerow(new_row)
                multiply -= 1

    multiply_rows(num, filename)


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
    except Exception as ConnectionError:
        return ConnectionError

    # r.flushdb()
    context = pa.default_serialization_context()

    try:
        getted = context.deserialize(r.get(filename))
        print("I'm getting")
        return getted

    except Exception:
        generate_fake_data(num, filename)
        df_csv = pd.read_csv(URL + filename)
        print("i'm setting")
        r.set(filename, context.serialize(df_csv).to_buffer().to_pybytes())
        return df_csv
