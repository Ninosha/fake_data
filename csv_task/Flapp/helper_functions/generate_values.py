import string
import random


def get_random_data(column, df):
    """
    function get column from received file, gets values from passed column

    :param column: str
    :param df: pandas dataframe of file
    :return: list of column values
    """
    return random.choice(df[column].values.tolist())


def get_random_char(list_of_chars, number):
    """
    function gets passed number of random value from passed list

    :param list_of_chars: list
    :param number: int
    :return: random char or integer
    """
    return random.sample(list_of_chars, number)


def check_numeric(column, df):
    """
    function generates column values that are random/passwords, codes, ids

    :param column: str/file column name
    :param df: file dataframe in pandas
    :return: string/generated value
    """
    value = str(df[column][0])
    if value.isnumeric():
        random_list = random.sample(range(0, 10), len(value))
        new_id = "".join(list(map(str, random_list)))
        return new_id
    else:
        lst = ""
        for i in range(len(value)):
            if value[i].isnumeric():
                lst += ("".join(get_random_char(string.digits, 1)))
            elif value[i].isalpha() and value[i].islower():
                lst += ("".join(get_random_char(string.ascii_lowercase, 1)))
            elif value[i].isalpha() and value[i].isupper():
                lst += ("".join(get_random_char(string.ascii_uppercase, 1)))
            elif value[i].isalpha() and value[i].isupper():
                lst += ("".join(get_random_char(string.punctuation, 1)))
        return "".join(lst)
