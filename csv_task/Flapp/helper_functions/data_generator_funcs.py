from Flapp.helper_functions.generate_values import *
import csv


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


def gen_fake_data(matched_cols, not_matching_cols, random_names, df, codes_list):
    """
    function takes matched columns uses functions from generate_values module, adds generated values to matched
    columns, adds None as a value to non-matched columns, adds all the columns and values to dictionary

    :param matched_cols: dictionary of column:value of matched columns
    :param not_matching_cols: list of columns which program doesn't generate
    :param random_names: list of columns that is rendered as location, last name etc.
    :param df: pandas dataframe
    :param codes_list: list of columns that is rendered as id, password etc.
    :return: dict
    """
    fake_data_dict = {}
    for key, value in matched_cols.items():
        if key in random_names:
            fake_data_dict[value] = get_random_data(value, df)
        elif key in codes_list:
            fake_data_dict[value] = check_numeric(value, df)
        elif key == "email":
            if fake_data_dict["First name"]:
                name = fake_data_dict["First name"].lower()
                tail = get_random_data("Login email", df).split("@")[1]
                fake_data_dict['Login email'] = f"{name}@{tail}"

    for column in not_matching_cols:
        fake_data_dict[column] = None

    return fake_data_dict


def order(file, fake_data_dict):
    """
    function orders generated dictionary returned from gen_fake_data function according to original data columns
    :param file: file as pandas dataframe
    :param fake_data_dict: fake data dictionary from get_fake_data func
    :return: sorted dict
    """
    sorted = {}
    for column in file.columns:
        sorted[column] = fake_data_dict[column]
    return sorted


def multiply_rows(number, file_name, length, ordered_dict):
    """
    function reads how many line of data csv contains and multiplies newly generated rows and adds to csv file

    :param number: number user wants to multiply data
    :param file_name: str of filename
    :param length: int/length of dataframe
    :param ordered_dict: ordered dictionary from order func
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
            new_row = list(ordered_dict.values())
            csvwriter.writerow(new_row)
            multiply -= 1
