import string
import random


def get_random_data(column, df):
    return random.choice(df[column].values.tolist())


def get_random_char(what, number):
    return random.sample(what, number)


def check_numeric(column, df):
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
