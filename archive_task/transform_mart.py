from helper_functions.format_json import format_json
from helper_functions.insert_read_db import insert_to_db
import pandas as pd
from vars import ENGINE

def insert_mart_to_db(RAW_JSON_URL):
    """
    :return: inserts mart to db
    """

    def create_mart(list_of_columns):
        """
        :param list_of_columns: list of columns needed
        :return: Returns filtered DataFrame for Mart
        """
        formated_json = format_json(raw_json_url=RAW_JSON_URL)

        df = pd.DataFrame(formated_json)
        mart = df.filter(items=list_of_columns)

        return mart

    def insert_db():
        """
        :return: inserts mart to db
        """

        position = create_mart(['latitude', 'longitude', "timestamp"])
        distance = create_mart(['velocity', "timestamp"])
        height = create_mart(['altitude', "timestamp"])
        footprint = create_mart(['footprint', "timestamp"])
        visibility = create_mart(['visibility', 'timestamp'])
        trajectory = create_mart(['latitude', 'longitude', "velocity", "timestamp"])

        variable_table = {"satelite_position": position,
                          "satelite_distance": distance,
                          "satelite_height": height,
                          "satelite_footprint": footprint,
                          "satelite_visibility": visibility,
                          "satelite_trajectory": trajectory,
                          }

        for table, variable in variable_table.items():
            try:
                insert_to_db(variable, table, engine=ENGINE)
            except Exception as e:
                raise IOError(f"{e}")

    try:
        insert_db()
    except Exception as e:
        raise IOError(f"{e}")

    return True
