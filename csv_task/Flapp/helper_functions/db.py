from sqlalchemy import create_engine


def to_databse(host, database, user, password, port, data, filename):
    """
    function inserts data to postgres database with received connection parameters

    :param host: postgres host
    :param database: postgres database
    :param user: postgres user
    :param password: postgres password
    :param port: postgres port
    :param data: dataframe
    :param filename: filename
    :return: returns error if connection not set
    """

    filename = filename[:-4]
    try:
        ENGINE = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        data.to_sql(filename, con=ENGINE, if_exists="append", index=False)

    except Exception as e:
        return f"{e}"
