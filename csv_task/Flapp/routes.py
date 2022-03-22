from Flapp import app
from Flapp.helper_functions.data_generator import check_in_redis, read_csv, URL
from flask import request
from Flapp.helper_functions.db import to_databse


@app.route("/", methods=['POST', 'GET'])
def home():
    if request.method == "POST":

        # gets data from postman
        number = request.form["number"]
        dataset = request.files["dataset"]
        filename = dataset.filename

        # checks if file is csv format, saves in ../Data directory
        if dataset.content_type == "text/csv":
            dataset.save(f"app/Data/{dataset.filename}")

        # unpacks values from connection string
        conn = request.form["conn"]
        host, database, user, password, port = eval(conn).values()

        # gets psa data
        psa_data = read_csv(URL)
        psa_filename = f"psa_{filename}"

        # gets fake data
        redis_checked_data = check_in_redis(int(number), filename)

        # inserts psa data to postgres
        to_databse(host, database, user, password, port, psa_data, psa_filename)

        # inserts generated fake data to postgres
        to_databse(host, database, user, password, port, redis_checked_data, filename)

        response = "Success"
        return response, 200

    if request.method == "GET":
        response = "Success"
        return response, 200
