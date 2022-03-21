from FLask_app import app
from FLask_app.helper_functions.get_data import redis_base


@app.route("/")
def home():
    result = "Add Id number as a parameter"
    return result


@app.route("/<_id>")
def get_data(_id):
    response = (redis_base(_id))
    return response
#