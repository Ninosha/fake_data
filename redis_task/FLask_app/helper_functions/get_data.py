import redis
import requests
import json

BASE_URL = "https://jsonplaceholder.typicode.com/posts/"


def api_data(url, _id):
    """
    :param url: Base url
    :param _id: id passed from user via url parameter
    :return: if id is in fetched data: json data, if not: False
    """
    r = requests.get(url + str(_id))
    data = r.json()
    if data:
        return json.dumps(data)
    else:
        return False


def redis_base(_id):
    """
    :param _id: id passed from user via url parameter
    :return: if data in redis: data, if data not in redis: error message
    """

    data = api_data(BASE_URL, _id)

    def check_redis_data():
        """
        :return: if data in redis: gets data and returns it, if data not in redis: sets data and returns fetched data
        """
        r = redis.Redis()
        res_key = r.get(_id)
        if bool(res_key):
            print("getting from redis")
            return res_key
        else:
            print("setting in redis")
            r.set(_id, data)
            return data

    if data:
        return check_redis_data()
    else:
        return "error: data with the id doesn't exist"
#