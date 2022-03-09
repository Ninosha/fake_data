import requests as rq


def fetch_data():
    """
    :return: requested data in json
    """
    api_response = rq.get("https://api.wheretheiss.at/v1/satellites/25544")
    if api_response:

        requested_data = api_response.json()

        return requested_data

    else:
        return api_response.content

