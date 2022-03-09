import requests

url = "https://api.wheretheiss.at/v1/satellites/25544"


def fetch_data():
    result = requests.get(url)
    res = result.json()
    return res



