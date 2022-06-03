import requests
from .utils import base_request


def list_targets():
    r = requests.get(**base_request("/targets"))
    return r.status_code, r.json()


def get_target(name):
    r = requests.get(**base_request("/targets/"+name))
    return r.status_code, r.json()
