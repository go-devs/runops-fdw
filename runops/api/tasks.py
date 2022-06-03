import requests
from .utils import base_request


def list_tasks():
    r = requests.get(**base_request("/tasks"))
    return r.status_code, r.json()


def create_task(target, script, message=None, task_type=None):
    r = requests.post(**base_request("/tasks"), json={'target': target, 'script': script, 'message': message, 'type': task_type})
    return r.status_code, r.json()


def get_task(task_id):
    return requests.get(**base_request("/tasks/" + task_id)).json()


def get_task_logs(task_id):
    return requests.get(**base_request(f"/tasks/{task_id}/logs")).json()
