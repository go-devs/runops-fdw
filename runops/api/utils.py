import os.path


def base_request(path):
    return {
        "url": "https://api.runops.io/v1" + path,
        "headers": {
            "Authorization": AUTH_BEARER_JWT,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    }


def get_runops_jwt():
    config = SECRET_CONFIG if os.path.exists(SECRET_CONFIG) else DEFULT_CONFIG
    with open(config) as f:
        lines = f.readlines()
    return lines[0]


SECRET_CONFIG = "/run/secrets/runops_config"
DEFULT_CONFIG = os.path.expanduser('~') + "/.runops/config"
AUTH_BEARER_JWT = get_runops_jwt()
