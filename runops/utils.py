from collections import OrderedDict
from http import HTTPStatus
from multicorn import ColumnDefinition
from .fdw.utils import get_task_logs_data, read_tsv

from runops import api


def get_schemas_from_target(target):
    code, t = api.create_task(
        target,
        "SELECT schema_name FROM information_schema.schemata"
        " where schema_name not in ('information_schema', 'pg_catalog', 'performance_schema', 'mysql');",
        "Get all schemas"
    )
    if code != HTTPStatus.CREATED:
        raise RuntimeError(f"Error: target={target}, message={t['message']}")

    data = t['task_logs']
    if data[:5] == 'https':
        logs = api.get_task_logs(t['id'])
        data = get_task_logs_data(logs['logs_url'])

    return read_tsv(
        OrderedDict(schema_name=ColumnDefinition('schema_name', type_name='character varying')),
        data
    )
