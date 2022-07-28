import json
import re
import requests

from runops import api
from multicorn import ColumnDefinition, utils
from http import HTTPStatus
from pypika import Field, Criterion


def get_task_logs_data(task_logs):
    r = requests.get(task_logs)
    return r.text


def read_tsv(columns, data):
    body = data.split('\n', 1)[1]
    body = re.sub(r'(\n\(\d+ rows\))?\n$', '', body)
    values = list(columns.values())
    for row in body.split('\n'):
        column_data = row.split('\t')
        if len(values) > len(column_data):
            continue
        yield {values[i].column_name: to_column_type(values[i], column_data[i]) for i in range(len(values))}


def read_json(data):
    body = data.split('\n', 1)[1]
    body = re.sub(r'(\n\(\d+ rows\))?\n$', '', body)
    for row in body.split('\n'):
        yield json.loads(row)


def to_column_type(column: ColumnDefinition, value):
    conversion = {
        'bigint': lambda x: int(x),
        'smallint': lambda x: int(x),
        'numeric': lambda x: float(x)
    }
    null_values = ['NULL', '0000-00-00 00:00:00']

    return conversion.get(column.type_name, lambda x: x)(value) if null_values.count(value) == 0 else None


def create_task_and_return_logs(target, script, message=''):
    code, t = api.create_task(
        target=target,
        script=script,
        message=message
    )
    if code != HTTPStatus.CREATED:
        utils.log_to_postgres(f"Error: target={target}, message={t['message']}", utils.ERROR)
    utils.log_to_postgres(f"Task {t['id']} created")
    data = t['task_logs']
    if data[:5] == 'https':
        logs = api.get_task_logs(t['id'])
        data = get_task_logs_data(logs['logs_url'])

    return data


OPERATORS = {
    '=': lambda a, b: Field(a).eq(b),
    '<': lambda a, b: Field(a).lt(b),
    '>': lambda a, b: Field(a).gt(b),
    '<=': lambda a, b: Field(a).lte(b),
    '>=': lambda a, b: Field(a).gte(b),
    '<>': lambda a, b: Field(a).ne(b),
    '~~': lambda a, b: Field(a).like(b),
    '~~*': lambda a, b: Field(a).ilike(b),
    '!~~*': lambda a, b: Field(a).not_ilike(b),
    '!~~': lambda a, b: Field(a).not_like(b),
    ('=', True): lambda a, b: Field(a).isin(b),
    ('<>', False): lambda a, b: Field(a).notin(b)
}


def quals_to_criterion(quals):
    return Criterion.all([OPERATORS.get(q.operator)(q.field_name, q.value) for q in quals])
