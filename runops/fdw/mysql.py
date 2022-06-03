from runops import api
from multicorn.utils import log_to_postgres, ERROR
from multicorn import TableDefinition, ColumnDefinition
from http import HTTPStatus
from collections import OrderedDict
from pypika import MySQLQuery, Schema, Table, EmptyCriterion, Order

import runops.fdw.utils as utils


def get_mysql_introspect_query(restriction_type, restricts) -> str:
    columns = Schema('INFORMATION_SCHEMA').COLUMNS
    query = MySQLQuery.from_(columns) \
        .select('TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'IS_NULLABLE', 'COLUMN_KEY') \
        .where(columns.TABLE_SCHEMA.notin(['information_schema', 'performance_schema', 'mysql'])) \
        .where(
        {
            'limit': columns.TABLE_NAME.isin(restricts),
            'except': columns.TABLE_NAME.notin(restricts)
        }.get(restriction_type, EmptyCriterion())) \
        .orderby(*['TABLE_SCHEMA', 'TABLE_NAME', 'ORDINAL_POSITION'])

    return str(query)


def mysql_schema_tables(target, options, restriction_type, restricts):
    code, t = api.create_task(
        target=options['target'],
        script=get_mysql_introspect_query(restriction_type, restricts),
        message='mysql database introspection'
    )
    if code != HTTPStatus.CREATED:
        log_to_postgres(f"Error: target={options['target']}, message={t['message']}", ERROR)
    log_to_postgres(f"Task {t['id']} created")
    data = t['task_logs']
    if data[:5] == 'https':
        logs = api.get_task_logs(t['id'])
        data = utils.get_task_logs_data(logs['logs_url'])

    tables = []
    columns = []
    current_table = ''
    current_schema = ''
    columns_def = OrderedDict(
        TABLE_SCHEMA=ColumnDefinition('TABLE_SCHEMA', type_name='character varying'),
        TABLE_NAME=ColumnDefinition('TABLE_NAME', type_name='character varying'),
        COLUMN_NAME=ColumnDefinition('COLUMN_NAME', type_name='character varying'),
        DATA_TYPE=ColumnDefinition('DATA_TYPE', type_name='character varying'),
        IS_NULLABLE=ColumnDefinition('IS_NULLABLE', type_name='character varying'),
    )
    for column in utils.read_tsv(columns_def, data):
        if current_schema != column['TABLE_SCHEMA']:
            current_schema = column['TABLE_SCHEMA']
        if current_table != column['TABLE_NAME']:
            if current_table != '':
                tables.append(TableDefinition(current_table, current_schema, columns, {
                    'target': target['name'],
                    'type': target['type'],
                    'resource': current_schema + '.' + current_table,
                    'limit': options.get('limit', '500')
                }))
                columns = []
            current_table = column['TABLE_NAME']
        columns.append(
            ColumnDefinition(column_name=column['COLUMN_NAME'], type_name=mysql_to_postgres_types(column['DATA_TYPE']))
        )
    log_to_postgres(f"Creating {len(tables)} table(s).")
    return tables


def mysql_to_postgres_types(type_name):
    map_types = {
        'datetime': 'timestamptz',
        'tinyint': 'smallint',
        'enum': 'varchar',
        'tinyblob': 'BYTEA',
        'mediumblob': 'BYTEA',
        'longblob': 'BYTEA',
        'blob': 'BYTEA',
        'double': 'double precision',
        'tinytext': 'text',
        'text': 'text',
        'mediumtext': 'text',
        'longtext': 'text',
        'year': 'integer',
        'varbinary': 'bytea',
        'binary': 'bytea',
        'mediumint': 'integer'
    }
    return map_types.get(type_name, type_name)


def run_mysql_task(fdw, quals, columns, sortkeys):
    sql = MySQLQuery.from_(Table(*fdw.resource.split('.')[::-1])) \
        .select(*[c for c in columns]) \
        .where(utils.quals_to_criterion(quals)) \
        .limit(fdw.limit)
    for s in sortkeys or []:
        sql = sql.orderby(s.attname, order=Order.desc if s.is_reversed else Order.asc)
    log_to_postgres("QUERY: " + str(sql))
    code, task = api.create_task(fdw.target, str(sql))
    if code != HTTPStatus.CREATED:
        log_to_postgres(f"Error: target={fdw.target}, message={task['message']}", ERROR)
    log_to_postgres(f"Task id: {task['id']}")
    if task['status'] != 'success':
        log_to_postgres(f"TASK ERROR: target={fdw.target}, task={task['id']}, log={task['task_logs']}", ERROR)

    data = task['task_logs']
    if data[:5] == 'https':
        log_to_postgres("Get LOG: " + str(data))
        logs = api.get_task_logs(task['id'])
        data = utils.get_task_logs_data(logs['logs_url'])

    return utils.read_tsv(columns, data)
