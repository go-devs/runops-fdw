import runops.fdw.utils as utils

from pypika import PostgreSQLQuery, Table, EmptyCriterion, Order
from collections import OrderedDict
from multicorn import TableDefinition, ColumnDefinition
from multicorn.utils import log_to_postgres


def get_postgres_introspect_query(schema, restriction_type, restricts) -> str:
    columns = Table('columns', 'information_schema')
    query = PostgreSQLQuery.from_(columns) \
        .select('table_schema', 'table_name', 'column_name', 'data_type', 'is_nullable') \
        .where(columns.table_schema.notin(['information_schema', 'pg_catalog'])) \
        .where(columns.table_schema == schema) \
        .where(
        {
            'limit': columns.table_name.isin(restricts),
            'except': columns.table_name.notin(restricts)
        }.get(restriction_type, EmptyCriterion())) \
        .orderby('table_schema', 'table_name', 'ordinal_position')

    return str(query)


def postgres_schema_tables(target, schema, options, restriction_type, restricts):
    data = utils.create_task_and_return_logs(
        target=target['name'],
        script=get_postgres_introspect_query(schema, restriction_type, restricts),
        message='postgres database introspection'
    )

    tables = []
    columns = []
    current_table = ''
    current_schema = ''
    columns_def = OrderedDict(
        TABLE_SCHEMA=ColumnDefinition('table_schema', type_name='character varying'),
        TABLE_NAME=ColumnDefinition('table_name', type_name='character varying'),
        COLUMN_NAME=ColumnDefinition('column_name', type_name='character varying'),
        DATA_TYPE=ColumnDefinition('data_type', type_name='character varying'),
        IS_NULLABLE=ColumnDefinition('is_nullable', type_name='character varying'),
    )
    for column in utils.read_tsv(columns_def, data):
        if current_schema != column['table_schema']:
            current_schema = column['table_schema']
        if current_table != column['table_name']:
            if current_table != '':
                tables.append(TableDefinition(current_table, current_schema, columns, {
                    'target': target['name'],
                    'type': target['type'],
                    'resource': current_schema + '.' + current_table,
                    'limit': options.get('limit', '500')
                }))
                columns = []
            current_table = column['table_name']
        columns.append(
            ColumnDefinition(column_name=column['column_name'], type_name=column['data_type'])
        )
    log_to_postgres(f"Creating {len(tables)} table(s).")
    return tables


def execute_postgres_query(fdw, quals, columns, sortkeys):
    schema, table = fdw.resource.split('.')
    sql = generate_query(PostgreSQLQuery, schema, table, columns, quals, sortkeys, fdw.limit)

    data = utils.create_task_and_return_logs(fdw.target, str(sql))
    return utils.read_tsv(columns, data)


def generate_query(cls, schema, table, columns, quals, sortkeys, limit):
    sql = cls.from_(Table(table, schema)) \
        .select(*[c for c in columns]) \
        .where(utils.quals_to_criterion(quals)) \
        .limit(limit)
    for s in sortkeys or []:
        sql = sql.orderby(s.attname, order=Order.desc if s.is_reversed else Order.asc)
    log_to_postgres("QUERY: " + str(sql))
    return sql
