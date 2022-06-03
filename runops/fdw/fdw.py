from abc import ABC
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres, ERROR
from http import HTTPStatus
from runops import api
from .runops import runops_schema_tables
from .mysql import mysql_schema_tables, run_mysql_task
from .postgres import postgres_schema_tables, execute_postgres_query
from collections import OrderedDict


class RunopsForeignDataWrapper(ForeignDataWrapper, ABC):

    def __init__(self, options, columns):
        super(RunopsForeignDataWrapper, self).__init__(options, columns)
        self.resource = options.get("resource")
        self.target = options.get("target")
        self.type = options.get("type")
        self.limit = options.get("limit")
        self.columns = columns

    def execute(self, quals, columns, sortkeys=None):
        log_to_postgres(f"COLUMNS: {str(columns)}")
        data = self.retrieve_data(quals, columns, sortkeys)
        for row in data:
            yield {column_name: row[column_name] for column_name in columns}
            # for column_name in columns:
            #     line[column_name] = row[column_name]
            # yield line

    def can_sort(self, sortkeys):
        return sortkeys

    @classmethod
    def import_schema(cls, schema, srv_options, options, restriction_type, restricts):
        if schema == 'runops':
            return runops_schema_tables(restriction_type, restricts)

        code, target = api.get_target(options['target'])
        if code != HTTPStatus.OK:
            throw_runops_error(code, f"Failed retrieving target {options['target']} - {target['message']}")
        if target['type'] == 'mysql':
            return mysql_schema_tables(target, options, restriction_type, restricts)
        if target['type'] == 'postgres':
            return postgres_schema_tables(target, schema, options, restriction_type, restricts)

    def get_columns_definitions(self, columns):
        od = OrderedDict()
        for c in columns:
            od[c] = self.columns[c]
        return od

    def retrieve_data(self, quals, columns, sortkeys=None):
        if self.target == 'runops':
            fetch = {
                'targets': api.list_targets,
                'tasks': api.list_tasks
            }
            status, data = fetch.get(self.resource, lambda _: [])()
            if status >= 400:
                throw_runops_error(status, data['message'])
            return data

        if self.type == 'mysql':
            return run_mysql_task(self, quals, self.get_columns_definitions(columns), sortkeys)

        if self.type == 'postgres':
            return execute_postgres_query(self, quals, self.get_columns_definitions(columns), sortkeys)


def throw_runops_error(code, msg):
    err = f"RUNOPS: {code} - {msg}"
    log_to_postgres(err, ERROR)
    raise RuntimeError(err)
