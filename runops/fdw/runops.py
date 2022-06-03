from multicorn import TableDefinition, ColumnDefinition


def runops_schema_tables(restriction_type, restricts):
    if restriction_type is None:
        return [task_table_definition(), targets_table_definition()]
    tables = []
    compare = {
        'limit': lambda x, y: x == y,
        'restricts': lambda x, y: x != y,
    }
    tables_definition = {
        'tasks': task_table_definition,
        'targets': targets_table_definition,
    }
    for r in restricts:
        for k, t in tables_definition:
            if compare[restriction_type](r, k):
                tables.append(t)

    return tables


def task_table_definition():
    return TableDefinition(
        'tasks',
        columns=[
            ColumnDefinition('id', type_name='int8'),
            ColumnDefinition('description', type_name='varchar'),
            ColumnDefinition('redact', type_name='varchar'),
            ColumnDefinition('script', type_name='varchar'),
            ColumnDefinition('type', type_name='varchar'),
            ColumnDefinition('created', type_name='timestamptz'),
            ColumnDefinition('status', type_name='varchar'),
            ColumnDefinition('elapsed_time_ms', type_name='int8'),
            ColumnDefinition('target', type_name='varchar')
        ],
        schema='runops',
        options={
            'resource': 'tasks',
            'target': 'runops'
        }
    )


def targets_table_definition():
    return TableDefinition(
        'targets',
        columns=[
            ColumnDefinition('name', type_name='varchar'),
            ColumnDefinition('tags', type_name='varchar'),
            ColumnDefinition('review_type', type_name='varchar'),
            ColumnDefinition('redact', type_name='varchar'),
            ColumnDefinition('type', type_name='varchar'),
            ColumnDefinition('created', type_name='timestamptz'),
            ColumnDefinition('status', type_name='varchar'),
            ColumnDefinition('channel_name', type_name='varchar'),
            ColumnDefinition('groups', type_name='varchar[]'),
            ColumnDefinition('message', type_name='varchar'),
            ColumnDefinition('reviewers', type_name='varchar'),
        ],
        schema='runops',
        options={
            'resource': 'targets',
            'target': 'runops'
        }
    )
