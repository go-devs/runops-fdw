create or replace function list_targets(out target jsonb) returns setof jsonb as
$$
    import json
    from runops.api import list_targets
    code, data = list_targets()
    plpy.info(code)
    for d in data:
        yield (json.dumps(d))
$$ LANGUAGE plpython3u;

create or replace procedure create_schemas_from_target(in target text, in limit_to int default 500, in map json default Null) as
$$
    import json
    from runops.utils import get_schemas_from_target

    schema_map = json.loads(map if map else '{}')

    for s in get_schemas_from_target(target):
        schema_name = s['schema_name']
        to_schema = schema_map[schema_name] if schema_name in schema_map else schema_name
        plpy.info(f"importing foreign schema {schema_name} into {to_schema}")
        plpy.execute(f"create schema IF NOT EXISTS {to_schema}")
        plpy.execute(f"""
IMPORT FOREIGN SCHEMA {schema_name} FROM SERVER runops_fdw INTO {to_schema}
options (
target '{target}',
limit '{limit_to}'
);
"""
    )

$$ LANGUAGE plpython3u;
