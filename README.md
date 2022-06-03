# Runops FDW

The Runops FDW is a Multicorn Python Wrapper for [PostgreSQL Foreign Data Wrapper (FDW)](https://wiki.postgresql.org/wiki/Foreign_data_wrappers).
Its purpose is to facilitate access to remote data that can only be accessed through Runops.
This project is *not* a server simulator and will not help you if you need to create and test queries in MySQL or Postgres.
It will just provide a SQL interface for interacting with Runops.

# Set up and running

First, start by installing the runops cli. More details [here](https://runops.io/docs/developers/#setup).
Execute the login command:
```
$ echo "your@email.com" | runops login
```

Then
```
$ docker compose up --build -d
```

Now you have a full postgres server up and running.

# What you can do?

You can execute single postgres' sql queries in order to interact with runops.
If you want to see your targets, just run:
```postgresql
select * from runops.targets;

-- +-----------------------+--------------+-----------+------+------------+--------------------------+------+------------+----------------+---------------+---------+
-- |name                   |tags          |review_type|redact|type        |created                   |status|channel_name|groups          |message        |reviewers|
-- +-----------------------+--------------+-----------+------+------------+--------------------------+------+------------+----------------+---------------+---------+
-- |datalake               |NULL          |none       |none  |postgres-csv|2021-02-23 13:27:00 +00:00|active|NULL        |                |Datalake Target|NULL     |
-- |read-pgsqldb-production|2638-us-east-2|none       |none  |postgres    |2022-04-11 17:08:00 +00:00|active|NULL        |{admin,cloudops}|NULL           |NULL     |
-- |read-mysql-local       |3446-us-west-1|none       |none  |mysql       |2022-02-14 16:29:00 +00:00|active|NULL        |{admin,cloudops}|NULL           |NULL     |
-- |read-mysql-local-latam |3446-us-west-1|none       |none  |mysql       |2022-02-14 16:30:00 +00:00|active|NULL        |{admin,cloudops}|NULL           |NULL     |
-- +-----------------------+--------------+-----------+------+------------+--------------------------+------+------------+----------------+---------------+---------+

```

If you want to see your tasks, you can run:
```postgresql
select * from runops.tasks;

-- +-----+-----------+------+----------------+--------+---------------------------------+-------+---------------+------------------+
-- |id   |description|redact|script          |type    |created                          |status |elapsed_time_ms|target            |
-- +-----+-----------+------+----------------+--------+---------------------------------+-------+---------------+------------------+
-- |97803|NULL       |none  |select version()|postgres|2022-06-01 04:43:00.000000 +00:00|success|166            |read-db-production|
-- +-----+-----------+------+----------------+--------+---------------------------------+-------+---------------+------------------+
```

# Importing MySql and Postgres targets as foreign schemas

You can import a MySql or Postgres target as a foreign schema and access their data directly.

```postgresql
create schema IF NOT EXISTS my_sql;
IMPORT FOREIGN SCHEMA local_production FROM SERVER runops_fdw INTO my_sql
    options (target 'read-mysql-local', limit '500');
```

This will import all tables from the database `local_production` and create them as foreign tables into `my_sql` schema, using the target `read-mysql-local`.
The option `limit '500'` will be the default fetch limit for the foreign tables in this schema.

Now it is possible to run postgres' queries to fetch data from `local_production` database and see then in your favorite PG Client, like datagrip, phpstorm, etc.

```postgresql
select * from my_sql.activity;

-- +--+-----------------+-----------------+--------------------------+--------------------------+
-- |id|name             |description      |created_at                |updated_at                |
-- +--+-----------------+-----------------+--------------------------+--------------------------+
-- |1 |Physical goods   |Physical goods   |2021-08-05 17:35:01 +00:00|2021-08-05 17:35:01 +00:00|
-- |2 |Online Services  |Online Services  |2021-08-05 17:35:01 +00:00|2021-08-05 17:35:01 +00:00|
-- |3 |Games            |Games            |2021-08-05 17:35:01 +00:00|2021-08-05 17:35:01 +00:00|
-- |4 |PSP              |PSP              |2021-08-05 17:35:01 +00:00|2021-08-05 17:35:01 +00:00|
-- +--+-----------------+-----------------+--------------------------+--------------------------+
```

There is a helper procedure to help you to import all schemas from a desired target. For example:
```postgresql
call create_schemas_from_target('read-pgsqldb-production');
```
It will import all schemas and tables from the target `read-pgsqldb-production` and create then as foreign schemas and tables locally.
This function accept 3 parameters, as follows:
- `target (text)` the runops target to be used
- `limit_to (int) default 500` limit that will be applied for every query in foreign tables. Default value is 500
- `map (json) default Null` a single json object to map names from one schema (remote) to another schema (local).

**Another example:** If you want to import from both MySql and PostgreSQL targets that starts with `read` and not finishes with `staging`, all their schamas and tables, you can just run:
```postgresql
do $body$
declare
    t record;
begin
    for t in select name, type from runops.targets where type in ('mysql', 'postgres') and name ~~ 'read%' and name !~~ '%staging'
    loop
        call create_schemas_from_target(t.name, map => $${"pay_argentina": "pay_latam", "local_production": "pay_local"}$$::json);
    end loop;
end
$body$;
```
The map attribute means that the remotes schemas/databases `pay_argentina` and `local_production` will be imported into `pay_latam` and `pay_local` respectively.
Every schema/database that doesn't have a key in map, will be imported in a schema with the same name. 

# Pushdown of join and aggregation

Although you can use _join_ and _aggregation_ into your queries, by now, we don't have implemented [pushdown of join and aggregation](https://www.enterprisedb.com/blog/postgresql-aggregate-push-down-postgresfdw) queries yet.
This means that the Runops FDW will fetch the data from the desired tables separately, and then perform the join or the aggregation in the local postgres server.

However, you can do funny things like perform joins between 2 or more different databases, just doing:
```postgresql
select ri.id, recipient_code, ri.name, mr.merchant_id, mr.updated_at, mr.active 
from pay_latam.merchant_recipient mr
join cerberus_local_latam.recipient_info ri using(recipient_code);
```
