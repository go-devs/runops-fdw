from multicorn import ColumnDefinition

from runops import api
from runops.fdw import utils, mysql


if __name__ == '__main__':
    print(mysql.get_mysql_introspect_query(None, None))
    print(mysql.get_mysql_introspect_query('limit', ['xpto']))
    print(mysql.get_mysql_introspect_query('except', ['xpto']))
