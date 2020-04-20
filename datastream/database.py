import os
import re
from glob import glob

import psycopg2

from datastream import config

__all__ = [
    'cursor_type',
    'connection_type',
    'create_connection',
]


cursor_type = psycopg2.extensions.cursor
connection_type = psycopg2.extensions.connection


def create_connection(**kws):
    """ Create postgres connection. 


    This function exists as a helper it can provide helpful defaults from
    the configuration.


    ```
    conn = psycopg2.connect(DSN)

    with conn:
        with conn.cursor() as curs:
            curs.execute(SQL1)

    with conn:
        with conn.cursor() as curs:
            curs.execute(SQL2)

    conn.close()
    ```
    Warning

    Unlike file objects or other resources, exiting the connection’s with block 
    doesn’t close the connection, but only the transaction associated to it. If
    you want to make sure the connection is closed after a certain point, you 
    should still use a try-catch block:
    ```
    conn = psycopg2.connect(DSN)
    try:
        # connection usage
    finally:
        conn.close()
    ```

    """
    defaults = {
        'database': config.POSTGRES_DB,
        'user': config.POSTGRES_USER,
        'password': config.POSTGRES_PASSWORD,
        'host': config.POSTGRES_HOST,
        'port': config.POSTGRES_PORT,
    }
    defaults.update(kws)
    return psycopg2.connect(**defaults)


def initialize_postgres_tables(admin_user=None, admin_password=None):
    """ Initialize the postgres schema and tables. 

    ONLY USE ONCE PER PROJECT.

    """
    # create connection
    kws = {}
    if admin_user:
        kws['username'] = admin_user
    if admin_password:
        kws['password'] = admin_password
    connection = create_connection(**kws)

    # get and check sql parameters
    if not re.match('^[\da-zA-Z_]*$', config.DATASTREAM_SCHEMA):
        raise ValueError(
            f'config.DATASTREAM_SCHEMA="{config.DATASTREAM_SCHEMA}" contains invalid characters'
        )
    params = {
        'schema': config.DATASTREAM_SCHEMA,
    }

    # get all sql files and execute with parameters
    sql_filelist = glob(os.path.join(config.POSTGRES_INITDB_PATH, '*.sql'))
    for sql_filepath in sql_filelist:
        with open(sql_filepath) as f:
            sql = f.read()
            sql_statements = sql.format(**params).split(';')
            print(f'-------------------------')
            print(f'Executing: {sql_filepath}')
            print(f'-------------------------')
            for sql in sql_statements:
                sql = sql.rstrip()
                if not len(sql):
                    continue
                print(sql)
                with connection:
                    with connection.cursor() as cursor:
                        cursor.execute(sql)
                print(f'-------------------------')
            print('')
