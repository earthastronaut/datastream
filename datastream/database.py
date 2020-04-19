import os

import psycopg2

__all__ = [
    'cursor_type',
    'connection_type',
    'create_connection',
]


cursor_type = psycopg2.extensions.cursor
connection_type = psycopg2.extensions.connection

default_connection_kws = dict(
    database=os.environ['POSTGRES_DB'],
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'],
    host=os.environ['POSTGRES_HOST'],
    port=os.environ.get('POSTGRES_PORT', 5432),
)


def create_connection(**kws):
    """ Create postgres connection. 


    This function exists as a helper so that the settings get in correctly.


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
    defaults = default_connection_kws.copy()
    defaults.update(kws)
    return psycopg2.connect(**defaults)
