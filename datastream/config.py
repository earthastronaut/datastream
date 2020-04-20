import os

ROOT_PATH = os.path.dirname(__file__)

# Postgres database backend

POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', 5432)

# Database schema

# This should not be changed often or imported from the environment
DATASTREAM_SCHEMA = 'datastream'

POSTGRES_INITDB_PATH = os.path.join(ROOT_PATH, 'postgres_initdb')
