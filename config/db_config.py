import psycopg2
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime
from sqlalchemy.engine import Engine

from config import DB_HARVESTING_DATE_COLUMN_NAME, DB_DATASTET_VERSION_COLUMN_NAME, DB_HARVESTED_STATUS_TABLE_NAME
from config.harvester_config import config_harvester

# postgre config
DB_USER = config_harvester["db"]["db_user"]
DB_PASSWORD = config_harvester["db"]["db_password"]
DB_HOST = config_harvester["db"]["db_host"]
DB_PORT = config_harvester["db"]["db_port"]
DB_NAME = config_harvester["db"]["db_name"]

try:
    print(f"Successfully connected to the database")
    connection = psycopg2.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME)

    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
except Exception as e:
    print(f'Did not manage to connect to the database, will be using the testing.postgresql database. Error = {e}')

    from testing.postgresql import Postgresql

    db: Postgresql = Postgresql()
    connection_string: str = db.url()

    print(f'Database connection url = {connection_string}')

engine: Engine = create_engine(connection_string)
meta = MetaData()

harvested_status_table = Table(
    DB_HARVESTED_STATUS_TABLE_NAME,
    meta,
    Column("doi", String),
    Column("uuid", String, primary_key=True),
    Column("is_harvested", String),
    Column("softcite_version", String),
    Column("grobid_version", String),
    Column("harvester_used", String),
    Column("domain", String),
    Column("url_used", String),
    Column(DB_HARVESTING_DATE_COLUMN_NAME, DateTime),
    Column(DB_DATASTET_VERSION_COLUMN_NAME, String),
    extend_existing=True,
)

meta.create_all(engine)
