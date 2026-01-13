#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine, text, inspect, select, func
from sqlalchemy.exc import OperationalError, ProgrammingError
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

def ingest_data(url: str,
                engine,
                target_table: str,
                chunksize: int = 100_000
                ) -> pd.DataFrame:
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    created = False
    counter=0
    rows_inserted=0
    for df_chunk in tqdm(df_iter):
        if not created:
            df_chunk.head(0).to_sql(name=target_table,
                                    con=engine,
                                    if_exists="replace"
                                    )
            created = True
            print(f"\nTable {target_table} Created")

        counter+=1
        print(f"Inserting chunk {counter}")
        df_chunk.to_sql(name=target_table,
                        con=engine,
                        if_exists="append"
                        )
        rows_inserted+=len(df_chunk)
        print(f"{rows_inserted} rows inserted.")

# you can configure environment variable to pass values from docker "environment"
# @click.option('--pg-host', envvar="pg_host", default='localhost', help='PostgreSQL Host')
@click.command()
@click.option('--pg-host', default='localhost', help='PostgreSQL Host')
@click.option('--pg-user', default='root', help='PostgreSQL Username')
@click.option('--pg-pass', default='root', help='PostgreSQL Password')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL Database')
@click.option('--pg-port', default=5432, help='PostgreSQL Port')
@click.option('--year', default=2019, help='Year of the data')
@click.option('--month', default=10, help='Month of the data')
@click.option('--chunksize', default=100_000, help='Size of the ingested chunk')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def main(pg_host, pg_user, pg_pass, pg_db, pg_port, year, month, chunksize, target_table):
    engine= create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    ingest_data(url,
                engine=engine,
                target_table=target_table,
                chunksize=chunksize)

def db_exists_and_have_data(dbname: str, tablename: str) -> bool:
    url = f"postgresql+psycopg2://root:root@pgdatabase:5432/{dbname}"
    try:
        engine = create_engine(url, pool_pre_ping=True)
        insp = inspect(engine)

        # check table
        if tablename not in insp.get_table_names(schema="public"):
            return False

        # check for record existance
        with engine.connect() as conn:
            row = conn.execute(
                select(func.count()).select_from(text(f'"{tablename}"'))
            ).one()
            return row[0] > 0
    except (OperationalError, ProgrammingError) as e:
        # DB unreachable or auth error
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    if db_exists_and_have_data("ny_taxi", "yellow_taxi_data"):
        print("Table exists and has data.")
    else:
        print("Table missing or empty.")
        main()