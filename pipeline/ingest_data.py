#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine

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
    for df_chunk in tqdm(df_iter):
        if not created:
            df_chunk.head(0).to_sql(name=target_table,
                                    con=engine,
                                    if_exists="replace"
                                    )
            created = True
            print(f"Table {target_table} Created")

        counter+=1
        print(f"Inserting chunk {counter}")
        df_chunk.to_sql(name=target_table,
                        con=engine,
                        if_exists="append"
                        )
        print(f"{len(df_chunk)} rows inserted.")

def main():
    pg_user = "root"
    pg_pass = "root"
    pg_db = "ny_taxi"
    pg_host = "localhost"
    pg_port = 5432
    year = 2019
    month = 10
    chunksize = 100_000
    target_table = "yellow_taxi_data"

    engine= create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

    ingest_data(url,
                engine=engine,
                target_table=target_table,
                chunksize=chunksize)


if __name__ == "__main__":
    main()