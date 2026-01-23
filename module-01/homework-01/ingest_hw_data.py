#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
import click

gtt_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

gtt_df = pd.read_parquet(gtt_url)

zones_df= pd.read_csv(zones_url)

@click.command()
@click.option('--pg-user', default="root", help="postgres user")
@click.option('--pg-pass', default="root")
@click.option('--pg-host', default="localhost")
@click.option('--pg-port', default="5432")
@click.option('--pg-db', default="ny_taxi_hw")
def ingest(PG_USER, PG_PASS, PG_HOST, PG_PORT, PG_DB):
    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}')

    print("creating tables..")
    gtt_df.head(0).to_sql(
        name="green_taxi_trips",
        con=engine,
        if_exists="replace"
    )
    print("green taxi data table created")

    zones_df.head(0).to_sql(
        name="zones",
        con=engine,
        if_exists="replace"
    )
    print("zones tables created")

    print("ingesting data..")
    gtt_df.to_sql(
        name="green_taxi_trips",
        con=engine,
        if_exists="append"
    )
    print("done ingesting data for green taxi trips table")
    zones.to_sql(
        name="zones",
        con=engine,
        if_exists="append"
    )
    print("done ingesting data for zones table")

if __name__ == '__main__':
    ingest()