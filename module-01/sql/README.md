# SQL Refresher – Practice Database

The purpose of this folder is to provide the data that **Alexey** was working with during the **SQL Refresher lecture**.

---

## Prerequisites

Before continuing, make sure you already have:
1. The **ingestion scripts** from the previous lecture.
2. The **`Dockerfile`** (and any related build files) used to create the `taxi_ingest:v001` image.

---

## Getting the Data

You can obtain this folder in one of the following ways:

- Clone the folder using Git by following this tutorial:  
  [Git Clone Tutorial](https://github.com/orgs/community/discussions/102639)

- Or download it directly using this link:  
  [Download the Folder](https://downgit.github.io/#/home?url=https://github.com/Abdelrahman-Abdeljawad/data-engineering-zoomcamp-codes-hws/tree/main/module-01/sql)

---

## Running the Containers

After downloading or cloning the folder, run the following commands:

```bash
docker compose build
docker compose up
```

---

## Accessing pgAdmin

Once the containers are running, open your browser and go to:

```
http://127.0.0.1:8085
```

Alternatively, you can get the address from here:

![pg-port](images/pg-port.png)

---

## pgAdmin Configuration

Inside pgAdmin:

1. Create a **new server**
2. Configure it **exactly as shown in Lecture One**

After completing these steps, your **database will be ready and available in pgAdmin**.

---

## Load the Taxi Data

Don’t forget to run the ingestion script from the previous lecture:

(make sure you are in the same directory as the previous Dockerfile)

```bash
docker run -it --rm \
  --network=sql_pg-net \
  taxi_ingest:v001 \
  --pg-host=pgdatabase \
  --pg-user=root \
  --pg-pass=root \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips_2021_1 \
  --year=2021 \
  --month=1 \
  --chunksize=100000
```

Double-check that the database name, table name, and Docker network match your compose file.

---

## Result

![result](images/pgadmin.png)

You now have the same database setup used during the **SQL Refresher lecture**, ready for practice and experimentation.
