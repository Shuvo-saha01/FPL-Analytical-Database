# Fantasy Premier League Analytical Database
This project is a Data Engineering project aimed to build an analytical database for Fantasy Premier league.

### Features of this project
- Build an analytical Database for fantasy premeir league
- Dimensional Data Model for easier Query
- Orchestration for automation and regulation of ETL pipelines
- Idempotent pipelines for eliminating data redundancy

### Source of data 
The main source of this data is the Fantasy Premier League official documentation:



- Source for cards, events, phases, players, teams
```
https://fantasy.premierleague.com/api/bootstrap-static/
```

- Source for Fixtures
```
https://fantasy.premierleague.com/api/fixtures/
```
- Source for Fact player data
```
https://fantasy.premierleague.com/api/element-summary/{player_id}/
```

## How to setup the project 
**Install the following Technology to start**
- Docker (for setting up the postgres container)
- Airflow ( if you want to orchestrate ) 

> **Steps to setup**
- turn on your postgres server instance
- locate your airflow installation and copy the dags file content into the airflow dags folder
- turn on your airflow
- run the fpl_db_creation_dag
- run the fpl_data_update_dag

## Some major decision reasoning

### Why dimensional data modelling 
***Main goal of this project was to create an analyst friendly database***
- Dimensional data modelling because of decentrallized nature helps in easier joins
- Requires fewere joins during query 
- Joins are more optimized compared to a traditional centrallized databse

### Why Idempotent pipelines 
- Idempotent pipelines makes the ETL pipeline data redundant proof
- It makes the pipeline somewhat fault tolerant 

### How is the idempotency pipelines implemented
- Creates a staging table with all the fresh data that comes in from API
- Merges the staging table with actual table with a merge query that compares the indicators and updates element with changes 
- It also inserts new records which are not present in the main table 

