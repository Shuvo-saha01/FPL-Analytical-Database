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
