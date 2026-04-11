import requests
import pandas as pd 
from sqlalchemy import create_engine , text
from dotenv import load_dotenv
import os

load_dotenv()

json_data = requests.get("https://fantasy.premierleague.com/api/fixtures/").json()

postgres_URI = os.getenv("POSTGRES_URI") or ""
engine = create_engine(postgres_URI)

def etl_fixture():

    df = pd.DataFrame(json_data)

    df = df.drop(
        columns=[
            "finished_provisional",
            "id",
            "minutes",
            "provisional_start_time", "started", "stats", "team_h_difficulty", "team_a_difficulty", "pulse_id", "code"
        ]
    )


    df.rename(
        columns={
            "team_a" : "team_a_id",
            "team_h" : "team_h_id"
        } ,
        inplace=True
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                '''
                create table if not exists stg_fixture(
                    fixture_id int generated always as identity not null primary key,
                    event int,
                    finished bool,
                    kickoff_time date,
                    team_a_id int,
                    team_a_score int,
                    team_h_id int,
                    team_h_score int
                );
                '''
            )
        )
        conn.execute(
            text(
                "truncate stg_fixture restart identity"
            )
    )
        
    df.to_sql(
        con=engine,
        name="stg_fixture",
        if_exists="append",
        index=False
    )
        
    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE INTO fixture as target 
                USING stg_fixture as source 
                ON target.fixture_id = source.fixture_id
                
                WHEN MATCHED THEN
                    UPDATE SET
                        finished = source.finished,
                        kickoff_time = source.kickoff_time,
                        team_a_score = source.team_a_score,
                        team_h_score = source.team_h_score
                    
                WHEN NOT MATCHED THEN
                    INSERT (event, finished, kickoff_time, team_a_id, team_a_score, team_h_id, team_h_score)
                    VALUES (source.event, source.finished, source.kickoff_time, source.team_a_id, source.team_a_score, source.team_h_id, source.team_h_score)
                '''
            )
    )