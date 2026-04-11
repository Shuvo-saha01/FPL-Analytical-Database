import requests, pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

# loading env
load_dotenv()

# loading postgres env and defining engine
POSTGRES_URI = os.getenv("POSTGRES_URI") or ""
engine = create_engine(POSTGRES_URI)

# calling api data
apidata = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
json_data = apidata.json()

# defining ETL pipelines
def etl_cards():
    json_chips = json_data['chips']
    df_chips = pd.DataFrame(json_chips)
    df_result = df_chips.drop(columns=["id", "overrides"])

    df_result.rename(
        columns = {
            "name" : "card_name",
            "number" : "card_useable_number",
            "start_event" : "card_start_event",
            "stop_event" : "card_stop_event",
            "chip_type" : "card_type"
        }, 
        inplace= True
    )

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE cards RESTART IDENTITY;"))

    df_result.to_sql(
        name="cards",
        con=engine,
        if_exists="append",
        index=False
    )

def etl_events():

    df = pd.DataFrame(json_data["events"])
    df = df[["name", "deadline_time", "average_entry_score", "highest_scoring_entry","deadline_time_epoch","highest_score","ranked_count", "finished"]]

    # create staging table
    with engine.begin() as con:
        con.execute(
            text(
                '''
                create table if not exists stg_events(
                    event_id int generated always as identity primary key not null,
                    name varchar(100) not null,
                    deadline_time date ,
                    average_entry_score int,
                    highest_scoring_entry int,
                    deadline_time_epoch int,
                    highest_score int,
                    ranked_count int,
                    finished bool
                )
                '''
            )
        )
        
        con.execute(
            text(
                "truncate table stg_events restart identity"
            )
        )

    df.to_sql("stg_events", con=engine, if_exists="append", index=False)

    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE INTO events AS target
                USING stg_events AS source
                ON target.event_id = source.event_id
                
                WHEN MATCHED THEN
                    UPDATE SET 
                        name = source.name,
                        deadline_time = source.deadline_time,
                        average_entry_score = source.average_entry_score,
                        highest_scoring_entry = source.highest_scoring_entry,
                        deadline_time_epoch = source.deadline_time_epoch,
                        highest_score = source.highest_score,
                        ranked_count = source.ranked_count,
                        finished = source.finished
                
                WHEN NOT MATCHED THEN
                    INSERT (name, deadline_time, average_entry_score, highest_scoring_entry, deadline_time_epoch, highest_score, ranked_count, finished)
                    VALUES (source.name, source.deadline_time, source.average_entry_score, source.highest_scoring_entry, source.deadline_time_epoch, source.highest_score, source.ranked_count, source.finished);
                '''
            )
        )

def etl_teams():
    df = pd.DataFrame(json_data["teams"])

    df.drop(
        columns=["code", "id", "team_division", "unavailable", "pulse_id"],
        inplace=True
    )

    df.drop(
        columns=["form"],
        inplace=True
    )

    df.rename(
        columns={
            "name" : "team_name",
            "short_name" : "team_short_name",
            "strength" : "team_strength"
        },
        inplace=True
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                '''
                create table if not exists stg_teams(
                    team_id int generated always as identity primary key not null,
                    team_name varchar(100) not null,
                    draw int ,
                    loss int,
                    played int,
                    points int,
                    position int,
                    win int,
                    team_short_name varchar(50) not null,
                    team_strength int, 
                    strength_overall_home int ,
                    strength_overall_away int, 
                    strength_attack_home int ,
                    strength_attack_away int,
                    strength_defence_home int,
                    strength_defence_away int
                );
                '''
            )
        )
        conn.execute(
            text(
                "truncate stg_teams restart identity"
            )
        )

    df.to_sql(
        con=engine,
        name="stg_teams",
        if_exists="append",
        index=False
    )

    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE INTO teams as target 
                USING stg_teams as source
                ON target.team_id = source.team_id
                
                WHEN MATCHED THEN 
                    UPDATE SET 
                        team_name = source.team_name,
                        draw = source.draw,
                        loss = source.loss,
                        played = source.played ,
                        points = source.points,
                        position = source.position,
                        win = source.win ,
                        team_short_name = source.team_short_name, 
                        team_strength = source.team_strength,
                        strength_overall_home =  source.strength_overall_home,
                        strength_overall_away = source.strength_overall_away, 
                        strength_attack_home =  source.strength_attack_home,
                        strength_attack_away = source.strength_attack_away,
                        strength_defence_home = source.strength_defence_home,
                        strength_defence_away = source.strength_defence_away
                        
                WHEN NOT MATCHED THEN 
                    INSERT (team_name, draw, loss, played, points, position, win, team_short_name, team_strength, strength_overall_home, strength_overall_away, strength_attack_home, strength_attack_away, strength_defence_home, strength_defence_away)
                    VALUES (source.team_name, source.draw, source.loss, source.played, source.points, source.position, source.win, source.team_short_name, source.team_strength, source.strength_overall_home, source.strength_overall_away, source.strength_attack_home, source.strength_attack_away, source.strength_defence_home, source.strength_defence_away)
                '''
            )
        )


def etl_players():
    df = pd.DataFrame(json_data["elements"])

    df = df[["first_name", "second_name", "web_name","total_points", "transfers_in", "transfers_in_event", "transfers_out", "transfers_out_event", "region"]]

    with engine.begin() as con:
        con.execute(
            text(
                '''
                create table if not exists stg_players(
                    player_id int generated always as identity primary key not null,
                    first_name varchar(100) not null,
                    second_name varchar(100) not null,
                    web_name varchar(100) not null,
                    total_points int,
                    transfers_in int,
                    transfers_in_event int,
                    transfers_out int,
                    transfers_out_event int,
                    region int
                )
                '''
            )
        )
        
        con.execute(
            text(
                "truncate table stg_players restart identity"
            )
        )

    df.to_sql(
        name="stg_players",
        con=engine,
        if_exists="append",
        index=False
    )

    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE INTO players AS target
                USING stg_players AS source
                ON target.player_id = source.player_id
                
                WHEN MATCHED THEN
                    UPDATE SET 
                        first_name = source.first_name,
                        second_name = source.second_name,
                        web_name = source.web_name,
                        total_points = source.total_points,
                        transfers_in = source.transfers_in,
                        transfers_in_event = source.transfers_in_event,
                        transfers_out = source.transfers_out,
                        transfers_out_event = source.transfers_out_event,
                        region = source.region
                
                WHEN NOT MATCHED THEN
                    INSERT (first_name, second_name, web_name, total_points, transfers_in, transfers_in_event, transfers_out, transfers_out_event, region)
                    VALUES (source.first_name, source.second_name, source.web_name, source.total_points, source.transfers_in, source.transfers_in_event, source.transfers_out, source.transfers_out_event, source.region)
                '''
            )
        )

def etl_phases():
    df_phases = pd.DataFrame(json_data["phases"])

    df_phases = df_phases[df_phases["name"] != 'Overall']

    df_phases = df_phases.drop(columns=["id"])

    df_phases.rename(
        columns={
            
            "name" : "phase_name",
            "start_event" : "phase_start_event",
            "stop_event" : "phase_stop_event",
            "highest_score" : "phase_highest_score"
        },
        inplace=True
    )
    
    with engine.begin() as con:
        con.execute(
            text(
                '''
                create table if not exists stg_phases(
                    phase_id int generated always as identity primary key not null,
                    phase_name varchar(100) not null,
                    phase_start_event int not null,
                    phase_stop_event int not null, 
                    phase_highest_score int
                )
                '''
            )
        )

    with engine.begin() as con:
        con.execute(
            text(
                "truncate stg_phases RESTART IDENTITY;"
            )
        )
        
    df_phases.to_sql(
            name="stg_phases",
            con=engine,
            if_exists='append',
            index=False
        )
    
    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE into phases as target 
                USING stg_phases as source
                ON target.phase_id = source.phase_id
                
                WHEN MATCHED THEN 
                    UPDATE SET 
                        phase_name = source.phase_name,
                        phase_highest_score = source.phase_highest_score
                
                WHEN NOT MATCHED THEN 
                    INSERT (phase_name, phase_start_event, phase_stop_event, phase_highest_score)
                    VALUES (source.phase_name, source.phase_start_event, source.phase_stop_event, source.phase_highest_score)
                '''
            )
        )


