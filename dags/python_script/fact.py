import requests
import pandas as pd 
from sqlalchemy import create_engine , text
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRES_URI = os.getenv('POSTGRES_URI') or " "
engine = create_engine(POSTGRES_URI)

def etl_fact():
    players_df = pd.read_sql('SELECT * FROM players', con=engine)

    player_id_array = players_df['player_id'].tolist()

    df = pd.DataFrame()

    for i in player_id_array:
        url = f"https://fantasy.premierleague.com/api/element-summary/{i}/"
        response = requests.get(url)
        data = response.json()
        df = pd.concat([df, pd.DataFrame(data['history'])], ignore_index=True)

    df = df[["element", "fixture", "opponent_team", "total_points", "was_home", "kickoff_time", "team_a_score", "team_h_score", "round", "goals_scored", "assists", "clean_sheets", "goals_conceded", "own_goals", "penalties_saved", "penalties_missed", "yellow_cards", "red_cards", "saves", "bonus", "bps"]]

    df.rename(
        columns={
            "element": "player_id",
            "fixture" : "fixture_id",
            "opponent_team" : "opponent_team_id"
        },
        inplace=True
    )

    df_fixtures = pd.read_sql('SELECT * FROM fixture', con=engine)

    fixture_array = df["fixture_id"].tolist()
    event_array = []
    for i in fixture_array:
        event = df_fixtures[df_fixtures['fixture_id'] == i]["event"].values[0]
        event_array.append(event)

    df["event_id"] = event_array

    phase_events = {
    1: list(range(1, 3 + 1)),
    2: list(range(4, 6 + 1)),  
    3: list(range(7, 9 + 1)),   
    4: list(range(10, 13 + 1)), 
    5: list(range(14, 19 + 1)),  
    6: list(range(20, 24 + 1)),  
    7: list(range(25, 28 + 1)),  
    8: list(range(29, 31 + 1)), 
    9: list(range(32, 34 + 1)),  
    10: list(range(35, 38 + 1))  
}

    def assign_phase(event_id):
        for phase, events in phase_events.items():
            if event_id in events:
                return phase    
        return None

    phase_array = []

    for i in event_array:
        phase = assign_phase(i)
        phase_array.append(phase)
        
    df["phase_id"] = phase_array

    with engine.begin() as conn:
        conn.execute(
            text(
                '''
                create table stg_fact_player_data (
                    id int generated always as identity primary key not null,
                    player_id int,
                    fixture_id int,
                    event_id int,
                    phase_id int,
                    opponent_team_id int,
                    total_points int,
                    was_home bool,
                    kickoff_time date,
                    team_h_score int,
                    team_a_score int,
                    round int,
                    goals_scored int,
                    assists int,
                    clean_sheets int,
                    goals_conceded int,
                    own_goals int,
                    penalties_saved int,
                    penalties_missed int,
                    yellow_cards int,
                    red_cards int,
                    saves int,
                    bonus int,
                    bps int
                )
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
        name="stg_fact_player_data",
        if_exists="append",
        index=False
    )

    with engine.begin() as con:
        con.execute(
            text(
                '''
                MERGE INTO fact_player_data AS target 
                USING stg_fact_player_data AS source 
                ON target.player_id = source.player_id
                AND target.fixture_id = source.fixture_id

                WHEN MATCHED THEN
                    UPDATE SET
                        event_id = source.event_id,
                        phase_id = source.phase_id,
                        opponent_team_id = source.opponent_team_id,
                        total_points = source.total_points,
                        was_home = source.was_home,
                        kickoff_time = source.kickoff_time,
                        team_h_score = source.team_h_score,
                        team_a_score = source.team_a_score,
                        round = source.round,
                        goals_scored = source.goals_scored,
                        assists = source.assists,
                        clean_sheets = source.clean_sheets,
                        goals_conceded = source.goals_conceded,
                        own_goals = source.own_goals,
                        penalties_saved = source.penalties_saved,
                        penalties_missed = source.penalties_missed,
                        yellow_cards = source.yellow_cards,
                        red_cards = source.red_cards,
                        saves = source.saves,
                        bonus = source.bonus,
                        bps = source.bps

                WHEN NOT MATCHED THEN
                    INSERT (player_id, fixture_id, event_id, phase_id, opponent_team_id, total_points, was_home, kickoff_time, team_h_score, team_a_score, round, goals_scored, assists, clean_sheets, goals_conceded, own_goals, penalties_saved, penalties_missed, yellow_cards, red_cards, saves, bonus, bps)
                    VALUES (source.player_id, source.fixture_id, source.event_id, source.phase_id, source.opponent_team_id, source.total_points, source.was_home, source.kickoff_time, source.team_h_score, source.team_a_score, source.round, source.goals_scored, source.assists, source.clean_sheets, source.goals_conceded, source.own_goals, source.penalties_saved, source.penalties_missed, source.yellow_cards, source.red_cards, source.saves, source.bonus, source.bps)
                '''
            )
        )
