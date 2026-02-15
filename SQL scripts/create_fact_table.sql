-- this script creates the facts table along with relations to other table, 
-- NOTE: RUN IT AT THE VERY END

create table fact_player_data (
	id int generated always as identity primary key not null,
	player_id int,
	constraint fk_player
		foreign key (player_id)
		references players(player_id),
	
	fixture_id int,
	constraint fk_fixture
		foreign key(fixture_id)
		references fixture(fixture_id),
		
	event_id int,
	constraint fk_event
		foreign key(event_id)
		references events(event_id),
		
	phase_id int,
	constraint fk_phase
		foreign key(phase_id)
		references phases(phase_id),
		
	opponent_team_id int,
	constraint fk_opponent_team_id
		foreign key (opponent_team_id)
		references teams(team_id),

	team_id int,
	constraint fk_team_id
		foreign key (team_id)
		references teams(team_id)

		
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
