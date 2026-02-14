-- this script creates the fixture table
create table fixture(
	fixture_id int generated always as identity not null primary key,
	event int,
	finished bool,
	kickoff_time date,
	team_a_id int,
	team_a_score int,
	team_h_id int,
	team_h_score int
);