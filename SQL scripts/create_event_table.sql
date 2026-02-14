-- this script creates table for events
create table events(
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
