-- this script creates a phase table, that allows us to store the phase data for FPL 

create table phases(
	phase_id int generated always as identity primary key not null,
	phase_name varchar(100) not null,
	phase_start_event int not null,
	phase_stop_event int not null, 
	phase_highest_score int
)