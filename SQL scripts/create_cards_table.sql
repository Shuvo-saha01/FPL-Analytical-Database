-- Running this script creates a cards table, containing the details of power cards of FPL

create table cards (
	card_id int generated always as identity primary key not null,
	card_name varchar(100) not null,
	card_useable_number integer not null,
	card_start_event integer not null,
	card_stop_event integer not null,
	card_type varchar(100) not null
)
