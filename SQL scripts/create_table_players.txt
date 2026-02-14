-- This script allows us to create table for players data

create table players(
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
