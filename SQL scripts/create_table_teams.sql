-- This script creates a team table for storing team data for the league

create table teams(
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
)
