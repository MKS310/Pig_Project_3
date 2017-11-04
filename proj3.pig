/*Assignment: Project3, pig scripts
Class: DS730, Fall 2017
Author: Maggie Schweihs*/

/*
#1.	Output the birth city of the player who had the most at bats (AB) in his career. */

-- LOAD all data
batters =  LOAD 'hdfs:/home/ubuntu/pig/Batting.csv' using PigStorage(',') AS (playerID:chararray, yearID:int, teamID:chararray,
lgID:chararray,G:int,AB:float,R:int,H:float,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);
master =  LOAD 'hdfs:/home/ubuntu/pig/Master.csv' using PigStorage(',') AS (playerID:chararray,birthYear:int,birthMonth:int,
birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:chararray,deathDay:int,
deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,
weight:int,height:int,bats:int,throws:int,debut:chararray,finalGame:chararray,retroID:chararray,bbrefID:chararray);
fielding = LOAD 'hdfs:/home/ubuntu/pig/Fielding.csv' using PigStorage(',') AS (playerID:chararray,yearID:int,teamID:chararray,
lgID:chararray,POS:chararray, G:float,GS:int,InnOuts:int,PO:int,A:int,E:float,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

realbatters = FILTER batters BY $1>0;
-- Get columns of interest
bat_data = FOREACH realbatters GENERATE $0 as playerID, $5 as atbats;
-- Aggregate for each player
grouped_atbats = GROUP bat_data by playerID;
sum_hits = FOREACH grouped_atbats GENERATE group, SUM(bat_data.atbats);
-- Get columns for Player Names
id_names = FOREACH master GENERATE $0 as playerID, $14 as lastName, $15 as firstName, $6 as BirthCity;
-- Join At Bats to Player Names
named_atbats = JOIN sum_hits BY $0, id_names BY playerID;
nicer_named_atbats = FOREACH named_atbats GENERATE $3 as LastName, $4 as FirstName, $5 as BirthCity, $1 as atbats;
-- dump BirthCity of Player with most At Bats
sorted1 = ORDER nicer_named_atbats BY atbats DESC;
sorted = FOREACH sorted1 GENERATE $2 as BirthCity;
-- ORDER nicer_named_atbats BY atbats DESC;
top_atbats = LIMIT sorted 1;
dump top_atbats;

/* #2.
Output the top three birthdates that had the most players born. I am only looking for day and month combinations.
For instance, how many were born on February 3rd, how many were born on March 8th, how many were born on July 20th…
print out the top three dates. */

-- Clean up the data, some birthdays are missing
realplayer = FILTER master BY $2 >0 and $3>0;
master_data = FOREACH realplayer GENERATE $2 as birthMonth, $3 as birthDay;
-- First, group all birthdays by month
by_month = GROUP master_data BY birthMonth;
-- Flatten and regroup by birth month/day combo
X = FOREACH by_month GENERATE group, FLATTEN($1) as (mon,day);
Y = GROUP X BY (mon,day);
-- Count number of items in each bag of birth day/month combos
Z = FOREACH Y GENERATE group.mon,group.day, COUNT($1) AS ct;
-- sort by count, highest to lowest;
sorted1 = ORDER Z BY $2 DESC;
-- only want month, day in output
sorted = FOREACH sorted1 GENERATE $0 as month, $1 as day;
-- dump the three most popular birthdays
top_birthday = LIMIT sorted 3;
dump top_birthday;

/* #3.
Output the second most common weight. */

-- Clean up the data, some weights are missing
realplayer = FILTER master BY $16 >0;
master_data = FOREACH realplayer GENERATE $16 as weight;
-- First, group all data by weight
by_weight = GROUP master_data BY weight;
-- Count number of occurrences for each weight
Z = FOREACH by_weight GENERATE group, COUNT($1) AS ct;
-- sort by count, highest to lowest;
sorted3 = ORDER Z BY $1 DESC;
-- grab the top two
top_two = LIMIT sorted3 2;
-- reverse the list
sorted2 = ORDER top_two BY $1 ASC;
-- only want weight in output
sorted1 = FOREACH sorted2 GENERATE $0 as weight;
-- dump second most common weight (top of reversed top_two)
second_weight = LIMIT sorted1 1;
dump second_weight;

/* #4.
Output the team that had the most errors in 2001. */

-- Clean up and filter data
year_2001 = FILTER fielding BY yearID == 2001 AND E>0;
year_errors = FOREACH year_2001 GENERATE teamID, E;
-- Group by Team
teams_2001 = GROUP year_errors BY teamID;
-- Add up all errors for each team
teams_errors = foreach teams_2001 GENERATE group, SUM(year_errors.E);
-- sort by errors, highest to lowest;
sorted1 = ORDER teams_errors BY $1 DESC;
-- only want teamID in output
sorted = FOREACH sorted1 GENERATE $0 as teamID;
-- dump the team name having most errors
most_errors = LIMIT sorted 1;
dump most_errors;

/* #5.
Output the name of the player who had the most errors in all seasons combined. */

-- Clean up and filter data
batters_with_errors = FILTER fielding BY E>0;
errors_data = FOREACH batters_with_errors GENERATE playerID, E;
player_names = FOREACH master GENERATE $0 AS playerID, $14 as lastName, $15 as firstName;
-- Group by Team
by_player = GROUP errors_data BY playerID;
-- Add up all errors for each team
player_errors = foreach by_player GENERATE group, SUM(errors_data.E);
-- sort by errors, highest to lowest;
sorted1 = ORDER player_errors BY $1 DESC;
-- only want player name in output
sorted = FOREACH sorted1 GENERATE $0 as playerID;
sorted_players = JOIN sorted BY (playerID), player_names BY (playerID);
sorted_players2 = FOREACH sorted_players GENERATE $2 as lastName, $3 as firstName;
-- dump the player having most all-time errors
most_errors = LIMIT sorted_players2 1;
dump most_errors;

/* #6.
A player who hits well and doesn’t commit a lot of errors is obviously a player you want on your team.
Who were the top 3 players from 2005 through 2009 (including 2005 and 2009) who maximized the following criterion:
 (number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G))
The above equation might be skewed by a player who only had 3 at bats but got two hits.
To account for that, only consider players who had at least 40 at bats and played in at least 20 games over that entire 5 year span.
You should note that both files contain a number of games column. The 20 game minimum is from the Fielding file.
Be aware that some players played for multiple teams during that 5 year span. Also be aware that a player could have played multiple
positions during that span (see for instance, buchabr01 in 2004 who played LF, OF and RF in the same season).
You may need a UDF for this problem.
 */

-- Extract only data from years 2005 to 2009, only columns of interest: playerID, atbats, hits, games, and errors
realbatters_years = FILTER batters by $1>0 and yearID <= 2009 and yearID >= 2005;
batter_data = FOREACH realbatters_years GENERATE playerID, yearID, AB, H;
fielding_data = FOREACH fielding GENERATE playerID, yearID, G, E;
-- Join relations together
all_data = JOIN batter_data BY (playerID, yearID), fielding_data BY (playerID, yearID);
-- Clean up the joined data
all_data_nicer = FOREACH all_data GENERATE (batter_data::playerID) as playerID, batter_data::yearID as yearID,
batter_data::AB as AB, batter_data::H as H,  fielding_data::G as G, fielding_data::E as E;
-- Group and aggregate totals for each player
by_player = GROUP all_data_nicer BY playerID;
by_player_flat = FOREACH by_player GENERATE group as playerID, SUM(all_data_nicer.H) as sumH, SUM(all_data_nicer.AB) as sumAB,
    SUM(all_data_nicer.G) as sumG, SUM(all_data_nicer.E) as sumE;
-- Filter the player data to avoid skewed results. Want at least 40 atbats and at least 20 games
all_data_filtrd = FILTER by_player_flat BY sumAB>=40 and sumG>=20;
-- Use UDF to calculate player stats
REGISTER 'proj3udf.py' USING jython AS myudf;
answer = FOREACH all_data_filtrd GENERATE playerID, myudf.getTopStat(sumH, sumAB, sumG, sumE);
-- Get the top three player from 2005 to 2009 time period
answer_ordered = ORDER answer by value DESC;
top_three = LIMIT answer_ordered 3;
-- Join to player name
player_names = FOREACH master GENERATE $0 AS playerID, $14 as lastName, $15 as firstName;
top_three_players = JOIN top_three BY (playerID), player_names BY (playerID);
answer_final = FOREACH top_three_players GENERATE $3 as lastName, $4 as firstName;
dump answer_final;

/* #7.
Sum up the number of doubles and triples for each city/state combination.
Output the top 5 city/state combinations that produced the players who had the most doubles and triples.
 */

-- Extract only columns of interest: playerID, two base hit, three base hit, birth city and birth state
batter_data = FOREACH realbatters GENERATE playerID, B2, B3;
master_data = FOREACH master GENERATE playerID, birthState, birthCity;
-- Join relations together
all_data = JOIN batter_data BY playerID, master_data BY playerID;
-- Clean up the joined data
all_data_nicer = FOREACH all_data GENERATE master_data::birthState as state, master_data::birthCity as city,
batter_data::B2 as B2, batter_data::B3 as B3;
-- Group and aggregate totals for each State, City
by_state_city = GROUP all_data_nicer BY (state, city);
summed_by_state_city = FOREACH by_state_city GENERATE group as state_city, SUM(all_data_nicer.B2) as sumB2,
SUM(all_data_nicer.B3) as sumB3, SUM(all_data_nicer.B2) + SUM(all_data_nicer.B3) as total;
-- Get the top 5 city/state combos
answer_ordered = ORDER summed_by_state_city by total DESC;
top_five = LIMIT answer_ordered 5;
top_five_final = FOREACH top_five GENERATE state_city;
DUMP top_five_final;

/* #8.
Output the birthMonth/state combination that produced the worst players.
The worst players are defined by the lowest of:  (number of hits (H) / number of at bats (AB))
  To ensure 1 player does not skew the data, make sure that at least 5 people came from the same state and were born in
the same month. For this problem, the year does not matter. A player born in December, 1970 in Michigan and a player born
in December, 1982 in Michigan are in the same group because they were both born in December and are from Michigan.
 */

-- columns of interest: playerID, hits, atbats, birth month, birth state
batter_filtrd = FILTER batters by yearID>0 and AB>0;
master_filtrd = FILTER master by birthMonth is not null and birthState is not null;
batter_data = FOREACH batter_filtrd GENERATE playerID, H, AB;
master_data = FOREACH master_filtrd GENERATE playerID, birthMonth, birthState;
-- Join relations together
all_data = JOIN batter_data BY playerID, master_data BY playerID;
-- Clean up the joined data
all_data_nicer = FOREACH all_data GENERATE batter_data::playerID as playerID, master_data::birthMonth as birthMonth,
master_data::birthState as birthState, batter_data::H as hits, batter_data::AB as atbats;
-- group by Month, State combination
by_month_state_all = GROUP all_data_nicer BY (birthMonth, birthState);
-- Aggregate the sum of hits and atbats for each player in the bag of each Month/State combo
playersA = FOREACH by_month_state_filtrd GENERATE group, SUM(all_data_nicer.hits) as hits, SUM(all_data_nicer.atbats) as atbats,
 all_data_nicer.playerID as playerID;
-- Generate the number of unique players in each group
playersB = FOREACH playersA {
    uniquePlayers = DISTINCT playerID;
    GENERATE
        FLATTEN(group),
        COUNT(uniquePlayers) AS players,
        hits AS hits,
        atbats AS atbats;
}
-- filter for month/state combos that have at least 5 players
playersB_filtrd = FILTER playersB by players >=5;
-- calculate "worst players" score by state/month (not by player)
playersC = FOREACH playersB_filtrd GENERATE $0 as month, $1 as state,
hits, atbats, (hits/atbats) as score;
-- sort and find the worst state/month
worst_month_states = ORDER playersC by score DESC;
worst_clean = FOREACH worst_month_states GENERATE month, state;
worst = LIMIT worst_clean 1;
dump worst;