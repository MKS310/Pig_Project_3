/* Sum up the number of doubles and triples for each city/state combination.
Output the top 5 city/state combinations that produced the players who had the most doubles and triples.
 */
batters =  LOAD 'hdfs:/home/ubuntu/pigtest/Batting.csv' using PigStorage(',') AS (playerID:chararray, yearID:int, teamID:chararray,
lgID:chararray,G:int,AB:float,R:int,H:float,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);
master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',') AS (playerID:chararray,birthYear:int,birthMonth:int,
birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:chararray,deathDay:int,
deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,
weight:int,height:int,bats:int,throws:int,debut:chararray,finalGame:chararray,retroID:chararray,bbrefID:chararray);
fielding = LOAD 'hdfs:/home/ubuntu/pigtest/Fielding.csv' using PigStorage(',') AS (playerID:chararray,yearID:int,teamID:chararray,
lgID:chararray,POS:chararray, G:float,GS:int,InnOuts:int,PO:int,A:int,E:float,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);
-- Extract only columns of interest: playerID, two base hit, three base hit, birth city and birth state
realbatter = FILTER batters by $1>0;
batter_data = FOREACH realbatter GENERATE playerID, B2, B3;
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