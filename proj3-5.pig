/* Output the name of the player who had the most errors in all seasons combined. */
master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',') AS (playerID:chararray,birthYear:int,birthMonth:int,
birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:chararray,deathDay:int,
deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,
weight:int,height:int,bats:int,throws:int,debut:chararray,finalGame:chararray,retroID:chararray,bbrefID:chararray);
fielding = LOAD 'hdfs:/home/ubuntu/pigtest/Fielding.csv' using PigStorage(',') AS (playerID:chararray,yearID:int,teamID:chararray,lgID:chararray,POS:chararray,
G:int,GS:int,InnOuts:int,PO:int,A:int,E:int,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

-- Clean up and filter data
realbatters = FILTER fielding BY E>0;
errors_data = FOREACH realbatters GENERATE playerID, E;
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

