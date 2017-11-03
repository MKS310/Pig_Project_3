/*
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
batters =  LOAD 'hdfs:/home/ubuntu/pigtest/Batting.csv' using PigStorage(',') AS (playerID:chararray, yearID:int, teamID:chararray,
lgID:chararray,G:int,AB:float,R:int,H:float,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);
master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',') AS (playerID:chararray,birthYear:int,birthMonth:int,
birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:chararray,deathDay:int,
deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,
weight:int,height:int,bats:int,throws:int,debut:chararray,finalGame:chararray,retroID:chararray,bbrefID:chararray);
fielding = LOAD 'hdfs:/home/ubuntu/pigtest/Fielding.csv' using PigStorage(',') AS (playerID:chararray,yearID:int,teamID:chararray,
lgID:chararray,POS:chararray, G:float,GS:int,InnOuts:int,PO:int,A:int,E:float,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);
-- Extract only data from years 2005 to 2009, only columns of interest: playerID, atbats, hits, games, and errors
realbatter = FILTER batters by $1>0 and yearID <= 2009 and yearID >= 2005;
batter_data = FOREACH realbatter GENERATE playerID, yearID, AB, H;
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