/*
Output the birthMonth/state combination that produced the worst players.
The worst players are defined by the lowest of:  (number of hits (H) / number of at bats (AB))
  To ensure 1 player does not skew the data, make sure that at least 5 people came from the same state and were born in
the same month. For this problem, the year does not matter. A player born in December, 1970 in Michigan and a player born
in December, 1982 in Michigan are in the same group because they were both born in December and are from Michigan.
 */

batters =  LOAD 'hdfs:/home/ubuntu/pigtest/Batting.csv' using PigStorage(',') AS (playerID:chararray, yearID:int, teamID:chararray,
lgID:chararray,G:int,AB:float,R:int,H:float,B2:int,B3:int,HR:int,RBI:int,SB:int,CS:int,BB:int,SO:int,IBB:int,HBP:int,SH:int,SF:int,GIDP:int);
master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',') AS (playerID:chararray,birthYear:int,birthMonth:int,
birthDay:int,birthCountry:chararray,birthState:chararray,birthCity:chararray,deathYear:int,deathMonth:chararray,deathDay:int,
deathCountry:chararray,deathState:chararray,deathCity:chararray,nameFirst:chararray,nameLast:chararray,nameGiven:chararray,
weight:int,height:int,bats:int,throws:int,debut:chararray,finalGame:chararray,retroID:chararray,bbrefID:chararray);
fielding = LOAD 'hdfs:/home/ubuntu/pigtest/Fielding.csv' using PigStorage(',') AS (playerID:chararray,yearID:int,teamID:chararray,
lgID:chararray,POS:chararray, G:float,GS:int,InnOuts:int,PO:int,A:int,E:float,DP:int,PB:int,WP:int,SB:int,CS:int,ZR:int);

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

-- group by month/state
by_month_state = GROUP all_data_nicer BY (birthMonth, birthState);

playersA = FOREACH by_month_state GENERATE FLATTEN(group), SUM(all_data_nicer.hits) as hits, SUM(all_data_nicer.atbats) as atbats;
playersB = DISTINCT playersA;
playersC = FOREACH playersB GENERATE $0 as month, $1 as state,
hits, atbats, (hits/atbats) as score;

worst_month_states = ORDER playersC by score DESC;
worst_clean = FOREACH worst_month_states GENERATE month, state;
worst = LIMIT worst_clean 1;
dump worst;