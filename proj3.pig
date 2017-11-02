-- #1.	Output the birth city of the player who had the most at bats (AB) in his career.
batters = LOAD 'hdfs:/home/ubuntu/pigtest/Batting.csv' using PigStorage(',');
master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',');
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