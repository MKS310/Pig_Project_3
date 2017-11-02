/* Output the second most common weight. */

master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',');
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
-- dump second most common weight
second_weight = LIMIT sorted1 1;
dump second_weight;