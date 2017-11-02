/* Output the top three birthdates that had the most players born. I am only looking for day and month combinations. For instance, how many were born on February 3rd, how many were born on March 8th, how many were born on July 20th… print out the top three dates. */

master =  LOAD 'hdfs:/home/ubuntu/pigtest/Master.csv' using PigStorage(',');
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