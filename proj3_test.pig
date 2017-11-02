bat_data = FOREACH batters {
	realbatters = FILTER batters BY $1>0;
	atbats = GENERATE (realbatters.$0) as playerID, (realbatters.$5) as atbats;
	grouped_atbats = GROUP atbats by playerID;
	sum_hits = FOREACH grouped_atbats GENERATE group, SUM(bat_data.atbats);
}

nicer_named_atbats = FOREACH master {
	id_names = GENERATE $0 as playerID, $14 as lastName, $15 as firstName;
	named_atbats = JOIN sum_hits BY $0, id_names BY playerID;
	nicer_named_atbats = FOREACH named_atbats GENERATE $3 as LastName, $4 as FirstName, $1 as atbats;
}

top_atbats = LIMIT (ORDER nicer_named_atbats BY atbats DESC) 10;
dump top_atbats;