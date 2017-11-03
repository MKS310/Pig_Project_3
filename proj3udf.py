#!/usr/bin/python
# -*- coding: UTF-8 -*-
# from pig_utils import outputSchema 
#  getTopStat is used to determine the top baseball player using equation:
#  (number of hits (H) / number of at bats (AB)) – (number of errors (E) / number of games (G))
@outputSchema('value:double')
def getTopStat ( hits, atbats, games, errors ):
    hits = float(hits)
    atbats = float(atbats)
    games=float(games)
    errors=float(errors)
    result = 0
    if atbats == 0 or games == 0:
        result = 0
    else:
        result = float(( hits/atbats ) - ( errors/games ))
    return round(result, 3)
