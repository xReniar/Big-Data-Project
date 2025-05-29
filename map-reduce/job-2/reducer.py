#!/usr/bin/env python3

import sys

group = {}

for line in sys.stdin:
    line = line.strip()
    key, value = line.split("\t")
    counter, daysonmarket, description = value.split("::", 2)

    counter = int(counter)
    daysonmarket = int(daysonmarket)

    description = description.strip("[]")
    words = description.split(",") if description else []

    if key not in group:
        group[key] = dict(
            num_car = 0,
            daysonmarket = 0,
            word_count = {}
        )

    group[key]["num_car"] += counter
    group[key]["daysonmarket"] += daysonmarket
    
    for word in words:
        if word not in group[key]:
            group[key]["word_count"][word] = 0
        else:
            group[key]["word_count"][word] += 1
    

for key in group.keys():
    obj = group[key]

    city, year, price_tag = key.split("\t")

    num_car = obj["num_car"]
    daysonmarket = obj["daysonmarket"]

    word_dict:dict = group[key]["word_count"]
    word_dict = dict(sorted(word_dict.items(), key=lambda item: item[1], reverse=True))
    top_3_words = list(map(lambda x: x[0], word_dict))[:3]

    avg_days = round(daysonmarket / num_car, 2)

    print(f"{city}\t{year}\t{price_tag}\t{num_car}\t{avg_days}\t{top_3_words}")