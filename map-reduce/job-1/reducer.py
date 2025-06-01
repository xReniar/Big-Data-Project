#!/usr/bin/env python3

import sys


group = {}

for line in sys.stdin:
    make_name, model_name, count, price, year = line.strip().split("\t")

    count = int(count)
    price = float(price)
    year = int(year)

    key = f"{make_name}\t{model_name}"
    if key not in group:
        group[key] = dict(
            num_car = 0,
            prices = [],
            years = []
        )

    group[key]["num_car"] += count
    group[key]["prices"] += [price]
    group[key]["years"] += [year]

for key in group.keys():
    obj = group[key]

    make_name, model_name = key.split("\t")

    num_cars = obj["num_car"]
    min_value = min(obj["prices"])
    max_value = max(obj["prices"])
    avg_value = round(sum(obj["prices"]) / num_cars, 2)
    years = sorted(set(obj["years"]))

    print(f"{make_name}\t{model_name}\t{num_cars}\t{min_value}\t{max_value}\t{avg_value}\t{years}")
