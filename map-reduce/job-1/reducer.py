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
            price_sum = 0,
            price_min = float("inf"),
            price_max = float("-inf"),
            years = set()
        )

    group[key]["num_car"] += count
    group[key]["price_sum"] += price
    group[key]["price_min"] = min(price, group[key]["price_min"])
    group[key]["price_max"] = max(price, group[key]["price_max"])
    group[key]["years"].add(year)

for key in group.keys():
    obj = group[key]

    make_name, model_name = key.split("\t")

    num_cars = obj["num_car"]
    avg_value = round(obj["price_sum"] / num_cars, 2)
    years = sorted(set(obj["years"]))
    min_value = obj["price_min"]
    max_value = obj["price_max"]

    print(f"{make_name}\t{model_name}\t{num_cars}\t{min_value}\t{max_value}\t{avg_value}\t{years}")
