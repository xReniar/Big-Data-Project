#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()
    _, _, _, _, _, make_name, model_name, price, year = line.split(",")

    print(f"{make_name}\t{model_name}\t1\t{price}\t{year}")