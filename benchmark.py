import argparse
import subprocess
import time
import os
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser()
parser.add_argument("tool", type=str, choices=["map-reduce", "spark-core", "spark-sql"], help="Tool to benchmark")
parser.add_argument("--fractions", type=str, help="Dataset fractions to use for benchmark")
args = parser.parse_args()

datasets = list(map(lambda x: f"data-{float(x) * 100}%", args.fractions.split()))

for dataset in datasets:
    for i in range(2):
        start = time.time()
        process = subprocess.run(
            ["bash", "run.sh", f"job-{i + 1}", dataset],
            cwd=args.tool,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        end = time.time()
        print(f"Finished job-{i + 1} for dataset \"{dataset}\"")
        exection_time = end - start

        output_path = os.path.join("logs", args.tool, f"job-{i+1}")
        os.makedirs(output_path, exist_ok=True)
        with open(os.path.join(output_path, f"stdout-{dataset}.txt"), "wb") as f:
            f.write(process.stdout)

        with open(os.path.join(output_path, f"stderr-{dataset}.txt"), "wb") as f:
            f.write(process.stderr)