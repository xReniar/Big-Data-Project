import argparse
import subprocess
import time
import os
import matplotlib.pyplot as plt


parser = argparse.ArgumentParser()
parser.add_argument("job", type=str, choices=["job-1", "job-2"], help="Job name")
parser.add_argument("--fractions", type=str, help="Fractions of dataset to use")
args = parser.parse_args()

tools = ["map-reduce", "spark-core", "spark-sql"]
fractions = list(map(lambda x: f"data-{float(x) * 100}%", args.fractions.split())) + ["data_cleaned"]
exec_times = []

for tool in tools:
    for fraction in fractions:
        start = time.time()
        process = subprocess.run(
            ["bash", "run.sh", args.job, fraction],
            cwd=tool,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        end = time.time()
        
        print(f"Finished {args.job} for dataset \"{fraction}\"")
        exec_times.append(end - start)

        output_path = os.path.join("logs", tool, args.job)
        os.makedirs(output_path, exist_ok=True)
        with open(os.path.join(output_path, f"stdout-{fraction}.txt"), "wb") as f:
            f.write(process.stdout)

        '''
        with open(os.path.join(output_path, f"stderr-{fraction}.txt"), "wb") as f:
            f.write(process.stderr)
        '''

plt.figure(figsize=(8, 5))
plt.plot(fractions, exec_times, marker='o', linestyle='-', color='blue', label='Execution Time')
plt.xlabel('Dataset Fraction (%)')
plt.ylabel('Processing Time (seconds)')
plt.title(f'{args.tool}: {args.job}')
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join("logs", args.tool, f'benchmark_{args.tool}_{args.job}.png'), dpi=300)