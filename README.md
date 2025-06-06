# US-Used-Cars dataset analysis
The dataset that's been used is the [US Used Cards](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset) dataset, with about 3 million records where each record has 66 columns.

Tests were made with:
- Linux Ubuntu 22.04
- AMD Ryzen 7 5800X 8-Core Processor, 32GB RAM
- Java 11
- [Hadoop 3.4.1](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz)
- [Spark 3.5.5](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)

## Setup environment
This project makes use of `Hadoop` and `Spark` so make sure to install them first, after installing set the env variabiles. In my case I get the following output:
```bash
echo $JAVA_HOME
> /usr/lib/jvm/java-1.11.0-openjdk-amd64

echo $HADOOP_HOME
> /home/rainer/hadoop-3.4.1

echo $SPARK_HOME
> /home/rainer/spark-3.5.5-bin-hadoop3
```

Then create a virtual environment (Required only for `benchmark.py`):
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt --no-cache-dir
```

# How to use
This section explain gives a description on what the jobs do and how to execute them.

To start `Hadoop` use `setup.sh`:
```bash
source setup.sh
```
Then download dataset using `download.sh`, clean it and put it in HDFS with `generate_data.sh` :
```bash
cd dataset
bash download.sh
bash generate_data.sh local[*]
```
> [!NOTE]
> If `data` folder containing `data_cleaned.csv` already exists just run `generate_data.sh`, make sure to execute it inside `dataset` folder.
- `generate_data.sh` will clean the original dataset and create portions of the cleaned dataset. The portions are passed with the `--fractions` flag, to create more datasets modify `line:21` of `generate_data.sh`. After the execution a `data` folder will be created inside `dataset`, this contains the `cleaned_data.csv` that will be reused in case `setup.sh` will be executed again.
- To stop `hadoop` just run:
```bash
$HADOOP_HOME/sbin/stop-dfs.sh
```

## Run scripts
Each folder contains a `run.sh`, it takes 2 arguments:
- the name of the job
- the name of the dataset
- the `master` type, can be `yarn` or `local[*]`
```bash
# example for spark-core using data-20.0%

cd spark-core
bash run.sh job-1 data-20.0% local[*]
# after this a "spark-core/job-1" folder will appear in HDFS
```

Results are saved in HDFS, the structure of the directory in the HDFS is shown below:
```bash
.
└── user
    └── $USER
        ├── data
        │   ├── data_cleaned.csv
        │   └── ...
        ├── map-reduce
        │   └── *
        ├── spark-core
        │   └── *
        └── spark-sql
            └── *
```

## Benchmark
To see the execution time of each tool run:
```bash
bash experiments.sh
```
This will create a `log` folder with the output and an image for each job showing the execution times.