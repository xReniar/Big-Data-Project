# Big Data Project
The dataset that's been used is the [US Used Cards](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset) dataset, with about 3 million records where each record has 66 columns.

Tests were made with:
- Linux Ubuntu 22.04
- i5-9600k 3.70GHz, 16GB RAM
- Java 11
- [Hadoop 3.4.1](https://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz)
- [Spark 3.5.5](https://www.apache.org/dyn/closer.lua/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz)

## Setup environment
This project makes use of `Hadoop` and `Spark` so make sure to install them first, after installing set the env variabiles. In my case I get the following output:
```bash
echo $JAVA_HOME
# /usr/lib/jvm/java-1.11.0-openjdk-amd64

echo $HADOOP_HOME
# /home/rainer/hadoop-3.4.1

echo $SPARK_HOME
# /home/rainer/spark-3.5.5-bin-hadoop3
```

Then create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt --no-cache-dir
```

# How to use
This section explain gives a description on what the jobs do and how to execute them.

Start by executing `download.sh`, this will download the dataset:
```bash
bash download.sh
```
After this run `local.sh` to start `Hadoop` and `Spark`:
```bash
bash local.sh
```

## Job 1
## Job 2
## Job 3