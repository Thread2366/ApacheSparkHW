python3 input_generator.py

hdfs dfs -rm input.csv
hdfs dfs -put input.csv

hdfs dfs -rm -r output


spark-submit --class SparkApplication --master yarn --deploy-mode cluster $1 input.csv output $2
