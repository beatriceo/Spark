from pyspark.sql import SparkSession
import os


def search(spark, dataset, id):
    pass

def store(spark, datasets):
    pass

def read(spark):
    datasets = {}
    df = spark.readc
    for dataset in os.listdir("./ml-latest-small"):
        data = spark.read.option("header", True).format("csv").load(os.path.join(os.getcwd(),"ml-latest-small",dataset))
        datasets[dataset.split('.')[0]]=data
    return datasets

def main():
    spark = SparkSession.builder.master("local").appName("Movies").config("conf-key", "conf-value").getOrCreate()
    datasets = read(spark)
    pass

if __name__ == '__main__':
    main()