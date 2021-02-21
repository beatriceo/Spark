from pyspark.sql import SparkSession
import os
from search import Search


def store(spark, datasets):
    pass

def read(spark):
    datasets = {}
    for dataset in os.listdir("./ml-latest-small"):
        data = spark.read.option("header", True).format("csv").load(os.path.join(os.getcwd(),"ml-latest-small",dataset))
        datasets[dataset.split('.')[0]]=data
    return datasets

def main():
    spark = SparkSession.builder.master("local").appName("Movies").config("conf-key", "conf-value").getOrCreate()
    datasets = read(spark)
    search = Search(datasets, spark)
    # search.search_user_movies("1")
    # search.search_genre("Adventure")
    search.search_movie_name(None, "Toy Story (1995)")
    # search.search_genre("Adventure")
    search.list_rating(5)
    pass

if __name__ == '__main__':
    main()
