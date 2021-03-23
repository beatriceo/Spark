from pyspark.sql import SparkSession
import os
from search import Search
from plotting import Plotting

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
    plotting = Plotting(datasets, spark)
    # plotting.gen_movie_wordcloud("Toy Story")
    # plotting.gen_movies_report()
    # plotting.gen_user_report("1")
    search = Search(datasets, spark)
    # search.search_tags("Toy Story")
    # search.search_user_movies("1")
    # search.search_genre("Adventure")
    # search.search_movie_name(None, "Toy Story (1995)")
    # search.search_movie_name(None, "Toy Story")
    # search.search_movie_name(1, None)
    # search.search_genre("Adventure")
    # search.list_rating(5)
    # search.search_user_favourites("1")
    # search.search_users_movies(["1","2"])
    search.compare_users("1","2")
    pass

if __name__ == '__main__':
    main()
