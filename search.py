from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import explode, split, col, min, max, udf
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.functions import col, explode, split, count
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import MinMaxScaler

class Search():

    def __init__(self, datasets, spark):
        self.movies = datasets['movies']
        self.links = datasets['links']
        self.ratings = datasets['ratings'].withColumn("rating", datasets["ratings"].rating.cast(IntegerType()))
        self.tags = datasets['tags']
        self.spark = spark

    def search_user_movies(self, id):
        return self.ratings.filter(self.ratings.userId == id).count()

    '''
    Beatrice
    '''
    def search_user_genre(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        return df.groupBy("genres").agg(count("*"))

    def search_users_movies(self, users):
        return [self.search_user_movies(user) for user in users]
#         for user in users:
#             self.search_user(user)
#         pass

    def search_movie(self, id=None, name=None):
        if id is None:
            df = self.movies.title.rlike(name)
            df = self.movies.filter(df)
            df = df.join(self.ratings,'movieId').select("movieId", "rating")
            return df.groupBy("movieId").agg({"*": "count", "rating": "mean"}).join(self.movies,'movieId').select("movieId","avg(rating)","count(1)","title")
        if name is None:
            return self.ratings.filter(self.ratings.movieId==id).groupBy("movieId").agg({"*": "count", "rating":"mean"}).join(self.movies,'movieId').select("movieId","avg(rating)","count(1)","title")

    def search_movie_year(self, year):
        return self.movies.filter(self.movies.title.rlike("(" + year + ")"))

    def search_genre(self, genre):
        return self.movies.filter(self.movies.genres.rlike(genre))

    def search_genres(self, genres):
        return [self.search_genre(genre) for genre in genres]

    '''
       Beatrice
       '''
    def list_rating(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        movies_ratings = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        # I've just done a groupBy on movieTitle instead of movieId but this might not be the best idea
        # because the README says there might be errors in the titles
        movies_ratings = movies_ratings.groupBy(col("movies.movieId")).agg({"*": "count", "rating":"mean"}).withColumnRenamed("count(1)", "watched")
        movies_ratings = movies_ratings.join(movies, ['movieId'])\
            .orderBy(["avg(rating)", "watched"], ascending=False)
        return movies_ratings.limit(n)

    '''
       Beatrice
       '''
    def list_watches(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        movies_ratings = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        '''sam'''
        movies_ratings = movies_ratings.groupBy(col("movies.movieId")).agg(count("*").alias("watches"))
        movies_ratings = movies_ratings.join(movies, ["movieId"])\
            .orderBy("watches", ascending=False)
        return movies_ratings.limit(n)

    def search_user_favourites(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId==self.ratings.movieId)
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        # minmax_result = df.groupBy('genres').agg(min("watched").alias("min_watched"),max("watched").alias("max_watched"))
        df = df.groupBy('genres').agg({"*": "count", "rating":"mean"}).withColumnRenamed("count(1)", "watched")
        unlist = udf(lambda x: round(float(list(x)[0]), 3), DoubleType())
        assembler = VectorAssembler(inputCols=["watched"], outputCol="watched_vec")
        scaler = MinMaxScaler(inputCol="watched_vec", outputCol="watched_scaled")
        pipeline = Pipeline(stages=[assembler, scaler])
        scalerModel = pipeline.fit(df)
        df = scalerModel.transform(df)
        df = df.withColumn('score', df['avg(rating)'] * unlist(df['watched_scaled'])).orderBy("score", ascending=False)
        df.toPandas().to_csv("test.csv")
        # df.select(col('genres'), col('score')).show()
        return df.select(col('genres'), col('score'))

    def searched_highest_rated(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        df = df.orderBy("rating", ascending=False)
        return df

    def filter_decade(self, decade):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        watches = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        '''sam'''
        watches = watches.groupBy(col("movies.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy("watches",ascending=False)
        watches = watches.filter(watches.title.rlike("("+decade+"\d)"))
        return watches.limit(1).toPandas()

    def most_viewed_decade(self):
        most_watched = pd.DataFrame()
        for i in range(190,202):
            most_watched[str(i)+"0-"+str(i)+"9"] = (self.filter_decade(str(i))).iloc[0]

        return most_watched.T
