from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import explode, split, col, min, max, udf
from pyspark.ml import Pipeline
from pyspark.sql.window import Window


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
        pass

    def search_users(self, users):
        # return [self.search_user(user) for user in users]
        for user in users:
            self.search_user(user)
        pass

    def search_movie_name(self, id=None, name=None):
        if id is None:
            res=self.movies.filter(self.movies.title == "Toy Story (1995)").join(self.ratings,self.ratings.movieId==self.movies.movieId).agg({"*": "count", "rating":"mean"})
        if name is None:
            res = self.ratings.filter(self.ratings.movieId==(self.movies.filter(self.movies.title==name).movieId)).agg({"*": "count", "rating":"mean"})

    def search_movie_year(self, year):
        return self.movies.filter(self.movies.title.rlike("(" + year + ")"))

    def search_genre(self, genre):
        return self.movies.filter(self.movies.title.rlike(genre))

    def search_genres(self, genres):
        # return [self.search_user(user) for user in users]
        for genre in genres:
            self.search_genre(genre)
        pass

    '''
       Beatrice
       '''
    def list_rating(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        movies_ratings = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        # I've just done a groupBy on movieTitle instead of movieId but this might not be the best idea
        # because the README says there might be errors in the titles
        return movies_ratings.groupBy(col("movies.title")).avg("rating").orderBy("avg(rating)", ascending=False).take(n)

    '''
       Beatrice
       '''
    def list_watches(self, n):
        pass

    def search_user_favourites(self, id):
        # return [self.search_user(user) for user in users]
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
        df = df.withColumn('score', df['avg(rating)'] * unlist(df['watched_scaled'])).sort("score").show()
        pass