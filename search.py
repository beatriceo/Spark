from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.types import IntegerType


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
            id = self.movies.filter(self.movies.title == "Toy Story (1995)").collect()[0].movieId
            res = self.ratings.filter(self.ratings.movieId==id).agg({"*": "count", "rating":"mean"}).agg({"*": "count", "rating":"mean"})
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
