from pyspark.sql import SparkSession

class Search():

    def __init__(self, datasets, spark):
        self.movies = datasets['movies']
        self.links = datasets['links']
        self.ratings = datasets['ratings']
        self.tags = datasets['tags']
        self.spark = spark

    def search_user(self, id):
        pass

    def search_users(self, users):
        # return [self.search_user(user) for user in users]
        for user in users:
            self.search_user(user)
        pass

    def search_movie_name(self, id=None, name=None):
        if id is None:
            pass
        if name is None:
            pass

    def search_movie_year(self, year):
        pass

    def search_genre(self, genre):
        pass

    def search_genres(self, genres):
        # return [self.search_user(user) for user in users]
        for genre in genres:
            self.search_genre(genre)
        pass

    def list_rating(self, n):
        pass

    def list_watches(self, n):
        pass