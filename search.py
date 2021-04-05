from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, FloatType, ArrayType, StringType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import explode, split, col, min, max, udf, mean, sqrt, avg, arrays_zip, asc, desc
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.functions import col, explode, split, count, lit, from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS


class Search():

    def __init__(self, datasets, spark):
        self.movies = datasets['movies']
        self.links = datasets['links']
        self.ratings = datasets['ratings'].withColumn("rating", datasets["ratings"].rating.cast(FloatType())) \
            .withColumn("timestamp", from_unixtime(datasets["ratings"].timestamp, 'yyyy-MM-dd HH:mm:ss'))
        self.tags = datasets['tags']
        self.spark = spark
        self.model_explicit, self.model_implicit, self.training, self.test = self.recommend()

    '''
    This method caches frequent operations that use a given user ID so that other operations on that ID are faster
    '''

    def cache_user(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df.cache()
        df.join(self.movies, self.movies.movieId == self.ratings.movieId).cache()

    '''
    This method caches frequent operations that use a given movie ID so that other operations on that ID are faster
    '''

    def cache_movie(self, id):
        self.ratings.filter(self.ratings.movieId == id).cache()
        self.movies.filter(self.movies.movieId == id).cache()
        self.ratings.join(self.movies, self.movies.movieId == self.ratings.movieId).cache()

    '''
    This method caches frequent operations for movies with a given string (name or year) in the title
    '''

    def cache_movie_string(self, string):
        df = self.movies.title.rlike(string)
        # Filter the dataframe to contain only the matched movies
        df = self.movies.filter(df)
        df.cache()
        # Lazy join with the rating table's rating and id so that the rating tables entries are
        # only for the matched movies
        self.ratings.join(df, 'movieId').select("movieId", "rating").cache()

    '''
    Returns a count of how many movies a given user has watched
    '''

    def search_user_movie_count(self, id):
        return self.ratings.filter(self.ratings.userId == id).count()

    '''
    Given a user id, return a list of movies that user has watched
    '''

    def search_user_movies(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        return df.join(self.movies, ['movieId'])

    '''
    Given a list of users, for each user, get a list of movies they have watched
    '''

    def search_users_movies(self, ids):
        return [self.search_user_movies(id) for id in ids]

    '''
    Given a user, return the count of each genre the user has watched
    '''

    def search_user_genre(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        return df.groupBy('genres').agg({"*": "count", "rating": "mean"}).withColumnRenamed("count(1)", "watches")

    '''
    Given a list of users, return the count of each genre the user has watched
    '''

    def search_users_genres(self, ids):
        return [self.search_user_genre(id) for id in ids]

    '''
    Given a list of users, return movies each user has watched
    '''

    def search_users_movie_counts(self, ids):
        return [self.search_user_movie_count(id) for id in ids]

    '''
    Given an ID or name, return the movie/movies that match the name or ID along with 
    average rating and number of watches
    '''

    def search_movie(self, id=None, name=None):
        # If the search is given a name
        if id is None:
            # Lazy transformation to get the movies in the dataframe that match the name
            df = self.movies.title.rlike(name)
            # Filter the dataframe to contain only the matched movies
            df = self.movies.filter(df)
            # Lazy join with the rating table's rating and id so that the rating tables entries are
            # only for the matched movies
            df = self.ratings.join(df, 'movieId').select("movieId", "rating")
        # If the search is given an id
        elif name is None:
            # Lazy transformation to filter the ratings table by the id
            df = self.ratings.filter(self.ratings.movieId == id)
        else:
            return None
        # Group by id, aggregating the mean of ratings and counting the number of occurances of the ids
        df = df.groupBy("movieId").agg({"*": "count", "rating": "mean"})
        # Reattach movie names to the grouped values and return
        return df.join(self.movies, 'movieId').select("movieId", "avg(rating)", "count(1)", "title").withColumnRenamed(
            "count(1)", "watches")

    '''
    Given a year, return all movies that include the year surrounded by brackets from the movies table
    '''

    def search_movie_year(self, year):
        return self.movies.filter(self.movies.title.rlike("(" + year + ")"))

    '''
    Given a genre, return all movies that match the genre
    '''

    def search_genre(self, genre):
        return self.movies.filter(self.movies.genres.rlike(genre))

    '''
    Given a list of genres, return all movies that match a genre in the list
    '''

    def search_genres(self, genres):
        return [self.search_genre(genre) for genre in genres]

    '''
    List the top n movies with highest rating, ordered by the rating
    '''

    def list_rating(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')

        ratings = ratings.groupBy("movieId").agg({"*": "count", "rating": "mean"}).withColumnRenamed("count(1)",
                                                                                                     "watches")
        ratings = ratings.join(movies, movies.movieId == ratings.movieId).orderBy(["avg(rating)", "watches"],
                                                                                  ascending=False)
        df = ratings.limit(n).toPandas()
        # drop duplicated column based on:
        # https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns
        return df.loc[:, ~df.columns.duplicated()]

    '''
    List the top n movies with the highest number of watches, ordered by the number of watches
    '''

    def list_watches(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')

        '''Sam'''
        watches = ratings.groupBy(col("ratings.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy("watches", ascending=False)

        df = watches.limit(n).toPandas()
        # drop duplicated column based on:
        # https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns
        return df.loc[:, ~df.columns.duplicated()]

    """
        Given a user id, return a list of their favourite genres, with the score for how much a user likes a genre
        defined as their average rating for the genre multiplied by the number of movies in the genre they have watched
        scaled between 0 and 1
    """

    def search_user_favourites(self, id):
        # Get the dataframe containing how many movies and average rating of
        # movies in each genre and sort by number watched
        df = self.search_user_genre(id).orderBy("watches", ascending=False)
        # Create columns for the min and max watched value
        max = df.agg({"watches": "max"}).take(1)[0][0]
        min = df.agg({"watches": "min"}).take(1)[0][0]
        # Append the columns to the dataframe
        df = df.withColumn("max", lit(max))
        df = df.withColumn("min", lit(min))
        # Create a new column with the score as the average rating times the
        # number watched scaled using the min and max column
        df = df.withColumn('score',
                           df['avg(rating)'] * ((df['watches'] - df['min']) / (df['max'] - df['min']))).orderBy("score",
                                                                                                                ascending=False)
        return df

    '''
    Given a user Id, return the movies watched by the user ordered by rating (descending)
    '''

    def searched_highest_rated(self, id):
        # Filter the ratings dataframe to contain only results that match the user ID
        df = self.ratings.filter(self.ratings.userId == id)
        # Join with the movies dataframe to get the movie titles
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        # Order by rating
        df = df.orderBy("rating", ascending=False)
        return df

    '''
    Given the first 3 digits of a year (representing a decade), return the top movies of 
    the decade sorted by order up to a limit
    '''

    def filter_decade(self, decade, limit, order):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')

        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        # Filter the movies to only ones that contain the first 3 digits of the decade followed
        # by any digit surrounded by brackets
        watches = watches.filter(watches.title.rlike("(" + decade + "\d)"))
        watches = watches.groupBy(col("movies.movieId")).agg({"*": "count", "rating": "mean"}).withColumnRenamed(
            "count(1)", "watches")
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy(order, ascending=False)
        return watches.limit(limit).toPandas()

    '''
    Return the most watched movies of each decade
    '''

    def top_each_decade(self, column):
        most_watched = pd.DataFrame()
        # Call filter decade on every decade between 1900 and 2020 and get the first result
        for i in range(190, 202):
            most_watched[str(i) + "0-" + str(i) + "9"] = (self.filter_decade(str(i), 1, column)).iloc[0]
        # Return the transposed result
        return most_watched.T

    '''
    Given a year and a value n, return the top n movies of that year sorted by order
    '''

    def filter_year(self, year, n, order):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        # Filter the movies to only ones that include the year surrounded by brackets
        movies = movies.filter(movies.title.rlike("(" + year + ")"))
        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        watches = watches.groupBy(col("movies.movieId")).agg({"*": "count", "rating": "mean"}).withColumnRenamed(
            "count(1)", "watches")
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy(order, ascending=False)
        # Return the top n results
        return watches.limit(n).toPandas()

    '''
    Given a movie name, return all user tags for the movie
    '''

    def search_tags(self, name):
        # Get all movies that match the name
        df = self.movies.title.rlike(name)
        df = self.movies.filter(df)
        # Get the ids for the resulting movies
        df = df.join(self.ratings, 'movieId').select("movieId").distinct()
        # Return the tags for all movies with the id
        return self.tags.join(df, df.movieId == self.tags.movieId).select('tag').collect()

    '''
    Calculate similarity using Pearson's correlation coefficient (slide 9)
    https://personal.utdallas.edu/~nrr150130/cs6375/2015fa/lects/Lecture_23_CF.pdf
    '''

    def compare_users(self, user_a, user_b):

        # Adapted from: https://stackoverflow.com/questions/44580644/subtract-mean-from-pyspark-dataframe/49606192
        def normalize(df, mean_a, mean_b):
            agg_expr = [mean_a, mean_b]
            select_expr = df.columns
            select_expr.append((df['ratingA'] - agg_expr[0]).alias('%s-avg(%s)' % ('ratingA', 'ratingA')))
            select_expr.append((df['ratingB'] - agg_expr[1]).alias('%s-avg(%s)' % ('ratingB', 'ratingB')))
            return df.select(select_expr)

        # Get movie ids watched by user a
        user_a_data = self.ratings.filter(self.ratings.userId == user_a) \
            .alias('userA') \
            .select('movieId', col('rating').alias('ratingA'))

        user_a_data.cache()

        # Get movie ids watched by user b
        user_b_data = self.ratings.filter(self.ratings.userId == user_b) \
            .alias('userB') \
            .select('movieId', col('rating').alias('ratingB'))

        user_b_data.cache()

        intersection = user_a_data.join(user_b_data, ['movieId'], 'inner')
        intersection.cache()

        mean_rating_a = user_a_data.agg({"ratingA": "avg"}).first()[0]
        mean_rating_b = user_b_data.agg({"ratingB": "avg"}).first()[0]

        user_a_count, user_b_count = user_b_data.count(), user_a_data.count()
        min = user_a_count if user_a_count < user_b_count else user_b_count
        overlap_proportion = intersection.count() / min  # Sam's multiplier

        # Returns a dataframe with ratings subtracted by their respective mean ratings
        ratings_subtracted = normalize(intersection, mean_rating_a, mean_rating_b)
        ratings_subtracted.cache()

        numerator = ratings_subtracted.withColumn('numerator',
                                                  ratings_subtracted['ratingA-avg(ratingA)'] * ratings_subtracted[
                                                      'ratingB-avg(ratingB)']) \
            .agg({'numerator': 'sum'}).withColumnRenamed("sum(numerator)", "numerator")

        denom_1_temp = ratings_subtracted.withColumn('intermediate',
                                                     ratings_subtracted['ratingA-avg(ratingA)'] * ratings_subtracted[
                                                         'ratingA-avg(ratingA)']) \
            .agg({'intermediate': 'sum'})

        # The first sqrt in the denominator
        denom_1 = denom_1_temp.withColumn('denom1', sqrt(col('sum(intermediate)'))).select('denom1')

        denom_2_temp = ratings_subtracted.withColumn('intermediate',
                                                     ratings_subtracted['ratingB-avg(ratingB)'] * ratings_subtracted[
                                                         'ratingB-avg(ratingB)']) \
            .agg({'intermediate': 'sum'})

        # The second sqrt in the denominator
        denom_2 = denom_2_temp.withColumn('denom2', sqrt(col('sum(intermediate)'))).select('denom2')

        # Whole denominator
        denom_all = denom_1.join(denom_2).withColumn('denom', col('denom1') * col('denom2')).select('denom')

        similarity = numerator.join(denom_all) \
            .withColumn('similarity', overlap_proportion * (col('numerator') / col('denom'))).select('similarity')

        return similarity

    '''
    Recommend movies with the ALS algorithm using explicit or explicit feedback.
    Based on https://spark.apache.org/docs/latest/ml-collaborative-filtering.html#collaborative-filtering
    '''

    def recommend(self):
        ratings = self.ratings
        ratings = ratings.withColumn("userId", ratings.userId.cast(IntegerType()))
        ratings = ratings.withColumn("movieId", ratings.movieId.cast(IntegerType()))

        (training, test) = ratings.randomSplit([0.8, 0.2])

        als_explicit = ALS(maxIter=5, regParam=0.01,
                           userCol="userId", itemCol="movieId", ratingCol="rating",
                           coldStartStrategy="drop")

        als_implicit = ALS(maxIter=5, regParam=0.01, implicitPrefs=True,
                           userCol="userId", itemCol="movieId", ratingCol="rating",
                           coldStartStrategy="drop")

        model_explicit = als_explicit.fit(training)
        model_implicit = als_implicit.fit(training)
        return model_explicit, model_implicit, training, test

    '''
    Recommend the top n recommended movies for a list of users using either implicit or explicit feedback models
    '''
    def recommend_n_movies_for_users(self, n, users, implicit=False):
        model = self.model_implicit if implicit else self.model_explicit
        users = self.ratings.where(self.ratings.userId.isin(users)).distinct()

        subset = model.recommendForUserSubset(users, n)

        formatted_subset = subset.withColumn('recs_exp', explode('recommendations')) \
            .select('userId', col('recs_exp.movieId'), col('recs_exp.rating').alias('rating')) \
            .join(self.movies, 'movieId') \
            .select('userId', 'title', 'rating') \
            .orderBy(asc('userId'), desc('rating')) \
            .select('userId', 'title')

        return formatted_subset

    '''
    Cluster users based on movie rating similarity scores. Given a user u, find the top n users with the highest
    similarity scores to u.
    '''

    def cluster(self, user, n):
        users = self.ratings.select('userId') \
            .distinct() \
            .filter(col('userId') != user)

        users_pd = list(users.select('userId').toPandas()['userId'])

        # For each user in users, get their similarity score to user, order by similarity score, limit n
        return sorted([(i, self.compare_users(user, i).first()[0] or 0) for i in users_pd], key=lambda x: x[1],
                      reverse=True)[:n]
