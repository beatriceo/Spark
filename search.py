from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, FloatType, ArrayType, StringType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import explode, split, col, min, max, udf, mean, sqrt, avg
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.functions import col, explode, split, count, lit
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

class Search():

    def __init__(self, datasets, spark):
        self.movies = datasets['movies']
        self.links = datasets['links']
        self.ratings = datasets['ratings'].withColumn("rating", datasets["ratings"].rating.cast(FloatType()))
        self.tags = datasets['tags']
        self.spark = spark
        self.model = self.recommend()

    def search_user_movie_count(self, id):
        return self.ratings.filter(self.ratings.userId == id).count()


    def search_user_movies(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        return df.join(self.movies, self.movies.movieId == self.ratings.movieId)

    def search_users_movies(self, ids):
        return [self.search_user_movies(id) for id in ids]

    '''
    Beatrice
    '''
    def search_user_genre(self, id):
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        return df.groupBy('genres').agg({"*": "count", "rating":"mean"}).withColumnRenamed("count(1)", "watches")

    """
    Given a list of users, return the count of each genre the user has watched
    """
    def search_users_genres(self, ids):
        return [self.search_user_genre(id) for id in ids]

    """
    Given a list of users, return movies each user has watched
    """
    def search_users_movie_counts(self, ids):
        return [self.search_user_movie_count(id) for id in ids]

    """
    Given an ID or name, return the movie/movies that match the name or ID along with 
    average rating and number of watches
    """
    def search_movie(self, id=None, name=None):
        # If the search is given a name
        if id is None:
            # Lazy transformation to get the movies in the dataframe that match the name
            df = self.movies.title.rlike(name)
            # Filter the dataframe to contain only the matched movies
            df = self.movies.filter(df)
            # Lazy join with the rating table's rating and id so that the rating tables entries are
            # only for the matched movies
            df = df.join(self.ratings,'movieId').select("movieId", "rating")
        # If the search is given an id
        elif name is None:
            # Lazy transformation to filter the ratings table by the id
            df = self.ratings.filter(self.ratings.movieId==id)
        else:
            return None
        # Group by id, aggregating the mean of ratings and counting the number of occurances of the ids
        df = df.groupBy("movieId").agg({"*": "count", "rating": "mean"})
        # Reattach movie names to the grouped values and return
        return df.join(self.movies, 'movieId').select("movieId", "avg(rating)", "count(1)", "title")

    """
    Given a year, return all movies that include the year surrounded by brackets from the movies table
    """
    def search_movie_year(self, year):
        return self.movies.filter(self.movies.title.rlike("(" + year + ")"))

    """
    Given a genre, return all movies that match the genre
    """
    def search_genre(self, genre):
        return self.movies.filter(self.movies.genres.rlike(genre))

    """
    Given a list of genres, return all movies that match a genre in the list
    """
    def search_genres(self, genres):
        return [self.search_genre(genre) for genre in genres]
        # # Creates a regex 'or' statement of the genres
        # regex = '({})'.format('|'.join(genres))
        # # Lazy transformation getting all user movie genres that fulfill the regex. Genres are also an or statement,
        # # so any overlap is returned
        # return self.movies.filter(self.movies.genres.rlike(regex))

    '''
       Beatrice
       '''
    def list_rating(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        ratings = ratings.groupBy("movieId").agg({"*": "count", "rating": "mean"}).withColumnRenamed("count(1)", "watches")
        # movies_ratings = ratings.join(movies, ['movieId'])\
        #     .orderBy(["avg(rating)", "watched"], ascending=False)
        ratings = ratings.join(movies, movies.movieId==ratings.movieId).orderBy(["avg(rating)", "watches"], ascending=False)
        return ratings.limit(n).toPandas()
    '''
       Beatrice
       '''
    def list_watches(self, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        # movies_ratings = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        '''sam'''
        watches = ratings.groupBy(col("ratings.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId==watches.movieId).orderBy("watches", ascending=False)
        return watches.limit(n).toPandas()

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
        df = df.withColumn('score', df['avg(rating)'] * ((df['watches']-df['min'])/(df['max']-df['min']))).orderBy("score", ascending=False)
        # df.show()
        # df.toPandas().to_csv("test.csv")
        # df.select(col('genres'), col('score')).show()
        return df
        # return df.select(col('genres'), col('score'))



    """
    Given a user Id, return the movies watched by the user ordered by rating (descending)
    """
    def searched_highest_rated(self, id):
        # Filter the ratings dataframe to contain only results that match the user ID
        df = self.ratings.filter(self.ratings.userId == id)
        # Join with the movies dataframe to get the movie titles
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        # Order by rating
        df = df.orderBy("rating", ascending=False)
        return df

    """
    Given the first 3 digits of a year (representing a decade), return the top movies of 
    the decade sorted by order up to a limit
    """
    def filter_decade(self, decade, limit, order):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')

        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        # Filter the movies to only ones that contain the first 3 digits of the decade followed
        # by any digit surrounded by brackets
        watches = watches.filter(watches.title.rlike("(" + decade + "\d)"))
        watches = watches.groupBy(col("movies.movieId")).agg({"*": "count", "rating": "mean"}).withColumnRenamed("count(1)", "watches")
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy(order,ascending=False)
        return watches.limit(limit).toPandas()

    """
    Return the most watched movies of each decade
    """
    def top_each_decade(self, column):
        most_watched = pd.DataFrame()
        # Call filter decade on every decade between 1900 and 2020 and get the first result
        for i in range(190,202):
            most_watched[str(i)+"0-"+str(i)+"9"] = (self.filter_decade(str(i),1,column)).iloc[0]
        # Return the transposed result
        return most_watched.T

    """
    Given a year and a value n, return the top n movies of that year sorted by order
    """
    def filter_year(self, year, n, order):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        # Filter the movies to only ones that include the year surrounded by brackets
        watches = watches.filter(watches.title.rlike("(" + year + ")"))
        watches = watches.groupBy(col("movies.movieId")).agg({"*": "count", "rating": "mean"}).withColumnRenamed("count(1)", "watches")
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy(order, ascending=False)
        # Return the top n results
        return watches.limit(n).toPandas()

    """
    Given a movie name, return all user tags for the movie
    """
    def search_tags(self, name):
        # Get all movies that match the name
        df = self.movies.title.rlike(name)
        df = self.movies.filter(df)
        # Get the ids for the resulting movies
        df = df.join(self.ratings, 'movieId').select("movieId").distinct()
        # Return the tags for all movies with the id
        return self.tags.join(df, df.movieId==self.tags.movieId).select('tag').collect()

    def compare_users(self, user_a, user_b):

        # Adapted from: https://stackoverflow.com/questions/44580644/subtract-mean-from-pyspark-dataframe/49606192
        def normalize(df, mean_a, mean_b):
            agg_expr = [mean_a, mean_b]
            select_expr = df.columns
            select_expr.append((df['ratingA'] - agg_expr[0]).alias('%s-avg(%s)' % ('ratingA', 'ratingA')))
            select_expr.append((df['ratingB'] - agg_expr[1]).alias('%s-avg(%s)' % ('ratingB', 'ratingB')))
            return df.select(select_expr)

        '''
        Calculate similarity using Pearson's correlation coefficient (slide 9)
        https://personal.utdallas.edu/~nrr150130/cs6375/2015fa/lects/Lecture_23_CF.pdf
        '''

        # get movie ids watched by user a
        user_a_data = self.ratings.filter(self.ratings.userId == user_a) \
            .alias('userA') \
            .select('movieId', col('rating').alias('ratingA'))

        # get movie ids watched by user b
        user_b_data = self.ratings.filter(self.ratings.userId == user_b) \
            .alias('userB') \
            .select('movieId', col('rating').alias('ratingB'))

        intersection = user_a_data.join(user_b_data, ['movieId'], 'inner')

        mean_rating_a = user_a_data.agg({"ratingA": "avg"}).first()[0]
        mean_rating_b = user_b_data.agg({"ratingB": "avg"}).first()[0]

        user_a_count, user_b_count = user_b_data.count(), user_a_data.count()
        min = user_a_count if user_a_count < user_b_count else user_b_count
        overlap_proportion = intersection.count()/min
        # print(overlap_proportion)

        # returns a dataframe with ratings subtracted by their respective mean ratings
        test = normalize(intersection, mean_rating_a, mean_rating_b)

        numerator = test.withColumn('numerator', test['ratingA-avg(ratingA)'] * test['ratingB-avg(ratingB)']) \
            .agg({'numerator': 'sum'}).withColumnRenamed("sum(numerator)", "numerator")

        denom_1_temp = test.withColumn('intermediate', test['ratingA-avg(ratingA)'] * test['ratingA-avg(ratingA)']) \
            .agg({'intermediate': 'sum'})

        # the first sqrt in the denominator
        denom_1 = denom_1_temp.withColumn('denom1', sqrt(col('sum(intermediate)'))).select('denom1')

        denom_2_temp = test.withColumn('intermediate', test['ratingB-avg(ratingB)'] * test['ratingB-avg(ratingB)']) \
            .agg({'intermediate': 'sum'})

        # the second sqrt in the denominator
        denom_2 = denom_2_temp.withColumn('denom2', sqrt(col('sum(intermediate)'))).select('denom2')

        # whole denominator
        denom_all = denom_1.join(denom_2).withColumn('denom', col('denom1') * col('denom2')).select('denom')

        similarity = numerator.join(denom_all) \
            .withColumn('similarity', overlap_proportion * (col('numerator') / col('denom'))).select('similarity')

        return similarity

    # based on https://spark.apache.org/docs/latest/ml-collaborative-filtering.html#collaborative-filtering
    def recommend(self):
        ratings = self.ratings
        ratings = ratings.withColumn("userId", ratings.userId.cast(IntegerType()))
        ratings = ratings.withColumn("movieId", ratings.movieId.cast(IntegerType()))

        (training, test) = ratings.randomSplit([0.8, 0.2])
        als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")

        model = als.fit(training)
        return model

    def recommend_n_movies_for_users(self, n, users):
        model = self.model
        users = self.ratings.where(self.ratings.userId.isin(users)).distinct()

        subset = model.recommendForUserSubset(users, n)

        def get_predicted_movie_ids(l):
            return [x[0] for x in l]

        def get_predicted_ratings(l):
            return [x[1] for x in l]

        get_predicted_movie_ids_udf = udf(get_predicted_movie_ids, ArrayType(IntegerType()))
        get_predicted_ratings_udf = udf(get_predicted_ratings, ArrayType(FloatType()))

        formatted_subset = subset.withColumn('movieIds', get_predicted_movie_ids_udf(col('recommendations'))) \
            .withColumn('predictedRatings', get_predicted_ratings_udf(col('recommendations'))) \
            .select(['userId', explode('movieIds').alias('movieId')])\
            .join(self.movies, 'movieId')\
            .select('userId', 'title')

        return formatted_subset

    def cluster(self, user, n):
        users = self.ratings.select('userId')\
            .distinct()\
            .filter(col('userId') != user)

        print(users.count())

        users_pd = list(users.select('userId').toPandas()['userId'])

        # for each user in users, get their similarity score to user, order by similarity score, limit n
        return sorted([(i, self.compare_users(user, i).first()[0] or 0) for i in users_pd], key=lambda x: x[1], reverse=True)[:n]
