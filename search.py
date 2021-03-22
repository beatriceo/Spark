from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, FloatType, ArrayType, StringType
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql.functions import explode, split, col, min, max, udf, mean, sqrt
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
import pandas as pd
from pyspark.sql.functions import col, explode, split, count
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

    """
    Given a list of users, return movies each user has watched
    """
    def search_users_movies(self, users):
        return [self.search_user_movies(user) for user in users]
        # Creates a regex 'or' statement of the user ids
        regex = '(/^{}$/)'.format('$/|/^'.join(users))
        # Lazy transformation getting all user Ids that fulfill the regex
        df = self.ratings.userId.rlike(regex)
        # Lazy transformation to filter the ratings table to contain only matches
        df = self.ratings.filter(df)
        # Lazy join with the movies table in order to attach genres
        df = df.join(self.movies, self.movies.movieId == self.ratings.movieId)
        # Create new entries for each genre a movie is in so that each entry contains only a single genre
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        # Group by user Id and genre, counting occurrences of each pair as the
        # number of movies in the genre watched by that user
        return df.groupBy("genres", "userId").agg(count("*"))

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
        if name is None:
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

    """
    Given a user id, return a list of their favourite genres, with the score for how much a user likes a genre
    defined as their average rating for the genre multiplied by the number of movies in the genre they have watched
    scaled between 0 and 1
    """
    def search_user_favourites(self, id):
        # Perform the same transformations performed in the search_user_genre method
        df = self.ratings.filter(self.ratings.userId == id)
        df = df.join(self.movies, self.movies.movieId==self.ratings.movieId)
        df = df.withColumn("genres", explode(split(col("genres"), "\\|")))
        # minmax_result = df.groupBy('genres').agg(min("watched").alias("min_watched"),max("watched").alias("max_watched"))
        df = df.groupBy('genres').agg({"*": "count", "rating":"mean"}).withColumnRenamed("count(1)", "watched")
        # Define a function to retieve the first item in each list in a list of lists as a float
        unlist = udf(lambda x: round(float(list(x)[0]), 3), DoubleType())
        # Create an assembler to turn the watched column into a vector (required by MinMax scaler)
        assembler = VectorAssembler(inputCols=["watched"], outputCol="watched_vec")
        # Define a minmax scaler
        scaler = MinMaxScaler(inputCol="watched_vec", outputCol="watched_scaled")
        # Add the vector assemble and pipeline to a pipeline
        pipeline = Pipeline(stages=[assembler, scaler])
        # Fit the scaler to the dataframe and transform the dataframe
        scalerModel = pipeline.fit(df)
        df = scalerModel.transform(df)
        # Create the score column in the dataframe by multiplaying the scaled watched
        # value by the average rating and sort in descending order
        df = df.withColumn('score', df['avg(rating)'] * unlist(df['watched_scaled'])).orderBy("score", ascending=False)
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
    Given the first 3 digits of a year (representing a decade), return the most watched movie of that 
    decade
    """
    def filter_decade(self, decade):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        watches = watches.groupBy(col("movies.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy("watches",ascending=False)
        # Filter the movies to only ones that contain the first 3 digits of the decade followed
        # by any digit surrounded by brackets
        watches = watches.filter(watches.title.rlike("("+decade+"\d)"))
        # Return the top result
        return watches.limit(1).toPandas()

    """
    Return the most watched movies of each decade
    """
    def most_viewed_decade(self):
        most_watched = pd.DataFrame()
        # Call filter decade on every decade between 1900 and 2020
        for i in range(190,202):
            most_watched[str(i)+"0-"+str(i)+"9"] = (self.filter_decade(str(i))).iloc[0]
        # Return the transposed result
        return most_watched.T

    """
    Given a year and a value n, return the n most watched movies of the year
    """
    def most_watched_year(self, year, n):
        # Perform the same transformations performed in list_watches
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        watches = movies.join(ratings, movies.movieId == ratings.movieId)
        watches = watches.groupBy(col("movies.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy("watches", ascending=False)
        # Filter the movies to only ones that include the year surrounded by brackets
        watches = watches.filter(watches.title.rlike("(" + year + ")"))
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
        def normalize(df, columns):
            agg_expr = []
            for column in columns:
                agg_expr.append(mean(df[column]).alias(column))
            averages = df.agg(*agg_expr).collect()[0]
            select_expr = df.columns
            for column in columns:
                select_expr.append((df[column] - averages[column]).alias('%s-avg(%s)' % (column, column)))
            return df.select(select_expr)

        '''
        Calculate similarity using Pearson's correlation coefficient (slide 9)
        https://personal.utdallas.edu/~nrr150130/cs6375/2015fa/lects/Lecture_23_CF.pdf

        let i = movieId
        similarity(x, y) = sum((rating_xi - mean rating x) * (rating_yi - mean rating y)) / (sqrt(sum(rating_xi - mean rating x)^2) * sqrt(sum(rating_yi - mean rating y)^2))
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

        # returns a dataframe with ratings subtracted by their respective mean ratings
        test = normalize(intersection, ['ratingA', 'ratingB'])
        # test.show()

        numerator = test.withColumn('numerator', test['ratingA-avg(ratingA)'] * test['ratingB-avg(ratingB)']) \
            .agg({'numerator': 'sum'}).withColumnRenamed("sum(numerator)", "numerator")
        # numerator.show()

        denom_1_temp = test.withColumn('intermediate', test['ratingA-avg(ratingA)'] * test['ratingA-avg(ratingA)']) \
            .agg({'intermediate': 'sum'})

        # the first sqrt in the denominator
        denom_1 = denom_1_temp.withColumn('denom1', sqrt(col('sum(intermediate)'))).select('denom1')
        # denom_1.show()

        denom_2_temp = test.withColumn('intermediate', test['ratingB-avg(ratingB)'] * test['ratingB-avg(ratingB)']) \
            .agg({'intermediate': 'sum'})

        # the second sqrt in the denominator
        denom_2 = denom_2_temp.withColumn('denom2', sqrt(col('sum(intermediate)'))).select('denom2')
        # denom_2.show()

        similarity = numerator.join(denom_1).join(denom_2)\
            .withColumn('similarity', col('numerator') / col('denom1') * col('denom2')).select('similarity')
        # similarity.show()
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
        model = self.recommend()
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
