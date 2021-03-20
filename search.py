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

    def most_watched_year(self, year, n):
        ratings = self.ratings.alias('ratings')
        movies = self.movies.alias('movies')
        watches = movies.join(ratings, movies.movieId == ratings.movieId)  # join movies and ratings on movieId
        '''sam'''
        watches = watches.groupBy(col("movies.movieId")).agg(count("*").alias("watches"))
        watches = watches.join(movies, movies.movieId == watches.movieId).orderBy("watches", ascending=False)
        watches = watches.filter(watches.title.rlike("(" + year + ")"))
        return watches.limit(n).toPandas()

    def search_tags(self, name):
        df = self.movies.title.rlike(name)
        df = self.movies.filter(df)
        df = df.join(self.ratings, 'movieId').select("movieId").distinct()
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
