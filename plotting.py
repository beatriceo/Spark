from search import Search
import pandas as pd
from matplotlib import pyplot as plt
from wordcloud import WordCloud
import datetime

class Plotting():

    def __init__(self, search):
        self.search = search

    """
    Use matplot to generate a graphical report for a given user, showing what proportion 
    of watched movies each genre makes up and their favourite movies and the average rating they gave each genre
    """
    def gen_user_report(self, id, favourites=8):
        # # Get the users highest rated movies
        # self.search.searched_highest_rated(id)
        fig, axs = plt.subplots(2, figsize=(20,20))
        fig.suptitle('User report')
        # Get the user's favourite movies
        df = self.search.search_user_genre(id)
        df = df.orderBy("watches", ascending=False).toPandas()
        if df.shape[0]>favourites:
            # Group all genres that place after the favourites parameter into 'others'
            df_other = df[:favourites].copy()
            new_row = pd.DataFrame(data={
                'genres': ['others'],
                'watches': [df['watches'][favourites:].sum()],
                'avg(rating)': [df['avg(rating)'][favourites:].mean()]
            })
            # combine the top values with others
            df = pd.concat([df_other, new_row])
        axs[0].pie(df['watches'], labels=df['genres'], autopct='%1.1f%%',
                shadow=True, startangle=90)
        axs[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        axs[0].set_title("Proportion of movies watched in each genre")
        axs[1].bar(df['genres'], df['avg(rating)'])
        axs[1].set_ylabel('Average rating')
        axs[1].set_xlabel('Highest rated genres')
        axs[1].set_title("Average rating for each genre watched")
        plt.xticks(rotation=20)
        plt.show()
        pass


    """
    Given a function that returns a dataframe and a column name, plot the values in the column in a bar chart
    """
    def generic_plot(self, most_watched, column):
        fig, axs = plt.subplots(figsize=(20, 10))
        axs.bar(
            [str(i) + "\n" + j.replace(" ", "\n") for i, j in zip(most_watched.index, most_watched['title'])],
            most_watched[column])
        axs.set_ylabel(column)
        axs.set_xlabel('Movies')
        plt.xticks(rotation=20)


    """
    Given a movie, generate a word cloud of the most frequent tags for the movie
    """
    def gen_movie_wordcloud(self, movie):
        tags = self.search.search_tags(movie)
        # Join the tags into a string separated by spaces to be used by wordcloud
        text = " ".join([tag['tag'] for tag in tags])
        wordcloud = WordCloud().generate(text)
        plt.rcParams["figure.figsize"] = (10,10)
        plt.suptitle(movie+" tags word cloud")
        # Display the generated image:
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

