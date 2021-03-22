from search import Search
import pandas as pd
from matplotlib import pyplot as plt
from wordcloud import WordCloud

class Plotting():

    def __init__(self, datasets, spark):
        self.search = Search(datasets, spark)

    """
    Use matplot to generate a graphical report for a given user, showing what proportion 
    of watched movies each genre makes up and their favourite movies and the average rating they gave each genre
    """
    def gen_user_report(self, id, favourites=8):
        # # Get the users highest rated movies
        # self.search.searched_highest_rated(id)
        plt.plo
        fig, axs = plt.subplots(2, figsize=(10,20))
        fig.suptitle('User report')
        df = pd.read_csv("./test.csv").sort_values('watched', ascending=False)
        # Get the user's favourite movies
        df = self.search.search_user_favourites(id).toPandas()
        # Group all genres that place after the favourites parameter into 'others'
        df_other = df[:favourites].copy()
        new_row = pd.DataFrame(data={
            'genres': ['others'],
            'watched': [df['watched'][favourites:].sum()],
            'avg(rating)': [df['avg(rating)'][favourites:].mean()]
        })
        # combine the top values with others
        df_other = pd.concat([df_other, new_row])
        axs[0].pie(df_other['watched'], labels=df_other['genres'], autopct='%1.1f%%',
                shadow=True, startangle=90)
        axs[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        axs[1].bar(df_other['genres'], df_other['avg(rating)'])
        axs[1].set_ylabel('Average rating')
        axs[1].set_xlabel('Highest rated genres')
        plt.show()
        pass

    """
    Display the most watched movies for each decade and of all time, and the highest rated 
    movies of all time on bar charts
    """
    def gen_movies_report(self):
        # Get the tables to be plotted
        movies_ratings = self.search.list_rating(10).toPandas()
        movies_watched = self.search.list_watches(10).toPandas()
        most_watched_decade = self.search.most_viewed_decade()
        fig, axs = plt.subplots(3, figsize=(20,20))
        fig.suptitle('Movies report')
        axs[0].bar([i+"\n"+j.replace(" ","\n") for i,j in zip(most_watched_decade.index,movies_watched['title'])], movies_watched['watches'])
        axs[0].set_ylabel('Watches')
        axs[0].set_xlabel('Movies')
        plt.xticks(rotation=20)
        axs[1].bar([i+"\n"+j.replace(" ","\n") for i,j in zip(most_watched_decade.index,most_watched_decade['title'])], most_watched_decade['watches'])
        axs[1].set_ylabel('Watches')
        axs[1].set_xlabel('Movies')
        plt.xticks(rotation=20)
        axs[2].bar([i+"\n"+j.replace(" ","\n") for i,j in zip(most_watched_decade.index,movies_ratings['title'])], movies_ratings['avg(rating)'])
        axs[2].set_ylabel('Average rating')
        axs[2].set_xlabel('Movies')
        plt.xticks(rotation = 20)
        plt.show()
        return

    """
    Given a year and a limit, plot the most watched movies of the year
    """
    def gen_most_watched_year(self, year, limit):
        most_watched = self.search.most_watched_year(year, limit)
        fig, axs = plt.subplots()
        axs.bar(
            [str(i) + "\n" + j.replace(" ", "\n") for i, j in zip(most_watched.index, most_watched['title'])],
            most_watched['watches'])
        axs.set_ylabel('Watches')
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

        plt.suptitle(movie+" tags word cloud")
        # Display the generated image:
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.show()

