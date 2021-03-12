from search import Search
import pandas as pd
from matplotlib import pyplot as plt

class Plotting():

    def __init__(self, datasets, spark):
        self.search = Search(datasets, spark)

    def gen_user_report(self, id, favourites=8):
        self.search.searched_highest_rated(id)
        # results = self.search.search_user_favourites(id).collect()
        fig, axs = plt.subplots(2)
        fig.suptitle('User report')
        df = pd.read_csv("./test.csv").sort_values('watched', ascending=False)
        df_other = df[:favourites].copy()
        new_row = pd.DataFrame(data={
            'genres': ['others'],
            'watched': [df['watched'][favourites:].sum()],
            'avg(rating)': [df['avg(rating)'][favourites:].mean()]
        })
        # combining top 5 with others
        df_other = pd.concat([df_other, new_row])
        axs[0].pie(df_other['watched'], labels=df_other['genres'], autopct='%1.1f%%',
                shadow=True, startangle=90)
        axs[0].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        df = pd.read_csv("./test.csv").sort_values('avg(rating)', ascending=False)
        df_other = df[:favourites].copy()
        new_row = pd.DataFrame(data={
            'genres': ['others'],
            'watched': [df['watched'][favourites:].sum()],
            'avg(rating)': [df['avg(rating)'][favourites:].mean()]
        })
        # combining top 5 with others
        df_other = pd.concat([df_other, new_row])
        axs[1].bar(df_other['genres'], df_other['avg(rating)'])
        axs[1].set_ylabel('Average rating')
        axs[1].set_xlabel('Highest rated genres')
        plt.show()
        pass

    def gen_movies_report(self):
        movies_ratings = self.search.list_rating(10).toPandas()
        movies_watched = self.search.list_watches(10).toPandas()
        most_watched_decade = self.search.most_viewed_decade()
        fig, axs = plt.subplots(2)
        fig.suptitle('Movies report')
        axs[0].bar(movies_watched['title'], movies_watched['watches'])
        axs[0].set_ylabel('Watches')
        axs[0].set_xlabel('Movies')

        axs[1].bar([i+"\n"+j.replace(" ","\n") for i,j in zip(most_watched_decade.index,most_watched_decade['title'])], most_watched_decade['watches'])
        axs[1].set_ylabel('Watches')
        axs[1].set_xlabel('Movies')
        plt.xticks(rotation = 20)
        return