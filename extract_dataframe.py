from cmath import nan
import json
import pandas as pd
from textblob import TextBlob
from clean_tweets_dataframe import CleanTweets
from clean_tweets_dataframe import init


def read_json(json_file: str) -> list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file

    Returns
    -------
    length of the json file and a list of json
    """

    tweets_data = []
    for tweets in open(json_file, 'r'):
        tweets_data.append(json.loads(tweets))

    return len(tweets_data), tweets_data


class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe

    Return
    ------
    dataframe
    """

    def __init__(self, tweets_list):

        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self) -> list:
        try:
            statuses_count = [x["statuses_count"]
                              for x in self.tweets_list["user"]]
        except KeyError:
            statuses_count = 0

        return statuses_count

    def find_full_text(self) -> list:
        try:
            text = [x for x in self.tweets_list['text']]
        except KeyError:
            text = ''
        return text

    # continue working on this
    def find_sentiments(self, text) -> list:
        polarity = []
        subjectivity = []
        for tweet in text:
            blob = TextBlob(tweet)
            sentiment = blob.sentiment
            polarity.append(sentiment.polarity)
            subjectivity.append(sentiment.subjectivity)
        return polarity, subjectivity

    def find_created_time(self) -> list:
        try:
            created_at = [x for x in self.tweets_list['created_at']]
        except KeyError:
            created_at = ''

        return created_at

    # done
    def find_source(self) -> list:
        try:
            source = [x for x in self.tweets_list['source']]
        except KeyError:
            source = ''

        return source

    def find_screen_name(self) -> list:
        try:
            screen_name = [x["screen_name"]
                           for x in self.tweets_list["user"]]
        except KeyError:
            screen_name = ''

        return screen_name

    def find_followers_count(self) -> list:
        try:
            followers_count = [x["followers_count"]
                               for x in self.tweets_list["user"]]
        except KeyError:
            followers_count = 0

        return followers_count

    def find_friends_count(self) -> list:
        try:
            friends_count = [x["friends_count"]
                             for x in self.tweets_list["user"]]
        except:
            friends_count = 0

        return friends_count

    # done
    def is_sensitive(self) -> list:
        print(self.tweets_list.columns)
        try:
            is_sensitive = [x for x in self.tweets_list["possibly_sensitive"]]
        except KeyError:
            is_sensitive = None

        return is_sensitive

    def find_favourite_count(self) -> list:
        try:
            favorite_count = [
                x for x in self.tweets_list["favorite_count"]]
        except KeyError:
            favorite_count = 0

        return favorite_count

    def find_retweet_count(self) -> list:
        try:
            retweet_count = [x for x in self.tweets_list["retweet_count"]]
        except KeyError:
            retweet_count = 0

        return retweet_count

    # done
    def find_hashtags(self) -> list:
        try:
            hashtags = [x["hashtags"]
                        for x in self.tweets_list["entities"]]
        except KeyError:
            hashtags = []

        return hashtags

    # done
    def find_mentions(self) -> list:
        try:
            mentions = [x["user_mentions"]
                        for x in self.tweets_list["entities"]]
        except:
            mentions = []

        return mentions

    def find_location(self) -> list:
        try:
            location = [x["location"]
                        for x in self.tweets_list["user"]]
        except KeyError:
            location = ''

        return location

    def find_place_coordinates(self, places) -> list:
        wanted_places = []
        for place in places:
            if place != None:
                wanted_places.append(place)

        coordinates = []
        for wanted_place in wanted_places:
            coordinates.append(wanted_place["bounding_box"]["coordinates"])
        return coordinates

    def find_lang(self) -> list:
        try:
            lang = [x["lang"]
                    for x in self.tweets_list["user"]]
        except KeyError:
            lang = ''

        return lang

    def find_place(self) -> list:
        try:
            place = [x for x in self.tweets_list["place"]]
        except KeyError:
            place = ''
        return place

    def get_tweet_df(self, save=False) -> pd.DataFrame:
        """required column to be generated you should be creative and add more features"""

        columns = ['created_at', 'source', 'original_text', 'polarity', 'subjectivity', 'lang', 'favorite_count', 'retweet_count',
                   'original_author', 'followers_count', 'friends_count', 'possibly_sensitive', 'hashtags', 'user_mentions', 'place']

        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count,
                   screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('./processed_tweet_data.csv', index=False)
            print('File Successfully Saved!!!')

        return df


if __name__ == "__main__":

    _, tweet_list = read_json("./data/Economic_Twitter_Data.json")
    wanted_df = pd.DataFrame(tweet_list)
    cleaner = CleanTweets(wanted_df)
    df_after_unwanted_column = cleaner.drop_unwanted_column()
    df_after_drop_duplicate = cleaner.drop_duplicate(df_after_unwanted_column)
    cleaner.convert_to_datetime(df_after_drop_duplicate)
    cleaner.convert_to_numbers()
    cleaner.remove_non_english_tweets()
    tweet_df = cleaner.return_df().head(5)
    tweet = TweetDfExtractor(wanted_df.head(5))
    print(wanted_df.head(5).columns)
    text = tweet.find_full_text()
    print(tweet.is_sensitive())

    # use all defined functions to generate a dataframe with the specified columns above
