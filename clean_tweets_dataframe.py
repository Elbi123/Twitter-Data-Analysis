import pandas as pd
import json


def read_json_file(json_file: str) -> list:
    tweets_data = []
    for tweets in open(json_file, 'r'):
        tweets_data.append(json.loads(tweets))
    return tweets_data


class CleanTweets:
    """
    This class is responsible for cleaning the twitter dataframe

    Returns:
    --------
    A dataframe
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')

    def return_df(self):
        return self.df

    def drop_unwanted_column(self) -> pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = self.df[self.df['retweet_count']
                                == 'retweet_count'].index
        self.df.drop(unwanted_rows, inplace=True)

        return self.df

    def drop_duplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        drop duplicate rows
        """
        df = df.loc[df.astype(str).drop_duplicates().index]

        #  = df.drop_duplicates(keep='first')

        return df

    def convert_to_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        convert column to datetime
        """
        df['created_at'] = pd.to_datetime(
            df['created_at'], errors='coerce')

        df = df[df['created_at'] >= '2020-12-31']

        return df

    def convert_to_numbers(self) -> pd.DataFrame:
        """
        convert columns like retweet_count
        favorite_count to numbers
        """

        self.df['retweet_count'] = pd.to_numeric(
            self.df['retweet_count'], errors='coerce')
        self.df['favorite_count'] = pd.to_numeric(
            self.df['favorite_count'], errors='coerce')

        return self.df

    def remove_non_english_tweets(self) -> pd.DataFrame:
        """
        remove non english tweets from lang
        """

        self.df = self.df.query("lang == 'en' ")

        return self.df


def init(filepath: str):
    tweet_df = read_json_file(filepath)
    wanted_df = pd.DataFrame(tweet_df)
    cleaner = CleanTweets(wanted_df)
    df_after_unwanted_column = cleaner.drop_unwanted_column()
    df_after_drop_duplicate = cleaner.drop_duplicate(df_after_unwanted_column)
    cleaner.convert_to_datetime(df_after_drop_duplicate)
    cleaner.convert_to_numbers()
    cleaner.remove_non_english_tweets()


if __name__ == "__main__":
    filepath = "/home/ubuntu/projects/10academy/Twitter-Data-Analysis/data/Economic_Twitter_Data.json"
    init(filepath)
