import pandas as pd
# from extract_dataframe import


def read_csv_file(filepath):
    data = pd.read_csv(filepath)
    return data


class DataConsistencyChecker:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def duplicity_checker(self, df: pd.DataFrame) -> bool:
        pass

    def schema_consistency(self, df: pd.DataFrame) -> bool:
        pass


if __name__ == "__main__":

    # dcc = DataConsistencyChecker()
    print(pd.DataFrame(read_csv_file("./processed_tweet_data.csv")).duplicated())
