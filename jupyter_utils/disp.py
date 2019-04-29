import pandas as pd

from contextlib import contextmanager

@contextmanager
def all(df):
    with pd.option_context("display.max_rows", 1000, "display.max_columns", 1000):
        yield df


def missing(df):
    return df.isnull().sum().sort_index() / len(df)