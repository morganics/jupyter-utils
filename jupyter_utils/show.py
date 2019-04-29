import pandas as pd

from contextlib import contextmanager


@contextmanager
def all(df, cols=10000, rows=1000):
    with pd.option_context("display.max_rows", rows, "display.max_columns", cols):
        yield df


@contextmanager
def all_cols(df):
    with all(df, rows=50):
        yield df


@contextmanager
def all_rows(df):
    with all(df, cols=50):
        yield df


def missing(df):
    return df.isnull().sum().sort_index() / len(df)