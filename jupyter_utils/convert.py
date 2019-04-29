from pandas.api.types import is_string_dtype, is_numeric_dtype, is_categorical_dtype
import multiprocessing as mp
from jupyter_utils.datatype import *
import pandas as pd
import numpy as np
from functools import partial


def _to_categorical(series):
    if is_string_dtype(series):
        return series.astype('category').cat.as_ordered()
    else:
        return series


def _coerce_to_numeric(col, cutoff, logger, ignore=list()):
    if is_numeric(col.dtype):
        return col

    if is_timestamp(col.dtype):
        return col

    if col.name in ignore:
        return col

    values = col.dropna().unique()
    ratio = 0
    pre_length = len(values)

    if pre_length > 0:
        new_values = pd.to_numeric(col.dropna().unique(), errors='coerce')
        post_length = len(new_values[~np.isnan(new_values)])
        ratio = (pre_length - post_length) / pre_length

    if ratio <= cutoff:
        logger.debug("Converting column {} to numeric (ratio: {})".format(col, ratio))
        return pd.to_numeric(col, errors='coerce')
    else:
        logger.debug("Not converting column {} (ratio: {})".format(col, ratio))
        return col


def coerce_to_numeric(df, logger, cutoff=0.10, ignore=list()) -> pd.DataFrame:
    new_df = df.copy()
    with mp.Pool(mp.cpu_count()-1) as pool:
        for series in pool.imap(partial(_coerce_to_numeric, cutoff=cutoff, logger=logger, ignore=ignore),
                                (df[col] for col in df.columns)):
            new_df[series.name] = series

    return new_df


def categorical_to_codes(df, max_n_cat=50):
    df_copy = df.copy()
    for col in df_copy.columns.tolist():
        if is_categorical_dtype(df_copy[col].dtype):
            df_copy[col] = df_copy[col].cat.codes+1

    return df_copy


def to_onehot(df):
    return pd.get_dummies(df, columns=[col for col in df.columns.tolist() if is_categorical_dtype(df[col].dtype)])


def to_categorical(df, logger):
    new_df = df.copy()
    """Change any columns of strings in a panda's dataframe to a column of
    categorical values. This applies the changes inplace.

    Parameters:
    -----------
    df: A pandas dataframe. Any columns of strings will be changed to
        categorical values.

    Examples:
    ---------

    >>> df = pd.DataFrame({'col1' : [1, 2, 3], 'col2' : ['a', 'b', 'a']})
    >>> df
       col1 col2
    0     1    a
    1     2    b
    2     3    a

    note the type of col2 is string

    >>> train_cats(df)
    >>> df

       col1 col2
    0     1    a
    1     2    b
    2     3    a

    now the type of col2 is category
    """


    with mp.Pool(mp.cpu_count()-1) as pool:
        for series in pool.imap(_to_categorical, (df[col] for col in df.columns if is_string_dtype(df[col].dtype))):
            #logger.info("Processed {}".format(series.name))
            new_df[series.name] = series

    return new_df