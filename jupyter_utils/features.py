import jupyter_utils.datatype
import numpy as np
import pandas as pd



def remove_variable_variables(df: pd.DataFrame):
    column_names = df.select_dtypes(include=['object', 'categorical']).apply(lambda x: len(x.unique()) != len(df))
    return df[column_names[column_names == True].index.tolist()].copy()


def remove_mostly_empty_variables(df: pd.DataFrame, cutoff=0.1):
    length = len(df)
    new_df = df.copy()
    for column in df.columns.tolist():
        if len(df[column].dropna()) / length <= cutoff:
            new_df = new_df.drop(column, axis=1)

    return new_df


def remove_variables_too_much_variance(df: pd.DataFrame, num_states = 30):
    column_names = df.select_dtypes(include=['object', 'category']).apply(lambda x: len(x.unique()) >= num_states)
    cols = list(set(df.columns.tolist()) - set(column_names[column_names == True].index.tolist()))
    return df[cols].copy()


def create_dateparts(df, drop=True, time=False):
    """add_datepart converts a column of df from a datetime64 to many columns containing
    the information from the date. This applies changes inplace.

    Parameters:
    -----------
    df: A pandas data frame. df gain several new columns.
    fldname: A string that is the name of the date column you wish to expand.
        If it is not a datetime64 series, it will be converted to one with pd.to_datetime.
    drop: If true then the original date column will be removed.
    time: If true time features: Hour, Minute, Second will be added.

    Examples:
    ---------

    >>> df = pd.DataFrame({ 'A' : pd.to_datetime(['3/11/2000', '3/12/2000', '3/13/2000'], infer_datetime_format=False) })
    >>> df

        A
    0   2000-03-11
    1   2000-03-12
    2   2000-03-13

    >>> add_datepart(df, 'A')
    >>> df

        AYear AMonth AWeek ADay ADayofweek ADayofyear AIs_month_end AIs_month_start AIs_quarter_end AIs_quarter_start AIs_year_end AIs_year_start AElapsed
    0   2000  3      10    11   5          71         False         False           False           False             False        False          952732800
    1   2000  3      10    12   6          72         False         False           False           False             False        False          952819200
    2   2000  3      11    13   0          73         False         False           False           False             False        False          952905600
    """
    new_df = df.copy()
    for series in [new_df[col] for col in df.columns if jupyter_utils.datatype.is_timestamp(new_df[col].dtype)]:
        attr = ['Year', 'Month', 'Week', 'Day', 'Dayofweek', 'Dayofyear',
                'Is_month_end', 'Is_month_start', 'Is_quarter_end', 'Is_quarter_start', 'Is_year_end', 'Is_year_start']
        if time: attr = attr + ['Hour', 'Minute', 'Second']
        for n in attr: new_df[series.name + "_" + n] = getattr(series.dt, n.lower())
        new_df[series.name + '_Elapsed'] = series.astype(np.int64) // 10 ** 9
        if drop: new_df.drop(series.name, axis=1, inplace=True)

    return new_df