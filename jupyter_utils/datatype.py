import numpy as np


def is_numeric(dtype):
    return is_float(dtype) or is_int(dtype)

def is_float(dtype):
    return str(dtype) in {"float32", "float64"}

def is_int(dtype):
    return str(dtype) in {"int32", "int64", "uint32", "uint64"}

def is_bool(dtype):
    return str(dtype) == "bool"

def is_string(dtype):
    return str(dtype) == "object" or str(dtype) == "O"

def is_timestamp(dtype):
    for ts in ['timestamp64', 'timedelta64', 'datetime64']:
        if ts in str(dtype):
            return True

    return False

def could_be_int(col):
    if is_int(col.dtype):
        return True

    if is_float(col.dtype):
        for val in col.dropna().unique():
            if int(val) != val:
                return False

        return True

    return False


def get_categorical(df):
    return df.select_dtypes(['category'])


def get_continuous(df):
    return df[[col for col in df.columns if is_float(df[col].dtype)]]


def get_bool(df):
    return df.select_dtypes(['bool'])