
def random_split(df, train_size=0.5, valid_size=0.3):
    n = len(df)*train_size
    n_valid = len(df)*valid_size
    df_copy = df.copy()
    train = df_copy.sample(n=int(n), random_state=200)
    df_copy = df_copy.drop(train.index)
    valid = df_copy.sample(n=int(n_valid))
    test = df_copy.drop(valid.index)
    return train, valid, test
