from sklearn.ensemble import forest
from contextlib import contextmanager
from sklearn.ensemble import RandomForestClassifier
import pandas as pd

@contextmanager
def set_rf_samples(n):
    """ Changes Scikit learn's random forests to give each tree a random sample of
    n random rows.
    """
    forest._generate_sample_indices = (lambda rs, n_samples:
        forest.check_random_state(rs).randint(0, n_samples, n))

    yield

    forest._generate_sample_indices = (lambda rs, n_samples:
           forest.check_random_state(rs).randint(0, n_samples, n_samples))


def get_significant_features(rf:RandomForestClassifier, feature_list):
    # Get numerical feature importances
    importances = list(rf.feature_importances_)
    # List of tuples with variable and importance
    feature_importances = [(feature, round(importance, 2)) for feature, importance in zip(feature_list, importances)]
    # Sort the feature importances by most important first
    feature_importances = sorted(feature_importances, key=lambda x: x[1], reverse=True)
    return feature_importances


def _reduce(train, valid, n_est=10, ):


    from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    with set_rf_samples(30000):
        m = RandomForestClassifier(n_jobs=-1, n_estimators=100, oob_score=True, min_samples_leaf=1, max_features=0.5)
        m.fit(train[0], train[1])

    from jupyter_utils import score
    acc = score.accuracy(m, valid[0], valid[1], score.informedness)

    return m, acc

def reduce(df, target, leftover=0.8):
    models = []

    from imblearn.over_sampling import SMOTE

    from jupyter_utils import sample
    train, valid, test = sample.random_split(df)
    X_train, y_train, X_valid, y_valid, X_test, y_test = train.drop(target, axis=1), train[target], valid.drop(target,
                                                                                                               axis=1), \
                                                         valid[target], test.drop(target, axis=1), test[target]

    def run_smote(x, y):
        sm = SMOTE(random_state=12, ratio=1.0)
        X_train_r, y_train_r = sm.fit_sample(x,y)
        return (pd.DataFrame(data=X_train_r, columns=x.columns), y_train_r)

    (x_train_os, y_train_os) = run_smote(X_train, y_train)
    (x_valid_os, y_valid_os) = run_smote(X_valid, y_valid)

    for i in range(1,101,20):
        print("training model with n_est {}".format(i))
        m, acc = _reduce((x_train_os, y_train_os), (x_valid_os, y_valid_os), n_est=i)
        features = [f[0] for f in get_significant_features(m, x_train_os.columns.tolist())]
        p = int(len(features) * leftover)
        x_train_os = x_train_os[features[0:p]]
        x_valid_os = x_valid_os[features[0:p]]
        print(len(x_train_os.columns.tolist()))
        models.append((m, acc, features[0:p]))

    return models, (x_train_os, y_train_os), (x_valid_os, y_train_os)
