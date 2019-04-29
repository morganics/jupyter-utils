from sklearn.ensemble import forest
from contextlib import contextmanager
from sklearn.ensemble import RandomForestClassifier

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