import math
import sklearn.metrics

def rmse(x,y): return math.sqrt(((x-y)**2).mean())


def show(m, X_train, y_train, X_valid, y_valid):
    res = [m.score(X_train, y_train), m.score(X_valid, y_valid)]
    if hasattr(m, 'oob_score_'): res.append(m.oob_score_)
    print(res)

def accuracy(m, X_valid, y_valid, callback=None):
    res = sklearn.metrics.confusion_matrix(y_valid, m.predict(X_valid), labels=None,
                                                      sample_weight=None).ravel()

    return {'tn':res[0], 'fp':res[1], 'fn': res[2], 'tp': res[3]}, callback(*res)

def recall(tn,fp,fn,tp):
    return true_positive_rate(tn, fp, fn, tp)

def precision(tn, fp, fn, tp):
    return tp/(tp+fp)

def true_positive_rate(tn, fp, fn, tp):
    return tp/(fn+tp)

def false_positive_rate(tn, fp, fn, tp):
    return fp/(fp+fn)

def positive_likelihood_ratio(tn, fp, fn, tp):
    return true_positive_rate(tn, fp, fn, tp) / false_positive_rate(tn, fp, fn, tp)

def informedness(tn, fp, fn, tp):
    # TPR-FPR, the magnitude of which gives the probability of an informed decision between the two classes
    # (>0 represents appropriate use of information, 0 represents chance-level performance, <0 represents perverse use of information)
    return true_positive_rate(tn, fp, fn, tp) - false_positive_rate(tn, fp, fn, tp)