import math
import sklearn.metrics

def rmse(x,y): return math.sqrt(((x-y)**2).mean())


def show(m, X_train, y_train, X_valid, y_valid):
    res = [m.score(X_train, y_train), m.score(X_valid, y_valid)]
    if hasattr(m, 'oob_score_'): res.append(m.oob_score_)
    print(res)


def binary_errors(df, y_pred, y_valid):
    df['actual'] = y_valid
    df['errors'] = 'tp'
    df['predicted'] = y_pred
    df.loc[df['predicted'] > df['actual'], 'errors'] = 'fp'
    df.loc[df['predicted'] < df['actual'], 'errors'] = 'fn'
    df.loc[(df['predicted'] == 0) & (df['actual'] == 0), 'errors'] = 'tn'
    return df


def accuracy(m, X_valid, y_valid, callback=None):
    res = sklearn.metrics.confusion_matrix(y_valid, m.predict(X_valid), labels=None,
                                                      sample_weight=None).ravel()
    result = {'tn':res[0], 'fp':res[1], 'fn': res[2], 'tp': res[3], 'accuracy': 1-((res[1] + res[2]) / (res[0]+res[3])) }
    if hasattr(m, 'oob_score_'):
        result.update({'oob_score': m.oob_score_})

    x = None
    if callback is not None:
        x = callback(*res)

    return result, x

def roc(m, X_valid, y_valid):
    preds = m.predict_proba(X_valid)[:,1]
    fpr, tpr, threshold = sklearn.metrics.roc_curve(y_valid, preds)
    roc_auc = sklearn.metrics.auc(fpr, tpr)
    ## then plot with..
    ## plt.plot(fpr, tpr)
    ## plt.plot([0, 1], [0, 1],'r--')
    ## plt.show()

    return fpr, tpr, roc_auc, threshold

def cutoff_youdens_j(fpr, tpr, thresholds):
    import numpy as np
    '''Optimal cutoff for roc curve'''

    j_scores = tpr-fpr
    j_ordered = sorted(zip(j_scores,thresholds))
    v1 =  j_ordered[-1][1]

    optimal_idx = np.argmin(np.abs(tpr - fpr))
    optimal_threshold = thresholds[optimal_idx]
    return v1, optimal_threshold

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