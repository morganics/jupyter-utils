import pandas as pd
from pandas.api.types import is_string_dtype, is_numeric_dtype
import logging
import bayesianpy
import numpy as np

def find(df: pd.DataFrame):
    return (df.isnull().sum().sort_index() / len(df)).sort_values(ascending=False)


def drop(df: pd.DataFrame):
    x = find(df) == 1
    return df.drop(x.index, axis=1)




class Impute:
    def __init__(self):
        pass

    def fill_missing_cols(self):
        pass


class MedianImpute(Impute):
    def __init__(self, df: pd.DataFrame, logger: logging.Logger):
        super().__init__()
        self._df = df
        self._missing_indices = {}
        self._logger = logger

    def fill_continuous(self, create_missing_cols=True):
        df = self._df.copy()
        for col in df.columns:
            if is_numeric_dtype(df[col]):
                if create_missing_cols:
                    col_is_missing = pd.isnull(df[col])
                    if any(col_is_missing == True):
                        df["{}_missing".format(col)] = col_is_missing

                if pd.isnull(df[col]).sum():
                    df[col] = df[col].fillna(df[col].median())

        return df

    def fill_categorical(self, create_missing_cols=True):
        df = self._df.copy()
        for col in df.columns:
            if is_string_dtype(df[col]):
                if create_missing_cols:
                    col_is_missing = pd.isnull(df[col])
                    if any(col_is_missing == True):
                        df["{}_missing".format(col)] = col_is_missing

                codes = df[col].cat.codes
                if pd.isnull(df[col]).sum():
                    cats = df[col].cat.categories
                    cat_median = int(np.median(codes))

                    df[col] = df[col].fillna(cats[cat_median])

        return df

    def fill(self, mode='both', create_missing_cols=True) -> pd.DataFrame:
        df = None
        if mode == 'both' or mode == 'categorical':
            df = self.fill_categorical(create_missing_cols=create_missing_cols)
            self._df = df

        if mode == 'both' or mode == 'continuous':
            df = self.fill_categorical(create_missing_cols=create_missing_cols)

        return df

class BayesianImpute(Impute):

    def __init__(self, df:pd.DataFrame, logger:logging.Logger):
        super().__init__()
        self._df = df.copy()
        self._missing_indices = {}
        self._logger = logger
        self._autotype = bayesianpy.data.AutoType(self._df)


    def _train(self):

        nf = bayesianpy.network.NetworkFactory(self._logger)
        subset = self._df.sample(frac=0.02)
        self._logger.info("Selected {} records".format(len(subset)))
        with bayesianpy.data.DefaultDataSet(subset) as dataset:
            tpl = bayesianpy.template.MixtureNaiveBayes(self._logger,
                                                                   discrete=self._df[self._autotype.get_discrete_variables()],
                                                                   continuous=self._df[self._autotype.get_continuous_variables()]
                                                                   )
            nt = tpl.create(nf)
            nm = bayesianpy.model.NetworkModel(nt, self._logger)
            nm.train(dataset)
        return nm

    def fill_missing_cols(self):
        bayesianpy.jni.attach()
        model = self._train()
        self._logger.info("Finished training, querying whole dataset now.")
        disc = self._autotype.get_discrete_variables()
        cont = self._autotype.get_continuous_variables()
        with bayesianpy.data.DefaultDataSet(self._df) as dataset:
            queries = []
            q = bayesianpy.output.BatchQuery(model.get_network(), dataset, self._logger)
            for col in self._df.columns:
                if col in disc:
                    queries.append(bayesianpy.output.QueryFactory(
                        bayesianpy.output.QueryMostLikelyState,
                        target_variable_name=col,
                        output_dtype=self._df[col].dtype,
                        suffix=''))
                else:
                    queries.append(bayesianpy.output.QueryFactory(
                        bayesianpy.output.QueryMeanVariance,
                        target=col,
                        output_dtype=self._df[col].dtype,
                        result_mean_suffix=''
                    ))

            results = q.query(queries, append_to_df=False)
            self._missing_df = self._df.isnull()

        for col in self._df.columns:
            indices = np.where(self._df[col].isna())
            self._missing_indices.update({col: indices})

        self._df[self._df.isnull()] = results

        return self._df


