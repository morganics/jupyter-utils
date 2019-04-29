import unittest, yaml, unittest.mock as mock, logging
import pandas as pd
import jupyter_utils.missing, jupyter_utils.categoricals
import numpy as np

class ImputeTest(unittest.TestCase):

    def test_median_impute_provides_continuous_median_values(self):
        input =  [1, 2, 3, 4, np.nan, 6, 1, 2, 3, 4, 5, 6]
        df = pd.DataFrame({'A': input})
        total = df['A'].dropna().sum()
        mi = jupyter_utils.missing.MedianImpute(df, logging.getLogger())

        new_df = mi.fill_missing_cols()
        self.assertEqual(new_df['A'].sum() - total, 3, "Expecting the value 3 to be backfilled.")

    def test_median_impute_provides_discrete_median_values(self):
        input =  ['A', 'B', 'C', 'D', np.nan, 'F', 'A', 'B', 'C', 'D', 'E', 'F']
        df = pd.DataFrame({'A': input})
        jupyter_utils.categoricals.convert(df)

        mi = jupyter_utils.missing.MedianImpute(df, logging.getLogger())

        new_df = mi.fill_missing_cols(create_missing_cols=True)
        self.assertEqual(len(new_df[new_df['A'] == 'C']), 3, "Expecting the value C to be backfilled.")
        self.assertTrue("A_missing" in new_df.columns)
        self.assertFalse(all(new_df['A_missing'] == False))
        self.assertEqual(len(new_df[new_df['A_missing'] == True]), 1)

