import unittest, yaml, unittest.mock as mock, logging
import pandas as pd
import jupyter_utils.missing, jupyter_utils.categoricals
import numpy as np

class CategoricalsTest(unittest.TestCase):

    def test_categoricals_convert(self):
        input =  [1, 2, 3, 4, np.nan, 6, 1, 2, 3, 4, 5, 6]
        df = pd.DataFrame({'A': input, 'B': ['A','B','C','D','E','F','G','H','I','J','K', 'L']})

        mi = jupyter_utils.categoricals.convert(df)
        self.assertEqual(mi['B'].dtype.name, 'category')
