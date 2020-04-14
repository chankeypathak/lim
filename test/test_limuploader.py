import pandas as pd
from lim import limuploader
from lim import lim
import unittest
from random import random
from datetime import datetime


class TestLimUploader(unittest.TestCase):

    def test_upload_series(self):
        r = random()
        dn = datetime.now().date()
        df = pd.DataFrame([r], index=[dn], columns=['SPOTPRICE'])

        dfmeta = {
            'treepath': 'TopRelation:Test:SPOTPRICE',
            'column': 'TopColumn:Price:Close',
            'description': 'desc'
        }

        limuploader.upload_series(df, dfmeta)
        df = lim.series('SPOTPRICE')
        self.assertAlmostEquals(df.loc[dn]['SPOTPRICE'], r, 2)



if __name__ == '__main__':
    unittest.main()