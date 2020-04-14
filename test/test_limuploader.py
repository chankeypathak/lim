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
        df = pd.DataFrame([r], index=[dn], columns=['TopRelation:Test:SPOTPRICE;TopColumn:Price:Close'])

        dfmeta = {
            'description': 'desc'
        }

        limuploader.upload_series(df, dfmeta)
        df = lim.series('SPOTPRICE')
        self.assertAlmostEquals(df.loc[dn]['SPOTPRICE'], r, 2)



if __name__ == '__main__':
    unittest.main()