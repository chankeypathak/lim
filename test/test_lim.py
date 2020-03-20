import pandas as pd
from lim import lim
import unittest


class TestMP(unittest.TestCase):

    def test_lim_query(self):
        q = 'Show \r\nFB: FB FP: FP when date is after 2019'
        res = lim.call_lim_api_query(q)
        self.assertIsNotNone(res)
        self.assertIn('FB', res.columns)
        self.assertIn('FP', res.columns)

    def test_extneded_query(self):
        q = '''
        LET
        FP = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "actual prices")
        FP_M2 = FP(ROLLOVER_DATE = "5 days before expiration day",ROLLOVER_POLICY = "2 nearby actual prices")
        
        SHOW
        FP: FP
        FP_02: FP_M2
        '''
        res = lim.call_lim_api_query(q)
        self.assertIsNotNone(res)
        self.assertIn('FP', res.columns)
        self.assertIn('FP_02', res.columns)

    def test_series(self):
        res = lim.series('FP_2020J')
        self.assertEqual(res['FP_2020J']['2020-01-02'], 608.5)

        res = lim.series({'FP_2020J' : 'GO', 'FB_2020J' : 'Brent'})
        self.assertEqual(res['GO']['2020-01-02'], 608.5)
        self.assertEqual(res['Brent']['2020-01-02'], 65.56)

    def test_curve(self):
        res = lim.curve({'FP': 'GO', 'FB': 'Brent'})
        self.assertIn('GO', res.columns)
        self.assertIn('Brent', res.columns)


if __name__ == '__main__':
    unittest.main()