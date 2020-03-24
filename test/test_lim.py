import pandas as pd
from lim import lim
import unittest


class TestMP(unittest.TestCase):

    def test_lim_query(self):
        q = 'Show \r\nFB: FB FP: FP when date is after 2019'
        res = lim.query(q)
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
        res = lim.query(q)
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

        res = lim.curve('FB', curve_dates=pd.to_datetime('2020-03-17'))
        self.assertIn('FB', res.columns)

    def test_curve_history(self):
        res = lim.curve('FP', curve_dates=[pd.to_datetime('2020-03-17'), pd.to_datetime('2020-03-18')])
        self.assertIn('2020/03/17', res.columns)
        self.assertIn('2020/03/18', res.columns)

    def test_symbol_contracts(self):
        res = lim.get_symbol_contract_list('FB', monthly_contracts_only=True)
        self.assertIn('FB_1998J', res)
        self.assertIn('FB_2020Z', res)

    def test_futures_contracts(self):
        res = lim.futures_contracts('FB')
        self.assertIn('FB_2020Z', res.columns)

    def test_cont_futures_rollover(self):
        res = lim.continuous_futures_rollover('FB', months=['M1', 'M2'], after_date=2019)
        print(res.head())
        self.assertEqual(res['FB_M1'][pd.to_datetime('2020-01-02')], 66.25)
        self.assertEqual(res['FB_M2'][pd.to_datetime('2020-01-02')], 65.56)



if __name__ == '__main__':
    unittest.main()