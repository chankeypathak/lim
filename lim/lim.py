import pandas as pd
import os
import re
import time
from lxml import etree
import requests
from functools import lru_cache
import logging
import hashlib


limServer = os.environ['LIMSERVER'].replace('"', '')
limUserName = os.environ['LIMUSERNAME'].replace('"', '')
limPassword = os.environ['LIMPASSWORD'].replace('"', '')

lim_datarequests_url = '{}/rs/api/datarequests'.format(limServer)
lim_schema_futurues_url = '{}/rs/api/schema/relations/<SYMBOL>?showChildren=true&desc=true&showColumns=false&dateRange=true'.format(limServer)

calltries = 50
sleep = 2.5

curyear = pd.datetime.now().year
prevyear = curyear - 1

headers = {
    'Content-Type': 'application/xml',
}

proxies = {
    'http': os.getenv('http_proxy'),
    'https': os.getenv('https_proxy')
}


def alternate_col_val(values, noCols):
    for x in range(0, len(values), noCols):
        yield values[x:x + noCols]


def query_hash(query):
    r = hashlib.md5(query.encode()).hexdigest()
    rf = '{}.h5'.format(r)
    return rf


def build_dataframe(reports):
    columns = [x.text for x in reports.iter(tag='ColumnHeadings')]
    dates = [x.text for x in reports.iter(tag='RowDates')]
    values = [float(x.text) for x in reports.iter(tag='Values')]
    values = list(alternate_col_val(values, len(columns)))

    df = pd.DataFrame(values, columns=columns, index=pd.to_datetime(dates))
    return df


def query_cached(q):
    qmod = q
    res_cache = None
    rf = query_hash(q)
    if os.path.exists(rf):
        res_cache = pd.read_hdf(rf, mode='r')
        if res_cache is not None and 'date is after' not in q:
            cutdate = (res_cache.iloc[-1].name + pd.DateOffset(-5)).strftime('%m/%d/%Y')
            qmod += ' when date is after {}'.format(cutdate)

    res = query(qmod)
    hdf = pd.HDFStore(rf)
    if res_cache is None:
        hdf.put('d', res, format='table', data_columns=True)
        hdf.close()
    else:
        res = pd.concat([res_cache, res], sort=True).drop_duplicates()
        hdf.put('d', res, format='table', data_columns=True)
        hdf.close()

    return res


def query(q, id=None, tries=calltries, cache_inc=False):
    if cache_inc:
        return query_cached(q)

    r = '<DataRequest><Query><Text>{}</Text></Query></DataRequest>'.format(q)

    if tries == 0:
        raise Exception('Run out of tries')

    if id is None:
        resp = requests.request("POST", lim_datarequests_url, headers=headers, data=r, auth=(limUserName, limPassword), proxies=proxies)
    else:
        uri = '{}/{}'.format(lim_datarequests_url, id)
        resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword), proxies=proxies)
    status = resp.status_code
    if status == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        reqStatus = int(root.attrib['status'])
        if reqStatus == 100:
            res = build_dataframe(root[0])
            return res
        elif reqStatus == 130:
            logging.info('No data')
        elif reqStatus == 200:
            logging.debug('Not complete')
            reqId = int(root.attrib['id'])
            time.sleep(sleep)
            return query(q, reqId, tries - 1)
        else:
            raise Exception(root.attrib['statusMsg'])
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)


def build_series_query(symbols):
    q = 'Show \n'
    for symbol in symbols:
        q += '{}: {}\n'.format(symbol, symbol)
    return q


def series(symbols):
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    q = build_series_query(scall)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    return res


def build_let_show_when_helper(lets, shows, whens):
    query = '''
            LET
            {0}
            SHOW
            {1}
            WHEN
            {2}
        '''.format(lets, shows, whens)
    return query


def build_curve_history_query(symbols, column='Close', curve_dates=None):
    lets, shows, whens = '', '', ''
    counter = 0
    for curve_date in curve_dates:
        counter += 1
        curve_date_str, curve_date_str_nor = curve_date.strftime("%m/%d/%Y"), curve_date.strftime("%Y/%m/%d")

        inc_or = ''
        if len(curve_dates) > 1 and counter != len(curve_dates):
            inc_or = 'OR'
        lets += 'ATTR x{0} = forward_curve({1},"{2}","{3}","","","days","",0 day ago)\n'.format(counter, symbols[0], column, curve_date_str)
        shows += '{0}: x{1}\n'.format(curve_date_str_nor, counter)
        whens += 'x{0} is DEFINED {1}\n'.format(counter, inc_or)
    return build_let_show_when_helper(lets, shows, whens)


def build_curve_query(symbols, column='Close', curve_date=None):
    lets, shows, whens = '', '', ''
    counter = 0

    for symbol in symbols:
        counter += 1
        curve_date_str = "LAST" if curve_date is None else curve_date.strftime("%m/%d/%Y")

        inc_or = ''
        if len(symbols) > 1 and counter != len(symbols):
            inc_or = 'OR'

        lets += 'ATTR x{0} = forward_curve({1},"{2}","{3}","","","days","",0 day ago)\n'.format(counter, symbol, column, curve_date_str)
        shows += '{0}: x{1}\n'.format(symbol, counter)
        whens += 'x{0} is DEFINED {1}\n'.format(counter, inc_or)

    return build_let_show_when_helper(lets, shows, whens)


def curve(symbols, column='Close', curve_dates=None):
    scall = symbols
    if isinstance(scall, str):
        scall = [scall]
    if isinstance(scall, dict):
        scall = list(scall.keys())

    if curve_dates is not None and isinstance(curve_dates, list) and len(curve_dates) > 1:
        q = build_curve_history_query(scall, column, curve_dates)
    else:
        q = build_curve_query(scall, column, curve_dates)
    res = query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    # only keep the current forward curve, discard history
    res = res['{}-{}'.format(pd.datetime.now().year, pd.datetime.now().month):]
    # reindex dates to start of month
    res = res.resample('MS').mean()
    return res


def build_continuous_futures_rollover_query(symbol, months=['M1'], rollover_date='5 days before expiration day', after_date=prevyear):
    lets, shows, whens = '', '', 'Date is after {}\n'.format(after_date)
    for month in months:
        m = int(month[1:])
        if m == 1:
            rollover_policy = 'actual prices'
        else:
            rollover_policy = '{} nearby actual prices'.format(m)
        lets += 'M{1} = {0}(ROLLOVER_DATE = "{2}",ROLLOVER_POLICY = "{3}")\n '.format(symbol, m, rollover_date, rollover_policy)
        shows += 'M{0}: M{0} \n '.format(m)

    return build_let_show_when_helper(lets, shows, whens)


def continuous_futures_rollover(symbol, months=['M1'], rollover_date='5 days before expiration day', after_date=prevyear):
    q = build_continuous_futures_rollover_query(symbol, months=months, rollover_date=rollover_date, after_date=after_date)
    res = query(q)
    return res


@lru_cache(maxsize=None)
def futures_contracts(symbol, start_year=curyear, end_year=curyear+2):
    contracts = get_symbol_contract_list(symbol, monthly_contracts_only=True)
    contracts = [x for x in contracts if start_year <= int(x.split('_')[-1][:4]) <= end_year]
    df = series(contracts)
    return df


@lru_cache(maxsize=None)
def get_symbol_contract_list(symbol, monthly_contracts_only=False):
    """
    Given a symbol pull all futurues contracts related to it
    :param symbol:
    :return:
    """
    uri = lim_schema_futurues_url.replace('<SYMBOL>', symbol)
    resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword), proxies=proxies)

    if resp.status_code == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        contracts = [x.attrib['name'] for x in root[0][0]]
        if monthly_contracts_only:
            contracts = [x for x in contracts if re.findall('\d\d\d\d\w', x) ]
        return contracts
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)
