import pandas as pd
import os
import time
from lxml import etree
import requests
from functools import lru_cache
import logging

limServer = os.environ['LIMSERVER'].replace('"', '')
limUserName = os.environ['LIMUSERNAME'].replace('"', '')
limPassword = os.environ['LIMPASSWORD'].replace('"', '')

calltries = 50
sleep = 2.5

headers = {
    'Content-Type': 'application/xml',
}

lim_datarequests_url = '{}/rs/api/datarequests'.format(limServer)


def alternate_col_val(values, noCols):
    for x in range(0, len(values), noCols):
        yield values[x:x + noCols]


def build_dataframe(reports):
    columns = [x.text for x in reports.iter(tag='ColumnHeadings')]
    dates = [x.text for x in reports.iter(tag='RowDates')]
    values = [float(x.text) for x in reports.iter(tag='Values')]
    values = list(alternate_col_val(values, len(columns)))

    df = pd.DataFrame(values, columns=columns, index=pd.to_datetime(dates))
    return df


def call_lim_api_query(q, id=None, tries=calltries):
    r = '<DataRequest><Query><Text>{}</Text></Query></DataRequest>'.format(q)

    if tries == 0:
        raise Exception('Run out of tries')

    if id is None:
        resp = requests.request("POST", lim_datarequests_url, headers=headers, data=r, auth=(limUserName, limPassword))
    else:
        uri = '{}/{}'.format(lim_datarequests_url, id)
        resp = requests.get(uri, headers=headers, auth=(limUserName, limPassword))
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
            return call_lim_api_query(q, reqId, tries - 1)
        else:
            raise Exception(root.attrib['statusMsg'])
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp, resp.text))
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
    res = call_lim_api_query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    return res


def build_curve_helper(lets, shows, whens):
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
        curve_date_str, curve_date_str_nor = curve_date.strftime("%m/%d/%Y"), curve_date.strftime("%d/%m/%Y")

        inc_or = ''
        if len(curve_dates) > 1 and counter != len(curve_dates):
            inc_or = 'OR'
        lets += 'ATTR x{0} = forward_curve({1},"{2}","{3}","","","days","",0 day ago)\n'.format(counter, symbols[0], column, curve_date_str)
        shows += '{0}: x{1}\n'.format(curve_date_str_nor, counter)
        whens += 'x{0} is DEFINED {1}\n'.format(counter, inc_or)
    return build_curve_helper(lets, shows, whens)


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

    return build_curve_helper(lets, shows, whens)


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
    res = call_lim_api_query(q)

    if isinstance(symbols, dict):
        res = res.rename(columns=symbols)

    # only keep the current forward curve, discard history
    res = res['{}-{}'.format(pd.datetime.now().year, pd.datetime.now().month):]
    return res