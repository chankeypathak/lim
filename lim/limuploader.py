import pandas as pd
import os
from datetime import datetime
import time
from lxml import etree
import requests
from functools import lru_cache
import logging
from lim import lim
import lxml.etree
import lxml.builder


lim_upload_default_parser_url = '{}/rs/upload?username={}'.format(lim.limServer, lim.limUserName)
lim_upload_status_url = '{}/rs/upload/jobreport/'.format(lim.limServer)


headers = {
    'Content-Type': 'text/xml',
}

default_column = 'TopColumn:Price:Close'


def check_upload_status(jobid):
    url = '{}{}'.format(lim_upload_status_url, jobid)
    resp = requests.get(url, headers=lim.headers, auth=(lim.limUserName, lim.limPassword), proxies=lim.proxies)

    if resp.status_code == 200:

        root = etree.fromstring(resp.text.encode('utf-8'))
        status_el = root.find('status')
        if status_el is not None:
            code, msg = '', ''
            code_el = status_el.find('code')
            if code_el is not None:
                code = code_el.text
            message_el = status_el.find('message')
            if message_el is not None:
                msg = message_el.text
            if code != '300':
                logging.warning('Problem with upload job {}: {}'.format(jobid, msg))
            return code, msg
    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)


def build_upload_xml(df, dfmeta):
    E = lxml.builder.ElementMaker()
    ROOT = E.ExcelData
    ROWS = E.Rows
    xROW = E.Row
    xCOL = E.Col
    xCOLS = E.Cols

    entries = []
    count = 1
    for x, y in df.iterrows():
        tokens = y.index[0].split(';')
        treepath = tokens[0]
        column = default_column if len(tokens) == 1 else tokens[1]
        desc = dfmeta.get('description', '')
        erow = xROW(
            xCOLS(
                xCOL(treepath, num="1"),
                xCOL(column, num="2"),
                xCOL(str((x - datetime(1899, 12, 30).date()).days), num="3"), # excel dateformat
                xCOL(str(y[0]), num="4"),
                xCOL(desc, num="5"),
            ),
            num=str(count)
        )
        count = count + 1
        entries.append(erow)

    x = ROOT()
    xROWS = ROWS()
    [xROWS.append(x) for x in entries]
    x.append(xROWS)

    res = (lxml.etree.tostring(x, pretty_print=True))
    return res


def upload_series(df, dfmeta):
    url = '{}&parsername=DefaultParser'.format(lim_upload_default_parser_url)
    res = build_upload_xml(df, dfmeta)
    resp = requests.request("POST", url, headers=headers, data=res, auth=(lim.limUserName, lim.limPassword), proxies=lim.proxies)

    status = resp.status_code
    if status == 200:
        root = etree.fromstring(resp.text.encode('utf-8'))
        intStatus = root.attrib['intStatus']
        if intStatus == '202':
            jobid = root.attrib['jobID']
            for i in range(0, lim.calltries):
                code, msg = check_upload_status(jobid)
                if code in ['300', '302']:
                    return msg
                else:
                    logging.warning('Problem with upload job {}: {}'.format(jobid, msg))

                time.sleep(lim.sleep)

    else:
        logging.error('Received response: Code: {} Msg: {}'.format(resp.status_code, resp.text))
        raise Exception(resp.text)
