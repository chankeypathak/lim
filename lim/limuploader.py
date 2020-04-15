from datetime import datetime
from lxml import etree
import lxml.builder
import time
import requests
import logging
import pandas as pd
from lim import lim


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
    """
    Converts a dataframe (column headings being the treepath) into an XML that the uploader takes
    :param df:
    :param dfmeta:
    :return:
    """
    E = lxml.builder.ElementMaker()
    ROOT = E.ExcelData
    ROWS = E.Rows
    xROW = E.Row
    xCOL = E.Col
    xCOLS = E.Cols

    entries = []
    count = 1
    for irow, row in df.iterrows():
        for col, val in row.iteritems():
            if pd.isna(val):
                continue
            tokens = col.split(';')
            treepath = tokens[0]
            column = default_column if len(tokens) == 1 else tokens[1]
            desc = dfmeta.get('description', '')
            if isinstance(irow, pd.Timestamp):
                irow = irow.date()
            erow = xROW(
                xCOLS(
                    xCOL(treepath, num="1"),
                    xCOL(column, num="2"),
                    xCOL(str((irow - datetime(1899, 12, 30).date()).days), num="3"), # excel dateformat
                    xCOL(str(val), num="4"),
                    xCOL(desc, num="5"),
                ),
                num=str(count)
            )
            count = count + 1
            entries.append(erow)

    irow = ROOT()
    xROWS = ROWS()
    [xROWS.append(x) for x in entries]
    irow.append(xROWS)

    res = (lxml.etree.tostring(irow, pretty_print=True))
    return res


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def upload_chunk(df, dfmeta):
    url = '{}&parsername=DefaultParser'.format(lim_upload_default_parser_url)
    res = build_upload_xml(df, dfmeta)
    logging.info('Uploading df below to {}:\n'.format(url, df))
    resp = requests.request("POST", url, headers=headers, data=res, auth=(lim.limUserName, lim.limPassword),
                            proxies=lim.proxies)

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


def upload_series(df, dfmeta):
    for chunk in chunks(df, 100):
        upload_chunk(chunk, dfmeta)

