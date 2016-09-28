import funcy
import time
import multiprocessing
import requests
from requests.auth import HTTPDigestAuth
import numpy as np
import pandas as pd
from xml.etree import ElementTree
import os

from dev import (TRIAL_ID_URL, TRIAL_INFO_URL, AUTH_USER, AUTH_PASS,
                    TRIAL_XML_DIR, DRUG_XML_DIR, STORAGE_DIR, DRUG_IDS_FNAME, TRIAL_IDS_FNAME, DRUG_INFO_URL)


def parse_xml_for_trial_id(res_content):
    tree = ElementTree.fromstring(res_content)
    # first first child that is search results and get all child nodes that are trials
    trial_results = tree.find('SearchResults').findall('Trial')
    # trial results is a list of child nodes
    # the trial id is found in the .items() of each child note
    try:
        trial_ids = funcy.merge_with(list, *map(lambda x: dict(x.items()), trial_results))
        # flatten list of trial ids and return
        return trial_ids['Id']
    except KeyError as e:
        print trial_ids, e
        return None


def get_trial_ids(url, offset=None):
    if not offset:
        offset = 0
    res = requests.get(
        url, auth=HTTPDigestAuth( AUTH_USER, AUTH_PASS), params={
        'query': 'trialDateChangeLast:RANGE(%3E1900-09-16)',
        'hits': 500,
        'queryLanguage': 'ssql',
        'offset': offset,
    })
    if res.status_code != requests.codes.ok:
        print res.url
        print res.status_code
        return (res.status_code, offset, None)
    return (res.status_code, offset, parse_xml_for_trial_id(res.content))


def paginate_trial_ids(start):
    # each function call gets 50,000 ids
    res = []
    for x in xrange(100):
        print x
        loop_start = start + x*500
        loop_res = get_trial_ids(TRIAL_ID_URL, loop_start)
        res.append(loop_res)
    return res


# for all results, get trial ids
def batch_process_requests():
    # set up pool of processes
    pool = multiprocessing.Pool(processes=10)
    results = pool.map(paginate_trial_ids, xrange(0, 250000, 25000))
    return results


def compile_ids_output(request_res, filename=None, directory=None):
    # collect id values and output to csv
    ids = []
    for process_res in request_res:
        for (code, offset, res) in process_res:
            if code == requests.code.ok:
                ids.append(res)
    ser = pd.Series(np.array(funcy.flatten(ids)), name='TrialId')
    path = 'TrialIds.csv' or filename
    if directory:
        path = os.path.join(directory, path)
    ser.to_csv(path, index=False)


def store_xml(trial_id, res_content, storage_dir):
    # parse and store trial xml
    # save content as xml tree
    tree = ElementTree.fromstring(res_content)
    # convert to a tree
    t2 = ElementTree.ElementTree(tree)
    # write to .xml
    with open(os.path.join(storage_dir, '{}.xml'.format(trial_id)), 'w') as f:
        t2.write(f)


def search_trial_id(trial_id):
    # get trial information for each trial id
    params = {'idList': trial_id}
    print trial_id
    res = requests.get(TRIAL_INFO_URL, auth=HTTPDigestAuth(AUTH_USER, AUTH_PASS), params=params)
    print res.status_code
    if res.status_code != requests.codes.ok:
        print res.status_code
        return (res.status_code, trial_id, None)
    # get xml info and save
    store_xml(trial_id, res.content, TRIAL_XML_DIR)
    return (res.status_code, trial_id)


def read_ids(filename):
    ids = pd.read_csv(os.path.join(STORAGE_DIR, filename))
    ids_arr = ids.iloc[:, 0].values
    print len(ids_arr)
    assert isinstance(ids_arr[0], int)
    return ids_arr


def read_trial_ids(filename=None):
    # read a previously downloads csv of trial ids
    if not filename:
        filename = TRIAL_IDS_FNAME
    return read_ids(filename)


def read_drug_ids(filename=None):
    # read a previously downloads csv of trial ids
    if not filename:
        filename = DRUG_IDS_FNAME
    return read_ids(filename)


def batch_process_xml_download(download_fnc, ids):
    # set up multiprocess pool
    # create pool of processes
    start = time.time()
    pool = multiprocessing.Pool(processes=4)
    # using imap unorded because I don't care about order and don't want to take memory hit
    # putting list of all ids in memory
    for x in pool.imap_unordered(download_fnc, ids, chunksize=50):
        print("{} (Time elapsed: {}s)".format(x, int(time.time() - start)))


def check_for_existing_downloads(download_dir, total_ids):
    existing_ids = [f.split('.xml')[0] for f in os.listdir(download_dir) if f.endswith('.xml')]
    existing_ints = map(int, existing_ids)
    to_download = list(set(total_ids).difference(set(existing_ints)))
    print 'Need to download {} more ids'.format(len(to_download))
    return to_download


def download_trial_records(trial_ids_file=None):
    trial_ids = read_trial_ids(trial_ids_file)
    # check for already downloaded trials
    to_download = check_for_existing_downloads(TRIAL_XML_DIR, trial_ids)
    print type(to_download[0])
    print to_download
    if len(to_download) < 1000:
        res = map(search_trial_id, to_download)
        return res
    else:
    # return to_download
        return batch_process_xml_download(search_trial_id, to_download)


def search_drug_id(drug_id):
    params = {'includeSources': 1}
    print drug_id
    res = requests.get(
        DRUG_INFO_URL.format(drugId=drug_id), auth=HTTPDigestAuth(AUTH_USER, AUTH_PASS), params=params)
    print res.status_code
    if res.status_code != requests.codes.ok:
        print res.status_code
        return (res.status_code, drug_id, None)
    # get xml info and save
    store_xml(drug_id, res.content, DRUG_XML_DIR)
    return (res.status_code, drug_id)


def download_drug_records(drug_ids_file=None):
    drug_ids = read_drug_ids(drug_ids_file)
    # check if any existing drugs have already been downloaded
    to_download = check_for_existing_downloads(DRUG_XML_DIR, drug_ids)
    print type(to_download[0])
    print to_download
    if len(to_download) < 1000:
        res = map(search_drug_id, to_download)
        return res
    else:
        # return to_download
        return batch_process_xml_download(search_drug_id, to_download)


