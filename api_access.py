import funcy
import multiprocessing
import requests
from requests.auth import HTTPDigestAuth
import numpy as np
import pandas as pd
from xml.etree import ElementTree
import os

from dev import TRIAL_ID_URL, TRIAL_INFO_URL, AUTH_USER, AUTH_PASS, TRIAL_XML_DIR, STORAGE_DIR


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


def store_xml(trial_id, res_content, DIR=TRIAL_XML_DIR):
    # parse and store trial xml
    # save content as xml tree
    tree = ElementTree.fromstring(res_content)
    # convert to a tree
    t2 = ElementTree.ElementTree(tree)
    # write to .xml
    with open(os.path.join(DIR, '{}.xml'.format(trial_id)), 'w') as f:
        t2.write(f)


def search_trial_id(trial_id):
    # get trial information for each trial id
    params = {'idList': trial_id}
    res = requests.get(TRIAL_INFO_URL, auth=HTTPDigestAuth(AUTH_USER, AUTH_PASS), params=params)
    if res.status_code != requests.codes.ok:
        print res.status_code
        return (res.status_code, trial_id, None)
    # get xml info and save
    return (res.status_code, trial_id, store_xml(trial_id, res.content))


def get_trial_info(trial_ids):
    # map the search trial id function to a list of ids
    res = map(search_trial_id, trial_ids)


def read_trial_ids(filename=None):
    # read a previously downloads csv of trial ids
    if not filename:
        filename = 'TrialIds.csv'
    trial_ids = pd.read_csv(os.path.join(TRIAL_XML_DIR, filename))
    trial_ids_arr = trial_ids.TrialId.values
    print len(trial_ids_arr)
    return trial_ids_arr


def batch_process_xml_download(trial_ids):
    # set up multiprocess pool
    # create pool of processes
    pool = multiprocessing.Pool(processes=10)
    chunk_size = int(len(trial_ids)/10)+1
    print chunk_size
    results = pool.map(get_trial_info, funcy.chunks(chunk_size, trial_ids))
    print len(results)
    return results


def read_in_trials_filter(trial_ids_file, trial_xml_dir):
    trial_ids = pd.read_csv(os.path.join(STORAGE_DIR, 'TrialIds.csv'))
    # check for already downloaded trials
    existing_trial_ids = [f.split('.xml')[0] for f in os.listdir(TRIAL_XML_DIR) if f.endswith('.xml')]
    existing_trial_ints = map(int, existing_trial_ids)
    to_download = set(trial_ids.values).difference(set(existing_trial_ids))
    print 'Need to download {} more trial ids'.format(len(to_download))



