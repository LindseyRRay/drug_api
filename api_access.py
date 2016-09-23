import funcy
import multiprocessing
import requests
from requests.auth import HTTPDigestAuth
from xml.etree import ElementTree

URL = 'https://origin-edc-lsapi.thomson-pharma.com/ls-api-ws/ws/rs/auth-v2'
TRIAL_ID_URL = 'https://lsapi.thomson-pharma.com/ls-api-ws/ws/rs/trials-v2/trial/search/?query=trialDateChangeLast:RANGE(%3E1900-09-16)&hits=1000&queryLanguage=ssql'
# TRIAL_ID_URL = 'https://lsapi.thomson-pharma.com/ls-api-ws/ws/rs/trials-v2/trial/search'


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
        print tscrial_ids
        print e
        return None


def get_trial_ids(url, offset=None):
    if not offset:
        offset=0
    res = requests.get(
        url, auth=HTTPDigestAuth('MassInstech_001', 'FO72K93VSX5PI4BG'), params={
        'query': 'trialDateChangeLast:RANGE(%3E1900-09-16)',
        'hits': 500,
        'queryLanguage': 'ssql',
        'offset': offset,
    })
    if res.status_code != requests.codes.ok:
        print res.url
        print res.status_code
        print res.content
        print offset
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
    pool = multiprocessing.Pool(processes=5)
    results = pool.map(paginate_trial_ids, xrange(0, 250000, 50000))
    print len(results)
    return results
