"""Family planning data queries.

Variables
- DHIS2 Metadata
  These are commonly queried by uid (id), name, or code. Names are used here.
  - org_units (list): These are what 'geographies' are called in DHIS2 terms.
  - indicators (dict): Dict of indicators and their corresponding data element
    comprised formulae.
  - data_elements (list): List of data elements dynamically created as a
    function of those which comprise herein stipulated indicators.

Example URLs
- Example 01
endpoint + '.json\
?dimension=dx:N3FFtxkRDCy;YW8qksk9vDB;YHzlBaOHxUp;dcfWn8ZS2\
ie;MRM3kDTz5Oq;LBQ3CeEIrwx;WtRUiKMMeyZ;YJ6wwejMqpS;I4oqVMRU58z;di6qWEf5ylj;\
r8aBmbnY22K;JrfHpxf0fZB;xM9vMDt6xSq;dakp92oAECd;qJFiaE3DdKs;HBLgR9KYOt7;\
YYNfhajyjnz;RKGJ5hkRIIH;ROE4SGMZ1jM;z6zEiA849Fq;jx7NXfCFBfO;QSRvXP8v6T5\
;OOdh18PjbSx;rUGZUiy3R2C;Kr3A94oQ66i;fJ7CyT97iJC;MVIKFGNvNJp;xD7BTgHre20;\
CfpgknFZbyh;Wo4Hana1TLL;Js7Ia8ffGvX;n9kuCebQpqc\
&dimension=pe:LAST_5_YEARS\
&filter=ou:LEVEL-3;HfVjCurKxh2\
&displayProperty=NAME\
&outputIdScheme=UID'
"""
import os
import json
from glob import glob
from subprocess import Popen, PIPE
from datetime import datetime

import pandas as pd

from config import OUTPUT_DIR
from creds import DHIS2_KENYA_USERNAME, DHIS2_KENYA_PASSWORD


DATA_DIR = os.path.dirname(os.path.realpath(__file__)) + '/../source_data/'
ORG_UNITS_DATA = DATA_DIR + 'organisationUnits/all_level_2.csv'
INDICATORS_DATA = glob(DATA_DIR + 'indicators/*.json')
results = {
    'status': '',
    'headers': '',
    'content': {
        'text': '',
        'json': '',
        'list': []
    },
    'errors': [],
    'warnings': []
}
config = {
    'query_method': 'curl',
    'mode': 'list',
    'test_run': True,
    'logging': True,
    'debug': False
}
user, pwd = os.environ.get('DHIS2_KENYA_USERNAME', DHIS2_KENYA_USERNAME), \
            os.environ.get('DHIS2_KENYA_PASSWORD', DHIS2_KENYA_PASSWORD)
server = 'https://hiskenya.org'
endpoint = server + '/api/25/analytics'


def load_data():
    """Load data."""
    org_units_all = pd.read_csv(ORG_UNITS_DATA)
    org_unit_ids = org_units_all['id'].tolist()

    indicators_json = [json.load(open(j)) for j in INDICATORS_DATA]
    indicators_all = {}
    for bundle_of_groups in indicators_json:
        for group in bundle_of_groups['indicatorGroups']:  # dict w/ indicators
            for indicator in group['indicators']:
                indicators_all[indicator['name']] = {
                    k: v
                    for k, v in indicator.items() if k != 'name'
                }
                indicators_all[indicator['name']]['group'] = {
                    k: v
                    for k, v in group.items() if k != 'indicators'
                }
    indicator_ids = [v['id'] for k, v in indicators_all.items()]

    return org_unit_ids, indicator_ids, org_units_all, indicators_all


def log(output):
    """Log query results."""
    if config['logging']:
        print(output)


def save(output, return_file_path=False):
    """Save files."""
    _datetime = str(datetime.now()).replace(':', '-')
    output_file_name = 'family_planning_data_' + _datetime + '.csv'
    output_path = OUTPUT_DIR + output_file_name

    f = open(output_path, 'wb')
    f.write(output)
    f.close()

    if return_file_path:
        return output_path


def save_raw(output):
    """Save raw CSV."""
    file_path = save(output, return_file_path=True)
    return file_path


def format_csv(data, org_units, indicators):
    """Format csv."""
    raw_result = pd.read_csv(data)  # Make pandas df here?
    # raw_result = _input  # If doing test run on local file.
    raw_result.rename(columns={'Data': 'Indicator', 'Organisation unit':
        'Sub-County'}, inplace=True)
    # TODO: Add indicator name and sub-county name here.
    if org_units and indicators:
        pass
    result = raw_result

    print(result)

    return result


def run_curl(org_unit_id, indicator_ids):
    """Run query using curl sub-process.

    On Subprocess
    Good options to use are subprocess Popen() or call(). https://stackoverflow
      .com/questions/1996518/retrieving-the-output-of-subprocess-call

    Demo Example
    - Docs URL
    https://docs.dhis2.org/2.25/en/developer/html/dhis2_developer_manual_full
    .html#webapi_reading_data_values

    - Example Command
    curl "https://play.dhis2.org/demo/api/25/dataValueSets?dataSet=pBOMPrpg1QX
    &period=201401&orgUnit=DiszpKrYNg8" -H "Accept:application/xml" -u
    admin:district -v

    - Notes
    When using curl in the command line, you should have quotations around the
    positional url and the argument for -H. But in the list passed to Popen(),
    you shoudl not.
    """
    _format = 'csv'
    ind = ';'.join(indicator_ids)[:-1]
    period = 'LAST_5_YEARS'
    query_by = 'UID'
    display_property = 'NAME'

    url = endpoint + '.' + _format + '?dimension=dx:' + ind + \
          '&dimension=pe:' + period + '&dimension=ou:' + org_unit_id + \
          '&displayProperty=' + display_property + '&outputIdScheme=' + \
          query_by
    command = 'curl {} -H Accept:application/json -u {}:{} -v ' \
        .format(url, user, pwd)
    proc = \
        Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)

    raw_result, error = proc.communicate()
    results['errors'].append(error)
    # results['content']['list'].append(raw_result)

    return raw_result


def run(org_unit_ids, indicator_ids):
    """Run preferred means of query."""
    return {
        'curl': run_curl
    }[config['query_method']](org_unit_ids, indicator_ids)


if __name__ == '__main__':
    try:
        print('Querying API and saving files...')
        org_units, indicators, org_units_all_data, indicators_all_data = \
            load_data()
        i = 0
        a_tenth = len(org_units) / 10
        next_tenth = a_tenth
        for ou in org_units:
            result = run(ou, indicators)
            # result = format_csv(result)
            # log(result)
            save(result)
            if i > next_tenth:
                print(str(i / len(org_units) * 10)[:2] + '% complete.')
                next_tenth = next_tenth + a_tenth
            i += 1
        print('Complete.')
        # DEBUG
        # TEST_DATA = DATA_DIR + 'organisationUnits/test.csv'
        # test = pd.read_csv(TEST_DATA)
        # result = format_csv(test, org_units_all_data, indicators_all_data)

        # Single test
        # raw_result = run(org_units[0], indicators)
        # raw_file_path = save_raw(raw_result)
        # result = format_csv(raw_file_path, org_units_all_data,
        #                     indicators_all_data)

        # log(result)
        # save(result)


    except json.JSONDecodeError:
        if config['debug']:
            from pdb import set_trace
            set_trace()
        pass
