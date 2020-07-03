#!/usr/bin/env python3
from contextlib import contextmanager
from tempfile import TemporaryFile
from typing import Iterable

import requests
import tablib


COMMCARE_PROJECT_SPACE = 'my-project-space'
COMMCARE_USERNAME = 'user@example.com'
COMMCARE_PASSWORD = 's3cret'

COMMCARE_CASE_TYPE = 'facility'
COMMCARE_OWNER_ID = 'c0ffeeeeeeee464095baa96d7a621209'

COMMCARE_BASE_URL = 'https://www.commcarehq.org/'

DHIS2_BASE_URL = 'https://play.dhis2.org/dev/'
DHIS2_USERNAME = 'admin'
DHIS2_PASSWORD = 'district'

DHIS2_OPTIONSET_ID = 'tZ3Wxh1ED0A'  # Quarantine sites


def get_name(quarantine_site) -> str:
    """
    Returns a case name for a given quarantine site
    """
    return quarantine_site['name']


def get_dhis2_optionset_options() -> Iterable[dict]:
    """
    Returns an iterable of DHIS2 OptionSet Options.
    """

    params = {'paging': 'False'}
    headers = {'Accept': 'application/json'}
    auth = (DHIS2_USERNAME, DHIS2_PASSWORD)

    endpoint = f'/api/optionSets/{DHIS2_OPTIONSET_ID}/'
    url = prefix_base_url(DHIS2_BASE_URL, endpoint)
    response = requests.get(url, params, headers=headers, auth=auth)
    options = response.json()['options']
    for option in options:
        url = prefix_base_url(DHIS2_BASE_URL, f'/api/options/{option["id"]}/')
        response = requests.get(url, params, headers=headers, auth=auth)
        yield response.json()


def map_case_properties(optionset_options) -> Iterable[dict]:
    """
    Yields dictionaries with DHIS2 attributes mapped to case properties.
    """
    for option in optionset_options:
        case_properties = {
            'name': get_name(option),
            'external_id': option['id'],
            'owner_id': COMMCARE_OWNER_ID,
            'code': option['code'],
        }
        yield case_properties


@contextmanager
def save_cases(cases):
    """
    Saves cases to a temporary file. Returns the file object as a
    context object. Deletes the file after it has been used.
    """
    headers = [
        'name',
        'external_id',
        'owner_id',
        'code',
    ]
    data = tablib.Dataset(headers=headers)
    for case in cases:
        data.append([case[k] for k in headers])
    with TemporaryFile() as tempfile:
        excel_data = data.export('xlsx')
        tempfile.write(excel_data)
        tempfile.seek(0)
        yield tempfile


def bulk_upload_cases(tempfile):
    """
    Uploads case data stored in ``tempfile`` to CommCare HQ. Returns a
    status URL if upload succeeds. Raises an exception if upload fails.
    """
    endpoint = f'/a/{COMMCARE_PROJECT_SPACE}/importer/excel/bulk_upload_api/'
    url = prefix_base_url(COMMCARE_BASE_URL, endpoint)
    data = {
        'case_type': COMMCARE_CASE_TYPE,
        'search_field': 'external_id',
        'create_new_cases': 'on',
        'name_column': 'name',
        'comment': 'Imported from DHIS2 tracked entities',
    }
    files = {'file': (f'{COMMCARE_CASE_TYPE}_cases.xlsx', tempfile)}
    auth = (COMMCARE_USERNAME, COMMCARE_PASSWORD)
    response = requests.post(url, data, files=files, auth=auth)
    response.raise_for_status()
    return response.json()['status_url']


def prefix_base_url(base_url, endpoint):
    """
    Returns ``base_url`` + ``endpoint`` with the right forward slashes.

    >>> prefix_base_url('https://play.dhis2.org/dev/',
    ...                 '/api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    >>> prefix_base_url('https://play.dhis2.org/dev',
    ...                 'api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    """
    return '/'.join((base_url.rstrip('/'), endpoint.lstrip('/')))


if __name__ == '__main__':
    options = get_dhis2_optionset_options()
    cases = map_case_properties(options)
    with save_cases(cases) as tempfile:
        status_url = bulk_upload_cases(tempfile)
    print('Upload successful. Import in progress.')
    print(f'Poll {status_url} for progress updates')
