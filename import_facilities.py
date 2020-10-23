#!/usr/bin/env python3
# TODO: Configure the following constants:

# CommCare authentication details:
COMMCARE_PROJECT_SPACE = 'demo'
COMMCARE_USERNAME = 'user@example.com'
COMMCARE_API_KEY = 'abc123'

# The URL of the DHIS2 server and path to the API (excluding "api/"):
DHIS2_BASE_URL = 'https://play.dhis2.org/dev/'
DHIS2_USERNAME = 'admin'
DHIS2_PASSWORD = 'district'

# Map CommCare case properties to DHIS2 data element IDs
CASE_PROPERTY_MAP = {
    'live_births_last_month': {
        'id': 'gQNFkFkObU8',  # Live births
        'data_set': 'QX4ZTUbOt3a', # Reproductive Health
    },
}

# End of configuration


from contextlib import contextmanager
from datetime import date
from tempfile import TemporaryFile
from typing import Iterable

import requests
import tablib

COMMCARE_BASE_URL = 'https://www.commcarehq.org/'
COMMCARE_CASE_TYPE = 'facility'
COMMCARE_PAGE_SIZE = 20


def get_facility_cases() -> Iterable[dict]:
    endpoint = f'/a/{COMMCARE_PROJECT_SPACE}/api/v0.5/case/'
    url = prefix_base_url(COMMCARE_BASE_URL, endpoint)
    params = {
        'type': COMMCARE_CASE_TYPE,
        'closed': 'false',
        'limit': COMMCARE_PAGE_SIZE,
        'offset': 0,
        'format': 'json',
    }
    headers = {'Authorization': f'ApiKey {COMMCARE_USERNAME}:{COMMCARE_API_KEY}'}
    while True:
        response = requests.get(url, params, headers=headers)
        cases = response.json()['objects']
        for case in cases:
            facility = {
                'case_id': case['case_id'],
                'external_id': case['properties']['external_id'],
                'name': case['properties']['case_name'],
            }
            yield facility
        if len(cases) < COMMCARE_PAGE_SIZE:
            return
        params['offset'] += COMMCARE_PAGE_SIZE


def set_case_properties(facilities) -> Iterable[dict]:
    """
    Fetch all the data elements for the data set, because we don't know
    in advance what category option combos to query for
    """
    # e.g. https://play.dhis2.org/dev/api/dataValueSets?orgUnit=jNb63DIHuwU
    #   &period=202010
    #   &dataSet=QX4ZTUbOt3a  <-- The data set of the data element
    # not https://play.dhis2.org/dev/api/dataValues?ou=jNb63DIHuwU
    #   &pe=202010
    #   &de=gQNFkFkObU8  <-- The data element we want
    #   &co=L4P9VSgHkF6  <-- We don't know this, and without it we get a 409

    endpoint = '/api/dataValueSets'
    url = prefix_base_url(DHIS2_BASE_URL, endpoint)
    params = {
        'period': get_last_month(),
    }
    headers = {'Accept': 'application/json'}
    auth = (DHIS2_USERNAME, DHIS2_PASSWORD)
    for facility in facilities:
        params['orgUnit'] = facility['external_id']
        for case_property, data_element in CASE_PROPERTY_MAP.items():
            params['dataSet'] = data_element['data_set']
            response = requests.get(url, params, headers=headers, auth=auth)
            value = 0
            for data_value in response.json()['dataValues']:
                if data_value['dataElement'] == data_element['id']:
                    value += int(data_value['value'])
            facility[case_property] = value
        yield facility


def get_last_month() -> str:
    return date.today().strftime('%Y%m')


@contextmanager
def save_cases(cases):
    """
    Saves cases to a temporary file. Returns the file object as a
    context object. Deletes the file after it has been used.
    """
    headers = [
        'case_id',
        'external_id',
        'name',
        *CASE_PROPERTY_MAP,
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
    headers = {'Authorization': f'ApiKey {COMMCARE_USERNAME}:{COMMCARE_API_KEY}'}
    response = requests.post(url, data, files=files, headers=headers)
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
    cases = get_facility_cases()
    cases = set_case_properties(cases)
    with save_cases(cases) as tempfile:
        status_url = bulk_upload_cases(tempfile)
    print('Upload successful. Import in progress.')
    print(f'Poll {status_url} for progress updates')
