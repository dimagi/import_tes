#!/usr/bin/env python3
"""
Example script to demonstrate bulk-importing DHIS2 tracked entities as
CommCare cases.

Usage:
1. Customize constants at the start of the script.
2. Address "TODO" comments in the code.
3. Run::

        $ python3 import_tes.py

"""
from contextlib import contextmanager
from tempfile import TemporaryFile
from typing import Iterable

import requests
import tablib

DOMAIN = 'my-project-space'
COMMCARE_USERNAME = 'user@example.com'
COMMCARE_PASSWORD = 's3cret'

CASE_TYPE = 'person'
CASE_PROPERTY_MAP = {
    'trackedEntityInstance': 'external_id',
    'orgUnit': 'owner_id',
    'attributes': {
        'zDhUuAYrxNC': 'last_name',
        'w75KJ2mc4zz': 'first_name',
    }
}
ORG_UNIT_MAP = {
    'O6uvpzGd5pu': 'c0ffeeee-b450-4f9c-9fdb-c8f855f0c531',  # Bo
    # Bombali
    # Bonthe
    # Kailahun
    # Kambia
    # Kenema
    # Koinadugu
    # Kono
    # Moyamba
    # Port Loko
    # Pujehun
    # Tonkolili
    # Western Area
}
BASE_URL = 'https://www.commcarehq.org/'


def get_tracked_entities_from_dhis2() -> Iterable[dict]:
    """
    Returns an iterable of dictionaries, as returned by the DHIS2 web
    API. (Can be a list or a generator. A generator could be useful if
    the function handles pagination.)

    A tracked entity looks a bit like this. (Some properties have been
    omitted.) ::

        {
            'orgUnit': 'ImspTQPwCqd',
            'trackedEntityInstance': 'iHhCKKXHQv6',
            'trackedEntityType': 'nEenWmSyUEp',
            'attributes': [
                {
                    'displayName': 'Last name',
                    'attribute': 'zDhUuAYrxNC',
                    'value': 'May'
                },
                {
                    'displayName': 'First name',
                    'attribute': 'w75KJ2mc4zz',
                    'value': 'Isabel'
                }
            ]
        }

    """
    raise NotImplementedError
    # TODO: Left as an exercise for the reader
    # For example:
    # url = 'https://play.dhis2.org/dev/api/trackedEntityInstances'
    # params = {
    #     'ou': 'ImspTQPwCqd',  # Sierra Leone
    #     'trackedEntityType': 'nEenWmSyUEp'  # Person
    # }
    # headers = {'Accept': 'application/json'}
    # auth = ('admin', 'district')
    # response = requests.get(url, params, headers=headers, auth=auth)
    # return response.json()['trackedEntityInstances']


def map_tracked_entity_attributes(tracked_entities) -> Iterable[dict]:
    """
    Takes an iterable of tracked entities, and returns an interable of
    dictionaries with tracked entity attributes mapped to case property
    values.

    e.g. ::

        >>> generator = map_tracked_entity_attributes([{
        ...     'trackedEntityInstance': 'iHhCKKXHQv6',
        ...     'orgUnit': 'O6uvpzGd5pu',
        ...     'attributes': [
        ...         {'attribute': 'zDhUuAYrxNC', 'value': 'May'},
        ...         {'attribute': 'w75KJ2mc4zz', 'value': 'Isabel'}
        ...     ]
        ... }])
        >>> list(generator) == [{
        ...     'name': 'MAY, Isabel',
        ...     'external_id': 'iHhCKKXHQv6',
        ...     'owner_id': 'c0ffeeee-b450-4f9c-9fdb-c8f855f0c531',
        ...     'last_name': 'May',
        ...     'first_name': 'Isabel',
        ... }]
        True

    """
    for tracked_entity in tracked_entities:
        dhis2_id_property = CASE_PROPERTY_MAP['trackedEntityInstance']
        org_unit_property = CASE_PROPERTY_MAP['orgUnit']

        org_unit = tracked_entity['orgUnit']
        # Raises KeyError if org unit not found.
        # TODO: Change to correct behaviour (skip? log error? raise exception?)
        # Left as an exercise to the reader
        commcare_location = ORG_UNIT_MAP[org_unit]

        case_properties = {
            'name': get_name(tracked_entity),
            dhis2_id_property: tracked_entity['trackedEntityInstance'],
            org_unit_property: commcare_location,
        }
        for attr in tracked_entity['attributes']:
            if attr['attribute'] in CASE_PROPERTY_MAP['attributes']:
                dhis2_id = attr['attribute']
                property_name = CASE_PROPERTY_MAP['attributes'][dhis2_id]
                case_properties[property_name] = attr['value']
        yield case_properties


def get_name(tracked_entity) -> str:
    """
    Returns a name for a given ``tracked_entity``

    e.g. ::

        >>> tracked_entity = {
        ...     'trackedEntityInstance': 'iHhCKKXHQv6',
        ...     'orgUnit': 'O6uvpzGd5pu',
        ...     'attributes': [
        ...         {'attribute': 'zDhUuAYrxNC', 'value': 'May'},
        ...         {'attribute': 'w75KJ2mc4zz', 'value': 'Isabel'}
        ...     ]
        ... }
        >>> get_name(tracked_entity)
        'MAY, Isabel'

    """
    # TODO: Customize as required
    attrs = tracked_entity['attributes']
    family_names = [a['value'] for a in attrs if a['attribute'] == 'zDhUuAYrxNC']
    given_names = [a['value'] for a in attrs if a['attribute'] == 'w75KJ2mc4zz']
    if family_names and given_names:
        return f'{family_names[0].upper()}, {given_names[0]}'
    elif family_names:
        return family_names[0].upper()
    elif given_names:
        return given_names[0]


@contextmanager
def save_cases(cases):
    """
    Saves cases to a temporary file. Returns the file object as a
    context object. Deletes the file after it has been used.
    """
    headers = [
        'name',
        *(v for k, v in CASE_PROPERTY_MAP.items() if k != 'attributes'),
        *(v for v in CASE_PROPERTY_MAP['attributes'].values()),
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
    endpoint = f'/a/{DOMAIN}/importer/excel/bulk_upload_api/'
    url = prefix_base_url(endpoint)
    data = {
        'case_type': CASE_TYPE,
        'search_field': 'external_id',
        'create_new_cases': 'on',
        'name_column': 'name',
        'comment': 'Imported from DHIS2 tracked entities',
    }
    files = {'file': (f'{CASE_TYPE}_cases.xlsx', tempfile)}
    auth = (COMMCARE_USERNAME, COMMCARE_PASSWORD)
    response = requests.post(url, data, files=files, auth=auth)
    response.raise_for_status()
    return response.json()['status_url']


def prefix_base_url(endpoint):
    """
    Returns ``BASE_URL`` + ``endpoint`` with the right forward slashes.

    >>> BASE_URL = 'https://play.dhis2.org/dev/'
    >>> prefix_base_url('/api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    >>> BASE_URL = 'https://play.dhis2.org/dev'
    >>> prefix_base_url('api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    """
    return '/'.join((BASE_URL.rstrip('/'), endpoint.lstrip('/')))


if __name__ == '__main__':
    tracked_entities = get_tracked_entities_from_dhis2()
    cases = map_tracked_entity_attributes(tracked_entities)
    with save_cases(cases) as tempfile:
        status_url = bulk_upload_cases(tempfile)
    print('Upload successful. Import in progress.')
    print(f'Poll {status_url} for progress updates')
