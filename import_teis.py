#!/usr/bin/env python3
"""
Example script to demonstrate bulk-importing DHIS2 tracked entities as
CommCare cases.

Usage:
1. Customize constants at the start of the script.
2. Customize the ``get_name()`` function, if necessary.
3. Run::

        $ python3 import_teis.py

"""
from contextlib import contextmanager
from tempfile import TemporaryFile
from typing import Iterable

import requests
import tablib


# TODO: Configure the following constants:

# CommCare authentication details:
COMMCARE_PROJECT_SPACE = 'my-project-space'
COMMCARE_USERNAME = 'user@example.com'
COMMCARE_PASSWORD = 's3cret'

# The CommCare case type to be imported:
COMMCARE_CASE_TYPE = 'person'

# Map DHIS2 tracked entity attributes and JSON properties to CommCare
# case properties.
CASE_PROPERTY_MAP = {
    # DHIS2 TEI property (as given in JSON): CommCare case property
    'trackedEntityInstance': 'external_id',
    'orgUnit': 'owner_id',
    'attributes': {
        'zDhUuAYrxNC': 'last_name',
        'w75KJ2mc4zz': 'first_name',
    }
}

# Map DHIS2 organisation units to CommCare locations.
ORG_UNIT_MAP = {
    # DHIS2 OrgUnit ID: CommCare location ID
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

# If a TEI has an organisation unit that is not mapped to a CommCare
# location, should an error be raised? If set to `False`, the TEI will
# be skipped.
RAISE_ERROR_ON_MISSING_ORG_UNIT = True

# The URL of the DHIS2 server and path to the API (excluding "api/"):
DHIS2_BASE_URL = 'https://play.dhis2.org/dev/'
DHIS2_USERNAME = 'admin'
DHIS2_PASSWORD = 'district'

# API request parameters used for filtering the TEIs to be imported:
TEI_REQUEST_PARAMS = {
    'ou': 'ImspTQPwCqd',  # Sierra Leone
    'trackedEntityType': 'nEenWmSyUEp',  # Person
    # 'program': 'uy2gU8kT1jF',  # MNCH / PNC (Adult Woman)
}

# Optional constants:
COMMCARE_BASE_URL = 'https://www.commcarehq.org/'
DHIS2_PAGE_SIZE = 50


# TODO: Confirm `get_name()` uses the right tracked entity attributes
#       and returns the CommCare case name in the right format:
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
    attrs = tracked_entity['attributes']
    family_names = [a['value'] for a in attrs if a['attribute'] == 'zDhUuAYrxNC']
    given_names = [a['value'] for a in attrs if a['attribute'] == 'w75KJ2mc4zz']
    if family_names and given_names:
        return f'{family_names[0].upper()}, {given_names[0]}'
    elif family_names:
        return family_names[0].upper()
    elif given_names:
        return given_names[0]

# End of configuration


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
    endpoint = '/api/trackedEntityInstances'
    url = prefix_base_url(DHIS2_BASE_URL, endpoint)
    params = {
        **TEI_REQUEST_PARAMS,
        'paging': 'True',
        'pageSize': DHIS2_PAGE_SIZE,
        'page': 1,
    }
    headers = {'Accept': 'application/json'}
    auth = (DHIS2_USERNAME, DHIS2_PASSWORD)
    while True:
        response = requests.get(url, params, headers=headers, auth=auth)
        teis = response.json()['trackedEntityInstances']
        for tei in teis:
            yield tei
        if len(teis) < DHIS2_PAGE_SIZE:
            # The "trackedEntityInstances" endpoint does not give us
            # paging data like some other endpoints. So if we didn't get
            # a full page of results, we know we're on the last page.
            return
        params['page'] += 1


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

        name = get_name(tracked_entity)
        tei_id = tracked_entity['trackedEntityInstance']
        org_unit = tracked_entity['orgUnit']
        if org_unit not in ORG_UNIT_MAP:
            if RAISE_ERROR_ON_MISSING_ORG_UNIT:
                raise KeyError(
                    'CommCare location cannot be determined for organisation '
                    f'unit {org_unit!r}. Please add the organisation unit to '
                    '`ORG_UNIT_MAP`, or set `RAISE_ERROR_ON_MISSING_ORG_UNIT '
                    '= False`.'
                )
            else:
                print(f'Skipping TEI {tei_id!r} ({name}): Unknown org unit '
                      f'{org_unit!r}')
                continue
        commcare_location = ORG_UNIT_MAP[org_unit]

        case_properties = {
            'name': name,
            dhis2_id_property: tei_id,
            org_unit_property: commcare_location,
        }
        for attr in tracked_entity['attributes']:
            if attr['attribute'] in CASE_PROPERTY_MAP['attributes']:
                dhis2_id = attr['attribute']
                property_name = CASE_PROPERTY_MAP['attributes'][dhis2_id]
                case_properties[property_name] = attr['value']
        yield case_properties


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
    Returns ``BASE_URL`` + ``endpoint`` with the right forward slashes.

    >>> BASE_URL = 'https://play.dhis2.org/dev/'
    >>> prefix_base_url('/api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    >>> BASE_URL = 'https://play.dhis2.org/dev'
    >>> prefix_base_url('api/trackedEntityInstances')
    'https://play.dhis2.org/dev/api/trackedEntityInstances'

    """
    return '/'.join((base_url.rstrip('/'), endpoint.lstrip('/')))


if __name__ == '__main__':
    tracked_entities = get_tracked_entities_from_dhis2()
    cases = map_tracked_entity_attributes(tracked_entities)
    with save_cases(cases) as tempfile:
        status_url = bulk_upload_cases(tempfile)
    print('Upload successful. Import in progress.')
    print(f'Poll {status_url} for progress updates')
