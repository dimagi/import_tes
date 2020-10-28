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
    'dhis_hmis_data_discussed': {
        'id': 'krUiCzsUAZr',  # HMIS Management meetings conducted
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_admissions_in_quarter': {
        'id': 'yCJBWuhuOLc',  # HMIS Total # of Admissions (including Maternity)
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_suspected_malaria_cases_under_5yrs': {
        'id': 'ttpMSWCCq6s',  # HMIS17 Malaria Under 5 years  Admissions
        'data_set': 'uk3Vkwy5cIe'
    },
    'dhis_total_budget_for_drugs': {
        'id': 'P0YGSCM0OKb',  # HMIS Cumulative drug budget
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_expenditure_in_quarter': {
        'id': 'M2Pkr4zxryN',  # HMIS17 Cumulative actual expenditure in all programmes
        'data_set': 'uk3Vkwy5cIe'
    },
    'dhis_health_facilities_under_hospital_management_supervision': {
        'id': 'M3HHFh3RDNP',  # ENVT EH # Of Health Facilities In The  District
        'data_set': 'iTo9FkAJTSl'
    },
    'dhis_quarterly_estimated_pregnant_women_in_area': {
        'id': 'GxqYLY3iWcz',  # CHD EPI Pregnant women
        'data_set': 'XcgwcDqqE17'
    },
    'dhis_ambulances_functional': {
        'id': 'MRqq82xATzI',  # HMIS # of Functioning Ambulances
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_what_is_the_total_number_of_beds_at_the_facility': {
        'id': 'asCqjKclllu',  # HMIS Bed Capacity
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_quarterly_estimated_children_under_5_in_area': {
        'id': 'PVYgza4lLfj',  # CMED Under 5 Population
        'data_set': 'rkyO2EAX45C'
    },
    'dhis_prev_new_smear_positive_cases_cured': {
        'id': 'gKghGP99qDe',  # TBTO New Smear Positive Cured
        'data_set': 'VEqRXwmqhM1'
    },
    'dhis_prev_new_smear_positive_cases_dead': {
        'id': 'yssWGMYDkdA',  # TBTO New Smear Positive Died
        'data_set': 'VEqRXwmqhM1'
    },
    'dhis_prev_new_smear_positive_cases_treatment_failure': {
        'id': 'dmyxAlzfCiK',  # TBTO New Smear Positive Failure
        'data_set': 'VEqRXwmqhM1'
    },
    'dhis_prev_new_eptb_cases_cured': {
        'id': 'a8nx11YujJz',  # TB New Treatment outcome New EPTB Cured
        'data_set': 'fOiOJU7Vt2n'
    },
    'dhis_prev_new_eptb_cases_treatment_completed': {
        'id': 'NFRHK1cgHrc',  # TB New Treatment outcome New EPTB Treatment completed
        'data_set': 'fOiOJU7Vt2n'
    },
    'dhis_prev_new_eptb_cases_dead': {
        'id': 'dzRlRptoj38',  # TBTO EPTB Died
        'data_set': 'VEqRXwmqhM1'
    },
    'dhis_prev_new_eptb_cases_treatment_failure': {
        'id': 'wNKjjozXyQY',  # TB New Treatment outcome New EPTB Treatment failed
        'data_set': 'fOiOJU7Vt2n'
    },
# TB COMM Number of Sputum sample collection points in the catchment (BU493LnfBTD) not found
# TB COMM Number of Functional Sputum sample collection points in the catchment (GhUn5j5ajzK) not found
# NCD CC HIV Status +Ve on ART (E2TaryAVqeT) not found
    'dhis_village_clinics': {
        'id': 'gtLvoz94gur',  # CHD IMCI # of Functional Village Clinics Within Catchment
        'data_set': 'hWDsGIjs16g'
    },
    'dhis_cbdas': {
        'id': 'LGaHPDsUydT',  # HTS Number of CBDA/HSA
        'data_set': 'wLQlOnKX6yN'
    },
    'dhis_village_health_committees': {
        'id': 'J7fogdejE3j',  # HMIS HM Active village health committees within catchment area
        'data_set': 'q1Es3k3sZem'
    },
    'dhis_households_with_improved_latrines': {
        'id': 'u5erlSYbxTU',  # ENVT EH # Of Households Owning And Using Improved Sanitary Facilities
        'data_set': 'iTo9FkAJTSl'
    },
    'dhis_households_access_to_clean_water': {
        'id': 'BDzMvX3y7Kc',  # HMIS # of Households with Access to Safe Drinking Water
        'data_set': 'q1Es3k3sZem'
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
