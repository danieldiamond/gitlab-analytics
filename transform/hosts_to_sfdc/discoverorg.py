#!/usr/bin/python3
"""This module is a very minimal wrapper for the DiscoverOrg API."""


import os
import datetime
import requests
import json
from .dw_setup import metadata, engine
from sqlalchemy import Table
from .caching import update_cache_not_found, update_cache

dorg_key = os.environ.get('DORG_API_KEY')
dorg_user = os.environ.get('DORG_USERNAME')
dorg_pass = os.environ.get('DORG_PASSWORD')
url_base = 'https://papi.discoverydb.com/papi/'

discoverorg_cache = Table('discoverorg_cache',
                       metadata,
                       autoload=True,
                       autoload_with=engine)


def string_converter(dictlist):
    """
    Convert everything to strings.
    """

    for key, value in dictlist.items():
            try:
                dictlist[key] = str(value.encode("utf-8"))
            except:
                dictlist[key] = str(value)

    return dictlist


def get_dorg_token():
    """Log into the DiscoverOrg API and return an auth token."""
    data = dict(
        username=dorg_user,
        password=dorg_pass,
        partnerKey=dorg_key
    )
    json_data = json.dumps(data)
    url = url_base + 'login'
    r = requests.post(url, json_data)
    token = r.headers['X-AUTH-TOKEN']
    return token


def check_discoverorg(domain):
    """For a given domain, return the DiscoverOrg data."""
    url = url_base + 'v1/search/companies'
    token = get_dorg_token()
    header = {
        "X-AUTH-TOKEN": token,
        "Content-Type": 'application/json'
    }

    search_request = dict(
        companyCriteria=dict(
            queryString=domain,
            queryStringApplication=['EMAIL_DOMAIN']
        )
    )

    r = requests.post(url, headers=header, data=json.dumps(search_request))
    company = json.loads(r.content)
    if company.get("numberOfElements", 0) == 0:
        return None
    else:
        return company


def update_discoverorg(domain):
    """
    Check discoverorg and update cache if found.
    :param domain: The cleaned URL to search for.
    :return:
    """
    company = check_discoverorg(domain)

    if company is None:
        update_cache_not_found(domain, discoverorg_cache)
        return False
    else:
        content = company.get("content", [])
        if len(content) > 0:
            content = content[0]

        location = content.get("location", {})

        dictlist = dict(
            parsed_domain=domain,
            company_name=content.get('name', ''),
            company_legalname=content.get('fullName', ''),
            company_domain=content.get('emailDomain', ''),
            company_site=content.get('sector', ''),  # not in dorg
            company_industrygroup=content.get('industrygroup', ''),  # not in dorg
            company_industry=content.get('industry', ''),
            company_naics=','.join(str(v) for v in content.get('naics', [])),
            company_desc=content.get('description', ''),
            company_loc=', '.join([
                location.get('streetAddress1', ''), \
                location.get('city', ''), \
                location.get('stateProvicneRegion', ''), \
                location.get('postalCode', ''), \
                location.get('isoCountryCode', '')
            ]),
            company_ein=content.get('ein', ''),  # not in dorg
            company_emp=content.get('numEmployees', ''),
            company_emp_range=content.get('employeesRange', ''),  # not in dorg
            company_rev=content.get('revenue', ''),
            company_estrev=content.get('estimatedAnnualRevenue', ''),  # not in dorg
            company_type=content.get('ownershipType', ''),
            company_phone=content.get('mainPhoneNumber', ''),
            company_tech=content.get('tech', ''),  # not in dorg
            company_index=content.get('indexedAt', '1970-01-01 00:00:00.300000'),  # not in dorg
            last_update=datetime.datetime.now()
        )

        update_cache(string_converter(dictlist), discoverorg_cache)
        return True