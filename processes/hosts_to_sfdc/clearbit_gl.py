#!/usr/bin/python
"""Wrapper for the Clearbit API"""

import os
import datetime
from timeout import timeout
from sqlalchemy import Table
import clearbit
from dw_setup import metadata, engine
import caching

clearbit.key = os.environ.get('CLEARBIT_API_KEY')

clearbit_cache = Table('clearbit_cache',
                       metadata,
                       autoload=True,
                       autoload_with=engine)


@timeout(20)
def check_clearbit(domain):
    """Check the Clearbit API for a given domain.

    Identify company data based on the domain and
    returns the results. Returns None if not found.
    """
    # print("Querying Clearbit for " + domain)
    try:
        company = clearbit.Company.find(domain=domain, stream=True)
    except:
        company = None

    if company is not None and dict(company).get("name", None) is not None:
        pass
    else:
        company = None

    return company


def update_clearbit(domain):
    """
    Check Clearbit and cache if found
    :param domain: the cleaned domain to search for
    :return:
    """
    company = check_clearbit(domain)

    if company is None:
        caching.update_cache_not_found(domain, clearbit_cache)

    else:
        company_dict = dict(company)
        category = company_dict.get("category", {})
        metrics = company_dict.get("metrics", {})

        dictlist = dict(
            parsed_domain=domain,
            company_name=company_dict.get('name', ''),
            company_legalname=company_dict.get('legalName', ''),
            company_domain=company_dict.get('domain', ''),
            company_site=category.get('sector', ''),
            company_industrygroup=category.get('industryGroup', ''),
            company_industry=category.get('industry', ''),
            company_naics=category.get('naicsCode', ''),
            company_desc=company_dict.get('description', ''),
            company_loc=company_dict.get('location', ''),
            company_ein=company_dict.get('identifiers', {}).get('usEIN', ''),
            company_emp=metrics.get('employees', ''),
            company_emp_range=metrics.get('employeesRange', ''),
            company_rev=metrics.get('annualRevenue', ''),
            company_estrev=metrics.get('estimatedAnnualRevenue', ''),
            company_type=company_dict.get('type', ''),
            company_phone=company_dict.get('phone', ''),
            company_tech=company_dict.get('tech', ''),
            company_index=company_dict.get('indexedAt', ''),
            last_update=datetime.datetime.now()
        )

        # TODO Feel like there shouldn't be this much error catching for strings
        for key in dictlist:
            value = dictlist[key]
            if value is None:
                dictlist[key] = ""
            elif key == "last_update" or isinstance(value, list) or isinstance(value, int):
                dictlist[key] = str(value)
            else:
                dictlist[key] = str(value.encode("utf-8"))

        caching.update_cache(dictlist, clearbit_cache)

    return
