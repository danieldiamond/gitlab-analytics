#!/usr/bin/python
"""Wrapper for the Clearbit API"""

import os
from timeout import timeout
from sqlalchemy import *
import clearbit
# from hosts_to_sfdc import update_cache_not_found

clearbit.key = os.environ.get('CLEARBIT_API_KEY')


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
