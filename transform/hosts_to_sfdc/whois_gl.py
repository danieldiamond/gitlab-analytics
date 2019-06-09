#!/usr/bin/python

import datetime
import logging
from ipwhois import IPWhois
from sqlalchemy import Table
from toolz.itertoolz import get as list_get
from .dw_setup import metadata, engine
from .caching import update_cache_not_found, update_whois_cache

whois_cache = Table("whois_cache", metadata, autoload=True, autoload_with=engine)


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


def ask_whois(clean_ip):
    """Check RDAP for whois data.

    For a given ip address, attempt to identify the company that owns it.
    """

    try:
        obj = IPWhois(clean_ip)
        r = obj.lookup_rdap()
    except:
        # print("No one knows who " + ip + " is. Updating cache as not found.")
        logger.debug("Not found in WHOIS. Updating Cache.")
        update_cache_not_found(clean_ip, whois_cache)
        return

    name = r.get("network", {}).get("name", None)

    if name == "SHARED-ADDRESS-SPACE-RFCTBD-IANA-RESERVED":
        # print(ip + " is reserved IP space for ISPs. Updating as not found.")
        logger.debug("Reserved IP space for ISPs. Updating Cache.")
        update_cache_not_found(clean_ip, whois_cache)
        return

    else:
        if name is not None:
            org = name.encode("utf-8")

        remarks = list_get(0, r.get("network", {}).get("remarks", []), {})

        dictlist = dict(
            domain=clean_ip,
            name=org,
            description=remarks.get("description", None),
            asn_description=r.get("asn_description", None),
            country_code=r.get("asn_country_code", None),
            last_update=datetime.datetime.now(),
        )

        logger.debug("Updating WHOIS Cache.")
        update_whois_cache(string_converter(dictlist), whois_cache)

    return


logging.basicConfig(format="%(asctime)s %(message)s", datefmt="%Y-%m-%d %I:%M:%S %p")
logging.getLogger(__name__).setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)
