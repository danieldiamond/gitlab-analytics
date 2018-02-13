#!/usr/bin/python

import datetime
from ipwhois import IPWhois
from sqlalchemy import Table
from toolz.itertoolz import get as list_get
from dw_setup import metadata, engine
from caching import update_cache_not_found, update_whois_cache

whois_cache = Table('whois_cache',
                    metadata,
                    autoload=True,
                    autoload_with=engine)


def ask_whois(ip):
    """Check RDAP for whois data.

    For a given ip address, attempt to identify the company that owns it.
    """

    try:
        obj = IPWhois(ip)
        r = obj.lookup_rdap()
    except:
        print("No one knows who " + ip + " is. Updating cache as not found.")
        update_cache_not_found(ip, whois_cache)
        return

    name = r.get('network', {}).get('name', None)

    if (name == 'SHARED-ADDRESS-SPACE-RFCTBD-IANA-RESERVED'):
        print(ip + " is reserved IP space for ISPs. Updating as not found.")
        update_cache_not_found(ip, whois_cache)
        return

    else:
        if name is not None:
            org = name.encode('utf-8')

        remarks = list_get(0, r.get('network',{}).get('remarks', []), {})

        dictlist = dict(
            domain=ip,
            name=org,
            description=remarks.get('description', None),
            asn_description=r.get('asn_description', None),
            country_code=r.get('asn_country_code', None),
            last_update=datetime.datetime.now()
        )

        # TODO Feel like there shouldn't be this much error catching for strings
        for key in dictlist:
            value = dictlist[key]
            if value is None:
                pass
            elif key == "last_update" or isinstance(value, list) or isinstance(value, int):
                dictlist[key] = str(value)
            else:
                dictlist[key] = str(value.encode("utf-8"))

        update_whois_cache(dictlist, whois_cache)
        return