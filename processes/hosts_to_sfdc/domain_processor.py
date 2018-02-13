#!/usr/bin/python
"""GitLab CE instance ping to SFDC account processor.

This module is used to process an ever growing list of free GitLab instance
hosts. It identifies information about the organization that owns the domain
and creates an account in SFDC for it. If the account already exists in SFDC,
it updates it with the lastest number of GitLab CE instances and user count.
"""

from ipwhois import IPWhois
import timeit
import re
import psycopg2
import socket
import tldextract
from dw_setup import host, username, password, database
import discoverorg as dorg
import clearbit_gl as cbit
import caching
import whois_gl


def url_parse(host, ip=None):
    """Return a domain from a url and write to the clean domain cache

    :param host: the hostname to parse
    :return: domain and suffix if parsed or an error string if not.
    """
    # print("Parsing: " + domain)
    result = tldextract.extract(host)
    if result.domain:
        clean_domain = result.domain + '.' + result.suffix
        # Always writing to DB b/c there's no external API request - might as well just update
        caching.write_clean_domain(host, result, clean_domain)
        if ip is not None:
            caching.write_clean_domain(ip, result, clean_domain)
        return clean_domain
    else:
        # Can get an error with http://@#$^#$&*%*sfgdfg@3423
        err = "Not a valid domain"
        return err


def process_domain(domain):
    """Process a domain and update the cache with data if needed.

    This should only take in processed domains.

    Encodes everything in utf-8, as our data is international.
    """
    #TODO probably need their own functions as the data is different. OK for now.
    in_cb_cache = caching.in_cache(domain, 'clearbit_cache')
    in_dorg_cache = caching.in_cache(domain, 'discoverorg_cache')

    if in_cb_cache and in_dorg_cache:
        return

    # Update DiscoverOrg
    if not in_dorg_cache:
        written_to_dorg = dorg.update_discoverorg(domain)
        # This will skip Clearbit if we write to Dorg
        if written_to_dorg is True:
            return

    # Update Clearbit
    if not in_cb_cache and written_to_dorg is False:
        cbit.update_clearbit(domain)
        return


def is_ip(host):
    """
    Returns true if domain is an IP address, otherwise false.
    :param host:
    :return:
    """
    # parsed = urlparse.urlparse(host) # Probably don't need this extra standardization step
    tlded =tldextract.extract(host)
    if len(tlded.ipv4) > 0:
        return True
    else:
        return False


def process_ips(ip_address):
    """Identify a company from an ip address.

    Pulls a list of IP addresses for GitLab hosts and
    cache any data that is found in the data warehouse.
    """
    tlded = tldextract.extract(ip_address).ipv4

    if re.search(r'172\.(1[6-9]|2[0-9]|31)\.|192\.168|10\.', tlded):
        # These are reserved for private networks.
        return

    try:
        # Reverse DNS Lookup
        r = socket.gethostbyaddr(tlded)
        dns_domain = r[0]
        parsed_domain = url_parse(dns_domain, ip=ip_address)
        process_domain(parsed_domain)

    except socket.herror:
        # Check WHOIS
        whois_gl.ask_whois(tlded)



def url_processor(domain_list):
    """
    Takes a postgres result cursor and iterats through the domains

    :param domain_list:
    :return:
    """
    for url in domain_list:
        the_url = url[0]
        print "Procssing url " + the_url

        if is_ip(the_url):
            process_ips(the_url)
        else:
            parsed_domain = url_parse(the_url)
            process_domain(parsed_domain)


def process_version_checks():
    mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cursor = mydb.cursor()
    cursor.execute("SELECT referer_url FROM version.version_checks TABLESAMPLE SYSTEM_ROWS(5)")
                   # "WHERE updated_at ::DATE >= (now() - '60 days'::INTERVAL)"
                   # " LIMIT 50")
    result = cursor.fetchall()
    url_processor(result)


if __name__ == "__main__":
    total_time = timeit.timeit("process_version_checks()", setup="from __main__ import process_version_checks", number=1)
    print total_time
