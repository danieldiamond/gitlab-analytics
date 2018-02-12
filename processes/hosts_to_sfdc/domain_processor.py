#!/usr/bin/python
"""GitLab CE instance ping to SFDC account processor.

This module is used to process an ever growing list of free GitLab instance
hosts. It identifies information about the organization that owns the domain
and creates an account in SFDC for it. If the account already exists in SFDC,
it updates it with the lastest number of GitLab CE instances and user count.
"""

import datetime
from ipwhois import IPWhois
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql
import psycopg2
import socket
import tldextract
from dw_setup import metadata, engine, host, username, password, database
import discoverorg as dorg
import clearbit_gl as cbit
import caching


ip_to_url = Table('ip_to_url',
                  metadata,
                  schema='version',
                  autoload=True,
                  autoload_with=engine)


def in_cache(domain, table):
    """Return True if domain found in cache, False if not found.

    Check the specified cache for a domain to see if the domain has
    been cached in the last 30 days.

    :param table: String of table name
    """
    # print("Checking cache for " + domain)
    mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM " + table + " WHERE domain='" +
                   domain + "' AND last_update >  NOW() - INTERVAL '30 days';")
    domain_in_cache = False if cursor.rowcount == 0 else True

    mydb.commit()
    cursor.close()
    mydb.close()

    return domain_in_cache


def url_parse(host):
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
    in_cb_cache = in_cache(domain, 'clearbit_cache')
    in_dorg_cache = in_cache(domain, 'discoverorg_cache')

    if in_cb_cache and in_dorg_cache:
        return

    # Update DiscoverOrg
    if not in_dorg_cache:
        dorg.update_discoverorg(domain)

    # Update Clearbit
    if not in_cb_cache:
        cbit.update_clearbit(domain )

    #TODO Write to cleaned version_checks


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


def update_ip_to_url(ip, url):
    """Cache the results of the reverse DNS lookup.

    If we were able to translate the IP to a domain,
    update the ip_to_url cache with that value.
    """
    # print("Updating cache for " + ip, url)
    stmt = postgresql.insert(ip_to_url, bind=engine).values(
        host=ip,
        url=url,
        last_update=datetime.datetime.now())
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=['host'],
        set_=dict(url=url,
                  last_update=datetime.datetime.now()))
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()


def ask_whois(ip):
    """Check RDAP for whois data.

    For a given ip address, attempt to identify the company that owns it.
    """
    # print("Asking whois " + ip)
    # TODO Lookup in Cleaned version ping
    org = ""
    desc = ""
    # TODO write this to cleaned version ping
    try:
        obj = IPWhois(ip)
        r = obj.lookup_rdap()
    except:
        print("No one knows who " + ip + " is. Updating cache as not found.")
        # update_cache_not_found(ip)
        return
    if (r['network']['name'] == 'SHARED-ADDRESS-SPACE-RFCTBD-IANA-RESERVED'):
        print(ip + " is reserved IP space for ISPs. Updating as not found.")
        # update_cache_not_found(ip)
    else:
        try:
            if r['network']['name'] is not None:
                org = r['network']['name'].encode('utf-8')
        except TypeError:
            pass
            print("Whois has no name. Updating the organization desc.")
        try:
            if r['network']['remarks'][0]['description'] is not None:
                desc = \
                    r['network']['remarks'][0]['description'].encode('utf-8')
        except TypeError:
            print("Whois has no description. Updating the organization name.")
            pass
        # print("Whois " + ip + "? ARIN says it's " + org +
        #       ". Updating cache..")
        # update_cache_whois(ip, org, desc)


def process_ips(ip_address):
    """Identify a company from an ip address.

    Pulls a list of IP addresses for GitLab hosts and
    cache any data that is found in the data warehouse.
    """
    # parsed = urlparse.urlparse(ip_address) # Probably don't need this extra clean step
    tlded = tldextract.extract(ip_address).ipv4
    # thinking there's no need to do a cache lookup on the IP. The cost for doing a reverse lookup is cheap
    # I can just look up in the cleaned ping before asking WHOIS

    try:
        r = socket.gethostbyaddr(tlded)
        dns_domain = r[0]
        parsed_domain = url_parse(dns_domain)
        process_domain(parsed_domain)

    except socket.herror:
        print "I would've checked WHOIS"
        # ask_whois(tlded)



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
    cursor.execute("SELECT referer_url FROM version.version_checks TABLESAMPLE SYSTEM_ROWS(20)")
                   # "WHERE updated_at ::DATE >= (now() - '60 days'::INTERVAL)"
                   # " LIMIT 50")
    result = cursor.fetchall()
    url_processor(result)


process_version_checks()



# url_processor([('totalasdgijasdfpj#%@',)])
# print url_parse('totalasdgijasdfpj#%@')

# pprint.pprint(dorg.lookup_by_domain('example.com'))
# process_domains()
# process_ips()


