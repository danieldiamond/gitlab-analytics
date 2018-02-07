#!/usr/bin/python
"""GitLab CE instance ping to SFDC account processor.

This module is used to process an ever growing list of free GitLab instance
hosts. It identifies information about the organization that owns the domain
and creates an account in SFDC for it. If the account already exists in SFDC,
it updates it with the lastest number of GitLab CE instances and user count.
"""

import datetime
from clearbit_gl import check_clearbit
import discoverorg as dorg
from ipwhois import IPWhois
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import socket
import tldextract
from toolz.dicttoolz import dissoc

import os

# host = os.environ.get('PROCESS_DB_PROD_ADDRESS')
host = "localhost"
username = os.environ.get('PROCESS_DB_PROD_USERNAME')
password = os.environ.get('PROCESS_DB_PROD_PASSWORD')
database = os.environ.get('PROCESS_DB_PROD_DBNAME')

# Setup sqlalchemy
Base = declarative_base()
db_string = 'postgresql+psycopg2://' + username + ':' + password + '@' + \
            host + '/' + database
engine = create_engine(db_string)
metadata = MetaData(bind=engine)

clearbit_cache = Table('clearbit_cache',
                       metadata,
                       autoload=True,
                       autoload_with=engine)

discoverorg_cache = Table('discoverorg_cache',
                       metadata,
                       autoload=True,
                       autoload_with=engine)

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


def update_cache(dictlist, table):
    """If we have retrieved new data from the relevant API, we update the cache.

    Does an upsert to the table using the domain as the unique key.
    Does not return a value.

    :param table: SQLAlchemy Table
    """
    # print("Updating cache for " + dictlist.get("parsed_domain", ""))

    stmt = postgresql.insert(table, bind=engine).values(
        domain=dictlist.get("parsed_domain", ""),
        company_name=dictlist.get("company_name", ""),
        company_legalname=dictlist.get("company_legalname", ""),
        company_domain=dictlist.get("company_domain", ""),
        company_site=dictlist.get("company_site", ""),
        company_industrygroup=dictlist.get("company_industrygroup", ""),
        company_industry=dictlist.get("company_industry", ""),
        company_naics=dictlist.get("company_naics", ""),
        company_desc=dictlist.get("company_desc", ""),
        company_loc=dictlist.get("company_loc", ""),
        company_ein=dictlist.get("company_ein", ""),
        company_emp=dictlist.get("company_emp", ""),
        company_emp_range=dictlist.get("company_emp_range", ""),
        company_rev=dictlist.get("company_rev", ""),
        company_estrev=dictlist.get("company_estrev", ""),
        company_type=dictlist.get("company_type", ""),
        company_phone=dictlist.get("company_phone", ""),
        company_tech=dictlist.get("company_tech", ""),
        company_index=dictlist.get("company_index", ""),
        last_update=dictlist.get("last_update", "")
    )
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=['domain'],
        set_=dissoc(dictlist, "parsed_domain")
    )
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()


def update_cache_not_found(domain, table):
    """Update the cache for unknown domains.

    If we are unable to identify the company and obtain details,
    we update the specified cache with the domain and the last updated field,
    to prevent us from asking the API again for this domain for 30 days.
    We need to limit the number of API calls we make to these services.

    :param table: SQLAlchemy table
    """
    # print("Updating cache for " + domain)
    stmt = postgresql.insert(table,
                             bind=engine).values(domain=domain,
                                                 last_update=datetime.datetime.now())
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=['domain'],
        set_=dict(last_update=datetime.datetime.now()))
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()


def url_parse(domain):
    """Return a domain from a url.

    Parses the domain and suffix from the referer_url in the version ping.
    Returns the domain and suffix if parsed or an error sting if not.
    """
    # print("Parsing: " + domain)
    result = tldextract.extract(domain)
    if result.domain:
        return result.domain + '.' + result.suffix
    else:
        # Can get an error with http://@#$^#$&*%*sfgdfg@3423
        err = "Not a valid domain"
        return err


def get_sub_domain(domain):
    """Return the subdomain from a FQDM.

    Parses the subdomain from the referer_url in the version ping.
    Returns the subdomain if parsed.
    """
    # print("Parsing subdomain:" + domain)
    result = tldextract.extract(domain)
    return result.subdomain


def get_domains():
    """Return a list of domains to process.

    Queries the database for new domains that need to be parsed from
    the version ping and usage data.
    """
    mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cursor = mydb.cursor()
    cursor.execute("SELECT refer_url from version.domains")
    result = cursor.fetchall()
    return result


def process_domains():
    """Should probably be the __main__ function.

    Gets a list of domains and processes it.
    """
    domain_list = get_domains()
    for domain in domain_list:
        process_domain(domain)


def process_domain(domain):
    """Process a domain and update the cache with data if needed.

    Encodes everything in utf-8, as our data is international.
    """
    domain = ''.join(domain).encode('utf-8')
    parsed_domain = url_parse(domain)

    in_cb_cache = in_cache(parsed_domain, 'clearbit_cache')
    in_dorg_cache = in_cache(parsed_domain, 'discoverorg_cache')

    if in_cb_cache and in_dorg_cache:
        print domain + " is in both caches"
        return

    # Update DiscoverOrg
    if not in_dorg_cache:
        company = dorg.check_discoverorg(parsed_domain)

    if company is None:
        print "Error or not found in dorg for " + domain + " parsed as " + parsed_domain
        update_cache_not_found(parsed_domain, discoverorg_cache)
    else:
        print "Updating discoverorg cache for " + domain + " parsed as " + parsed_domain
        content = company.get("content", [])
        if len(content) > 0:
            content = content[0]

        location = content.get("location", {})

        dictlist = dict(
            parsed_domain=parsed_domain,
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

        # TODO Feel like there shouldn't be this much error catching for strings
        for key in dictlist:
            value = dictlist[key]
            if value is None:
                dictlist[key] = ""
            elif key == "last_update" or isinstance(value, list) or isinstance(value, int):
                dictlist[key] = str(value)
            else:
                dictlist[key] = str(value.encode("utf-8"))

        update_cache(dictlist, discoverorg_cache)

    # Update Clearbit
    if not in_cb_cache:
        company = check_clearbit(parsed_domain)

    if company is None:
        print "Error or not found in clearbit for " + domain + " parsed as " + parsed_domain
        update_cache_not_found(parsed_domain, clearbit_cache)

    else:
        print "Updating clearbit cache for " + domain + " parsed as " + parsed_domain
        company_dict = dict(company)
        category = company_dict.get("category", {})
        metrics = company_dict.get("metrics", {})

        dictlist = dict(
            parsed_domain=parsed_domain,
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

        #TODO Feel like there shouldn't be this much error catching for strings
        for key in dictlist:
            value = dictlist[key]
            if value is None:
                dictlist[key] = ""
            elif key == "last_update" or isinstance(value, list):
                dictlist[key] = str(value)
            else:
                dictlist[key] = str(value.encode("utf-8"))

        update_cache(dictlist, clearbit_cache)


def get_ips():
    """Return a list if IP addresses to process.

    Queries the database for new IP addresses that need to be processed from
    the version ping and usage data.
    """
    mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
    cursor = mydb.cursor()
    cursor.execute("SELECT refer_url from version.ips")
    result = cursor.fetchall()
    return result


def update_cache_whois(ip, company_name, company_address):
    """Update the cache with whois data for a domain.

    If the only data we could get for a domain was from whois,
    update the cache with that.
    """
    # print("Updating cache with whois data for " + ip)
    stmt = postgresql.insert(clearbit_cache, bind=engine).values(
        domain=str(ip),
        company_name=company_name,
        company_loc=company_address,
        last_update=datetime.datetime.now())
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=['domain'],
        set_=dict(company_name=company_name,
                  company_loc=company_address,
                  last_update=datetime.datetime.now()))
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()
    # print("Cache Updated.")


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
    ip = ''.join(ip)
    org = ""
    desc = ""
    try:
        obj = IPWhois(ip)
        r = obj.lookup_rdap()
    except:
        # print("No one knows who " + ip + " is. Updating cache as not found.")
        update_cache_not_found(ip)
        return
    if (r['network']['name'] == 'SHARED-ADDRESS-SPACE-RFCTBD-IANA-RESERVED'):
        # print(ip + " is reserved IP space for ISPs. Updating as not found.")
        update_cache_not_found(ip)
    else:
        try:
            if r['network']['name'] is not None:
                org = r['network']['name'].encode('utf-8')
        except TypeError:
            pass
            # print("Whois has no name. Updating the organization desc.")
        try:
            if r['network']['remarks'][0]['description'] is not None:
                desc = \
                    r['network']['remarks'][0]['description'].encode('utf-8')
        except TypeError:
            pass
        #     print("Whois has no description. Updating the organization name.")
        # print("Whois " + ip + "? ARIN says it's " + org +
        #       ". Updating cache..")
        update_cache_whois(ip, org, desc)


def process_ips():
    """Identify a company from an ip address.

    Pulls a list of IP addresses for GitLab hosts and
    cache any data that is found in the data warehouse.
    """
    ips = get_ips()
    for ip in ips:
        ip = ''.join(ip)
        if in_cache(ip):
            continue
        else:
            try:
                r = socket.gethostbyaddr(ip)
                update_ip_to_url(ip, r[0])
                process_domain(r[0])
            except socket.herror:
                # print("Can't find reverse DNS for " + ip)
                ask_whois(ip)

process_domain("totallyfakeurl.taylor")
process_domain("futurevault.com")
process_domain("example.com")
# print process_domain('http://209.208.3.11/admin')
# print process_domain('http://@#$^#$&*%*sfgdfg@3423')


# pprint.pprint(dorg.lookup_by_domain('example.com'))
# process_domains()
# process_ips()
