#!/usr/bin/python3

import datetime
from sqlalchemy import Table
from sqlalchemy.dialects import postgresql
import psycopg2
from toolz.dicttoolz import dissoc
from .dw_setup import metadata, engine, host, username, password, database

cleaned_urls = Table('cleaned_urls',
                    metadata,
                    autoload=True,
                    autoload_with=engine)


def in_cache(domain, table):
    """Return True if domain found in cache, False if not found.

    Check the specified cache for a domain to see if the domain has
    been cached in the last 30 days.

    :param table: String of table name
    """
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


def update_whois_cache(dictlist, table):
    """
    Updates the whois cache
    :param dictlist: dictionary of data to store
    :param table: Table object
    :return:
    """

    stmt = postgresql.insert(table, bind=engine).values(
        domain=dictlist.get("domain",""),
        name=dictlist.get("name", None),
        description=dictlist.get("description", None),
        asn_description=dictlist.get("asn_description", None),
        country_code=dictlist.get("country_code", None),
        last_update=dictlist.get("last_update", None)
    )
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=["domain"],
        set_=dissoc(dictlist, "domain")
    )
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()


def update_cache(dictlist, table):
    """If we have retrieved new data from the relevant API, we update the cache.

    Does an upsert to the table using the domain as the unique key.
    Does not return a value.

    :param table: SQLAlchemy Table
    """

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
    stmt = postgresql.insert(table,
                             bind=engine).values(domain=domain,
                                                 last_update=datetime.datetime.now())
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=['domain'],
        set_=dict(last_update=datetime.datetime.now()))
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()


def write_clean_domain(raw_domain, tldextract, clean_domain, table=cleaned_urls):
    """
    Writes a cleaned version of the domain to the DB

    :param raw_domain: String of the raw domain
    :param tldextract: tldextract object
    :param clean_domain: Our version of the cleaned domain (domain + subdomain)
    :param table: specified table
    :return:
    """
    subdomain = tldextract.subdomain
    primary_domain = tldextract.domain
    suffix = tldextract.suffix
    full_domain = subdomain + '.' + primary_domain + '.' + suffix

    stmt = postgresql.insert(table, bind=engine).values(
        domain=raw_domain,
        subdomain=subdomain,
        primary_domain=primary_domain,
        suffix=suffix,
        clean_domain=clean_domain,
        clean_full_domain=full_domain,
        last_update=datetime.datetime.now()
    )
    on_update_stmt = stmt.on_conflict_do_update(
        index_elements=["domain"],
        set_=dict(
            subdomain=subdomain,
            primary_domain=primary_domain,
            suffix=suffix,
            clean_domain=clean_domain,
            clean_full_domain=full_domain,
            last_update=datetime.datetime.now())
    )
    conn = engine.connect()
    conn.execute(on_update_stmt)
    conn.close()