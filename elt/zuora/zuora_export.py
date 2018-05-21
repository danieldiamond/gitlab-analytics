#!/usr/bin/env python3
import requests
import json
import time
import datetime
import psycopg2
import csv
import logging
import os

from requests.auth import HTTPBasicAuth
from elt.job import Job, State
from elt.utils import compose, setup_db
from elt.db import DB
from elt.process import write_to_db_from_csv, create_tmp_table, update_set_stmt
from elt.schema import schema_apply
from schema import describe_schema
from config import getEnvironment, getPGCreds, getZuoraFields, getObjectList


def recover_jobs():
    with DB.session() as session:
        failed_jobs = session.query(Job).filter_by(state=State.FAIL).all()
        for f in failed_jobs:
            logging.error(f.payload.get('zuroa_state', ''))
            logging.error(f.payload.get('file_id', ''))
            logging.error(f.payload.get('exception', ''))


def create_extract_job(item):
    username, password, url = getEnvironment()
    headers, data = zuroa_query_params(item)

    res = requests.post(
        url, auth=HTTPBasicAuth(username, password),
        headers=headers, data=data, stream=True)
    # TODO: add error handling
    result = res.json()
    job_id = result['id']

    job = Job(elt_uri='com.zuroa.meltano:1',
              payload={
                  'id': job_id,
                  'url': url,
                  'zuroa_state': 'query',
                  'http_response': result,
                  'query': data,
              })
    job.transit(State.RUNNING)
    job.started_at = datetime.datetime.utcnow()
    Job.save(job)

    return job


def load_extract_job(job, job_id):
    username, password, url = getEnvironment()
    params = {
        'auth': HTTPBasicAuth(username, password),
        'headers': {'Accept': 'application/json'},
    }

    url = url + 'jobs/' + job_id
    result = ''
    while True:
        res = requests.get(url, **params)
        result = json.loads(res.text)
        time.sleep(5)
        status = result['batches'][0]['status']
        logger.debug(status)
        job.payload['zuora_state'] = status
        Job.save(job)
        if status == 'completed':
            break

    # expect to only have 1 batch
    file_id = result['batches'][0]['fileId']
    logger.debug("File ID: " + file_id)
    job.payload['file_id'] = file_id
    Job.save(job)

    url = "https://www.zuora.com/apps/api/file/{}".format(file_id)
    return requests.get(url, **params)


def writeToFile(item, res):
    filename = "{}.csv".format(item)
    with open(filename, 'wb') as file:
        logger.debug('Writing to local file: ' + filename)
        file.write(res.content)
    return filename


def replace(fieldList):
    for i, v in enumerate(fieldList):
        if v.upper() == 'TRUE':
            fieldList.pop(i)
            fieldList.insert(i, '1')
        if v.upper() == 'FALSE':
            fieldList.pop(i)
            fieldList.insert(i, '0')
        if v == '':
            fieldList.pop(i)
            fieldList.insert(i, None)


def db_write_full(item):
    _, _, host, db, _ = getPGCreds()

    logger.debug("Writing to {}/{}".format(host, db))
    with open(item + '.csv', 'r') as file, \
        DB.open() as mydb, \
        mydb.cursor() as cursor:
        reader = csv.reader(file, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)

        header = next(file).rstrip().replace("{}.".format(item), "").lower()
        cursor.execute('TRUNCATE TABLE zuora.' + item)

        column = compose(
            psycopg2.sql.Identifier,
            lambda h: h.replace(".", "")
        )

        columns = map(column, header.split(','))

        copy = psycopg2.sql.SQL("COPY zuora.{} ({}) FROM STDIN with csv").format(
            psycopg2.sql.Identifier(item.lower()),
            psycopg2.sql.SQL(', ').join(columns),
        )
        cursor.copy_expert(file=file, sql=copy)
        logger.debug('Complteted copying records to ' + item + ' table.')

    os.remove(item + '.csv')


def db_write_incremental(item):
    _, _, host, db, _ = getPGCreds()

    logger.debug("Writing to {}/{}".format(host, db))
    with open(item + '.csv', 'r') as file, \
        DB.open() as mydb, \
        mydb.cursor() as cursor:

        primary_key = 'id'
        table_name = item.lower()
        tmp_table_name = create_tmp_table(mydb, 'zuora', table_name)

        schema = psycopg2.sql.Identifier('zuora')
        table = psycopg2.sql.Identifier(table_name)
        tmp_table = psycopg2.sql.Identifier(tmp_table_name)

        reader = csv.reader(file, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)

        header = next(file).rstrip().replace("{}.".format(item), "").lower()
        column = compose(
            psycopg2.sql.Identifier,
            lambda h: h.replace(".", "")
        )

        columns = header.split(',')

        # load tmp table
        copy = psycopg2.sql.SQL("COPY pg_temp.{} ({}) FROM STDIN with csv").format(
            tmp_table,
            psycopg2.sql.SQL(', ').join(map(column, columns)),
        )
        cursor.copy_expert(file=file, sql=copy)

        # upsert from tmp table
        update_columns = [col for col in columns if col != primary_key]
        update_query = psycopg2.sql.SQL("INSERT INTO {0}.{1} ({2}) SELECT {2} FROM {3}.{4} ON CONFLICT ({5}) DO UPDATE SET {6}").format(
            schema,
            table,
            psycopg2.sql.SQL(', ').join(map(column, columns)),
            psycopg2.sql.Identifier("pg_temp"),
            tmp_table,
            psycopg2.sql.Identifier(primary_key),
            psycopg2.sql.SQL(update_set_stmt(update_columns)),
        )
        print(update_query.as_string(cursor))
        cursor.execute(update_query)
        mydb.commit()

        # Drop temporary table
        drop_query = psycopg2.sql.SQL("DROP TABLE {0}.{1}").format(
            psycopg2.sql.Identifier("pg_temp"),
            tmp_table,
        )

        logger.info('Completed copying records to ' + item + ' table.')
    os.remove(item + '.csv')


def zuroa_query_params(item):
    query = "Select " + ', '.join(getZuoraFields(item)) + " FROM " + item + " LIMIT 100"
    logger.debug('Executing Query:  ' + query)
    # TODO: add Partner ID + Project ID to have incremental
    data = json.dumps({
        "format": "csv",
        "version": "1.2",
        "name": "Meltano",
        "encrypted": "none",
        "useQueryLabels": "true",
        "queries": [
            {
                "name": item,
                "query": query,
                "type": "zoqlexport",
                "apiVersion": "88.0"
            }
        ]
    })
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    return (headers, data)


def import_item(item):
    logger.debug('Retreiving: %s', item)

    _, _, url = getEnvironment()
    headers, data = zuroa_query_params(item)

    job = create_extract_job(item)
    file = load_extract_job(job, job.payload['id'])

    try:
        filename = writeToFile(item, file)
        with DB.open() as db:
            db_write_incremental(item)

        job.transit(State.SUCCESS)
    except psycopg2.Error as err:
        logger.debug('Something went wrong: {}'.format(err))
        job.transit(State.FAIL)
        job.payload['exception'] = str(err)
    finally:
        job.ended_at = datetime.datetime.utcnow()

    Job.save(job)


def main():
    start = time.time()

    recover_jobs()
    for item in getObjectList():
        import_item(item)
    end = time.time()

    totalMinutes = (end - start) / 60
    logger.debug('Completed Load in %1.1f minutes' % totalMinutes)


logger = logging.getLogger(__name__)

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    setup_db()
    with DB.open() as db:
        schema_apply(db, Job.describe_schema())
        schema_apply(db, describe_schema())
        print("apply schema")

    main()
