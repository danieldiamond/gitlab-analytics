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
from elt.db import db_open
from elt.process import write_to_db_from_csv
from elt.schema import schema_apply
from schema import describe_schema
from config import getEnvironment, getPGCreds, getZuoraFields, getObjectList


def create_extract_job(item):
    username, password, url = getEnvironment()
    headers, data = zuroa_query_params(item)

    res = requests.post(
        url, auth=HTTPBasicAuth(username, password),
        headers=headers, data=data, stream=True)
    # TODO: add error handling
    result = res.json()
    job_id = result['id']

    job = Job('com.zuroa.meltano::v1', payload={
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
    job.payload['zuora_state'] = 'load'
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


def writeToDbFromFile(item):
    _, _, host, db, _ = getPGCreds()

    logger.debug("Writing to {}/{}".format(host, db))
    with open(item + '.csv', 'r') as file, \
        db_open() as mydb, \
        mydb.cursor() as cursor:
        reader = csv.reader(file, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)

        header = next(file).rstrip().replace("{}.".format(item), "").lower()
        # cursor.execute('TRUNCATE TABLE zuora.' + item)

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
        with db_open() as db:
            writeToDbFromFile(item)
            #write_to_db_from_csv(db, filename,
            #                     table_name=item.lower(),
            #                     table_schema='zuroa',
            #                     header_transform=header_transform)
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

    schema = describe_schema()
    with db_open() as db:
       schema_apply(db, schema)

    main()
