#!/usr/bin/env python3
import requests
import json
import time
import datetime
import psycopg2
import csv
import logging
import os
import io

from requests.auth import HTTPBasicAuth
from elt.job import Job, State
from elt.utils import compose, setup_db
from elt.db import DB
from elt.process import write_to_db_from_csv, create_tmp_table, update_set_stmt
from elt.schema import schema_apply
from elt.error import Error, ExtractError
from schema import describe_schema, field_column_name
from config import environment, getPGCreds, getZuoraFields, getObjectList


def item_jobs(item):
    """
    Yields import jobs for a specific item from the following sources:
      - Previous failed jobs
      - Zuora API incremental job
    """
    for job in recover_jobs(item):
        logging.info("Recovering failed job {}".format(job))
        yield job

    # creating the incremental job to send in the pipeline
    job = Job(elt_uri=item_elt_uri(item),
              payload={
                  'zuora_state': 'init',
              })

    logging.info("Retrieving incremental job for {}.".format(item))
    yield job


def recover_jobs(item):
    with DB.session() as session:
        elt_uri = item_elt_uri(item)
        failed_jobs = session.query(Job).filter_by(state=State.FAIL,
                                                   elt_uri=elt_uri).all()

    logging.info("Found {} failed job for {}.".format(len(failed_jobs), elt_uri))
    return failed_jobs


def item_elt_uri(item):
    return "com.zuora.meltano:1:{}".format(item)


def create_extract_job(job, item):
    if job.payload['zuora_state'] != 'init':
        return

    url = environment['url']
    headers, data = zuora_query_params(item)

    job.transit(State.RUNNING)
    job.started_at = datetime.datetime.utcnow()
    job.payload.update({
        'query': data
    })
    Job.save(job)

    res = requests.post(url,
                        auth=HTTPBasicAuth(environment['username'], environment['password']),
                        headers=headers,
                        data=data,
                        stream=True)

    # TODO: add error handling
    raise ExtractError("This error is crafted.")

    result = res.json()
    if result['status'] == 'error':
        raise ExtractError(result['message'])

    job_id = result['id']
    job.payload.update({
        'id': job_id,
        'url': url,
        'zuora_state': 'query',
        'http_response': result,
    })
    Job.save(job)

    return job


def load_extract_job(job, job_id):
    if job.payload['zuora_state'] != 'query':
        return

    params = {
        'auth': HTTPBasicAuth(environment['username'],
                              environment['password']),
        'headers': {'Accept': 'application/json'},
    }

    url = "/".join((environment['url'], "jobs", job_id))
    result = ""
    while True:
        res = requests.get(url, **params)
        result = res.json()
        status = result['batches'][0]['status']

        job.payload['http_response'] = result
        job.payload['zuora_state'] = status
        Job.save(job)
        time.sleep(5)
        if status == 'completed':
            break

    # expect to only have 1 batch
    file_id = result['batches'][0]['fileId']
    logger.debug("File ID: " + file_id)
    job.payload['file_id'] = file_id
    Job.save(job)

    return job


def download_file(item, file_id):
    params = {
        'auth': HTTPBasicAuth(environment['username'],
                              environment['password']),
        'headers': {'Accept': 'application/json'},
    }

    filename = "{}.csv".format(item)
    fields = [field_column_name(field) for field in getZuoraFields(item)]

    url = "https://www.zuora.com/apps/api/file/{}".format(file_id)
    res = requests.get(url, **params)

    def rename_fields(row):
        renamed = dict()
        for col, val in row.items():
            column_name = col.replace("{}.".format(item), "")
            column_name = column_name.replace(".", "")
            column_name = column_name.lower()

            renamed[column_name] = val

        return renamed

    with open(filename, 'w') as file:
        buf = io.StringIO(res.text)
        dr = csv.DictReader(buf, delimiter=',')
        dw = csv.DictWriter(file,
                            delimiter=',',
                            fieldnames=fields,
                            extrasaction='ignore')

        # write the file back ignoring the fields that are not in our schema
        dw.writeheader()
        for row in dr:
            dw.writerow(rename_fields(row))

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

    logger.debug("[Restore] Writing to {}/{}".format(host, db))
    with open(item + '.csv', 'r') as file, \
        DB.open() as mydb, \
        mydb.cursor() as cursor:

        cursor.execute('TRUNCATE TABLE zuora.' + item)
        columns = [field_column_name(field) for field in getZuoraFields(item)]

        copy = psycopg2.sql.SQL("COPY zuora.{} ({}) FROM STDIN WITH(FORMAT csv, HEADER true)").format(
            psycopg2.sql.Identifier(item.lower()),
            psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, columns)),
        )
        print(copy.as_string(cursor))

        cursor.copy_expert(file=file, sql=copy)
        logger.debug('Complteted copying records to ' + item + ' table.')

    #os.remove(item + '.csv')


def db_write_incremental(item):
    _, _, host, db, _ = getPGCreds()

    logger.debug("[Update] Writing to {}/{}".format(host, db))
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

        columns = [field_column_name(field) for field in getZuoraFields(item)]

        # load tmp table
        copy = psycopg2.sql.SQL("COPY pg_temp.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
            tmp_table,
            psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, columns)),
        )
        cursor.copy_expert(file=file, sql=copy)

        # upsert from tmp table
        update_columns = [col for col in columns if col != primary_key]
        update_query = psycopg2.sql.SQL("INSERT INTO {0}.{1} ({2}) SELECT {2} FROM {3}.{4} ON CONFLICT ({5}) DO UPDATE SET {6}").format(
            schema,
            table,
            psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, columns)),
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
        mydb.commit()

        logger.info('Completed copying records to ' + item + ' table.')
    os.remove(item + '.csv')


def db_write(job, item):
    if job.payload['zuora_state'] != 'completed':
        return

    http_response = job.payload['http_response']

    # TODO: process multiple batch
    batch = http_response['batches'][0]

    if batch['status'] != 'completed':
        return

    if batch.get('recordCount', 0) == 0:
        return

    if batch.get('full', False):
        db_write_full(item)
    else:
        db_write_incremental(item)


def zuora_query_params(item):
    query = "Select " + ', '.join(getZuoraFields(item)) + " FROM " + item + " LIMIT 10"
    # TODO: add Partner ID + Project ID to have incremental
    data = json.dumps({
        "format": "csv",
        "version": "1.2",
        "name": "Meltano",
        "encrypted": "none",
        "useQueryLabels": "true",
        "partner": environment['partner_id'],
        "project": environment['project_id'],
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


def import_job(job, item):
    """
    Job pipeline:
      - Create an extract job
      - Wait for the extract job to be completed
      - Download the extracted file
      - Write it in the database

    Any exception caught here will change the job's state to `FAIL`
    set the `exception` key to the exception message.
    """
    try:
        create_extract_job(job, item)
        load_extract_job(job, job.payload['id'])
        filename = download_file(item, job.payload['file_id'])
        db_write(job, item)

        job.transit(State.SUCCESS)
    except (Error, psycopg2.Error) as err:
        logger.debug('Something went wrong: {}'.format(err))
        job.transit(State.FAIL)
        job.payload['exception'] = str(err)
    finally:
        job.ended_at = datetime.datetime.utcnow()

    Job.save(job)


def main():
    start = time.time()

    for item in getObjectList():
        for job in item_jobs(item):
            import_job(job, item)

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
