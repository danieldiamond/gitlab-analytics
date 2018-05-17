#!/usr/bin/env python3
import requests
import json
import time
import datetime
import psycopg2
import csv
import logging
import os

from configparser import SafeConfigParser
from requests.auth import HTTPBasicAuth


def getEnvironment():
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, '../config', 'environment.conf')
    EnvParser = SafeConfigParser()
    EnvParser.read(myPath)
    username = EnvParser.get('ZUORA', 'username')
    password = EnvParser.get('ZUORA', 'password')
    url = EnvParser.get('ZUORA', 'url')
    return(username, password, url)


def getPGCreds():
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, '../config', 'environment.conf')
    EnvParser = SafeConfigParser()
    EnvParser.read(myPath)
    username = EnvParser.get('POSTGRES', 'user')
    password = EnvParser.get('POSTGRES', 'pass')
    host = EnvParser.get('POSTGRES', 'host')
    database = EnvParser.get('POSTGRES', 'database')
    port = EnvParser.get('POSTGRES', 'port')
    return (username, password, host, database, port)


def getZuoraFields(item):
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, '../config', 'zuoraFields.conf')
    FieldParser = SafeConfigParser()
    FieldParser.read(myPath)
    fields = FieldParser.get(item, 'fields')
    return fields


def getObjectList():
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, '../config', 'zuoraFields.conf')
    ObjectList = SafeConfigParser()
    ObjectList.read(myPath)
    obj = ObjectList.get('Zbackup', 'objects')
    obj = obj.replace(" ", "")
    objList = obj.split(",")
    return objList

def create_extract_job(url, username, password, headers, data):
    r = requests.post(
        url, auth=HTTPBasicAuth(username, password),
        headers=headers, data=data, stream=True)
    # TODO: add error handling
    result = json.loads(r.text)
    job_id = result['id']

    job = Job('com.zuroa.meltano', payload={
        'id': job_id,
        'url': url,
        'zuroa_state': 'query',
        'data': data
    })
    job.transit(State.RUNNING)
    job.started_at = datetime.datetime.utcnow()

    return job


def load_extract_job(job, job_id, username, password):
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
        if status == 'completed':
            break

    # expect to only have 1 batch
    file_id = result['batches'][0]['fileId']
    logger.debug("File ID: " + file_id)
    job.payload['zuora_state'] = 'load'
    job.payload['file_id'] = file_id

    url = "https://www.zuora.com/apps/api/file/{}".format(file_id)
    return requests.get(url, **params)


def writeToFile(item, res):
    filename = "{}.csv".format(item)
    with open(filename, 'wb') as file:
        logger.debug('Writing to local file: ' + filename)
        file.write(res.content)


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


def writeToDbFromFile(item, username, password, host, database, port):
    logger.debug('Writing to ' + database + ' on ' + host)
    with open(item + '.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        try:
            mydb = psycopg2.connect(host=host, user=username,
                                    password=password, dbname=database, port=port)
            cursor = mydb.cursor()
            next(file)  # Skip the header row.
            cursor.execute('TRUNCATE TABLE zuora.' + item)
            copy = 'COPY zuora.' + item + ' FROM STDIN with csv'
            cursor.copy_expert(file=file, sql=copy)
            logger.debug('Complteted copying records to ' + item + ' table.')
            mydb.commit()
            cursor.close()
            mydb.close()
            file.close()
            os.remove(item + '.csv')
        except psycopg2.Error as err:
            logger.debug('Something went wrong: {}'.format(err))


def main():
    start = time.time()
    for item in getObjectList():
        import_item(item)
    end = time.time()
    totalMinutes = (end - start) / 60
    logger.debug('Completed Load in %1.1f minutes' % totalMinutes)


def zuroa_query_params(item):
    logger.debug('Executing Query:  ' + query)
    username, password, url = getEnvironment()
    query = "Select " + getZuoraFields(item) + " FROM " + item
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
    dBuser, dBpass, host, db, port = getPGCreds()

    headers, data = zuroa_query_params(item)
    job = create_extract_job(url, username, password, headers, data)
    file = load_extract_job(job, job.payload['id'], username, password)

    writeToFile(item, file)
    writeToDbFromFile(item, dBuser, dBpass, host, db, port)


logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    main()
