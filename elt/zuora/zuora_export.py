#!/usr/bin/env python
import requests
from requests.auth import HTTPBasicAuth
import json
import time
from ConfigParser import SafeConfigParser
import psycopg2
import csv
import logging
import os


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


def getResults(url, username, password, headers, data):
    r = requests.post(
        url, auth=HTTPBasicAuth(username, password),
        headers=headers, data=data, stream=True)
    result = json.loads(r.text)                    # add error handling
    job_id = result['id']
    headers = {
        'Accept': 'application/json',
    }
    url = url + 'jobs/' + job_id
    result = ''
    while True:
        r = requests.get(
            url, auth=HTTPBasicAuth(username, password), headers=headers)
        result = json.loads(r.text)
        time.sleep(5)
        if result['batches'][0]['status'] == 'completed':
            logger.debug(result['batches'][0]['status'])
            break
    logger.debug("File ID: " + result['batches'][0]['fileId'])
    file_id = result['batches'][0]['fileId']
    url = 'https://www.zuora.com/apps/api/' + 'file/' + file_id
    r = requests.get(url, auth=HTTPBasicAuth(username, password),
                     headers=headers)
    return r


def writeToFile(filename, r):
    logger.debug('Writing to local file: ' + filename)
    with open(filename + '.csv', "wb") as file:
        file.write(r.content)
        logger.debug("Writing file: " + filename + '.csv')
        file.close()


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


def writeToDb(username, password, host, database, item, port, r):
    logger.debug('Writing to ' + database + ' on ' + host)
    reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"',
                        quoting=csv.QUOTE_ALL)
    try:
        mydb = psycopg2.connect(host=host, user=username,
                                password=password, dbname=database, port=port)
        cursor = mydb.cursor()
        cursor.execute('TRUNCATE TABLE zuora.' + item)
        count = 0
        firstLine = True
        for row in reader:
            if firstLine:
                firstLine = False
                continue
            if len(row) == 0:
                continue
            rowString = ', '.join('?' * len(row))
            rowString = rowString.replace("?", "%s")
            query_string = 'INSERT INTO zuora.' + item + \
                ' VALUES (%s)' % rowString
            replace(row)
            cursor.execute(query_string, row)
            count = count + 1
            if count % 10000 == 0:
                logger.debug('Inserting: %s records into %s', count, database)
        mydb.commit()
        cursor.close()
        mydb.close()
    except psycopg2.Error as err:
        logger.debug('Something went wrong: {}'.format(err))
        logger.debug('Error: ' + str(row))

    logger.debug('Total Count: %s', count)


def main():
    start = time.time()
    username, password, url = getEnvironment()
    dBuser, dBpass, host, db, port = getPGCreds()
    objList = getObjectList()
    for item in objList:
        logger.debug('Retreiving: %s', item)
        query = "Select " + getZuoraFields(item) + " FROM " + item
        # query = "Select * from " + item  # TESTING
        logger.debug('Executing Query:  ' + query)
        data = json.dumps({
            "format": "csv",
            "version": "1.2",
            "name": "Sample",
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
        r = getResults(url, username, password, headers, data)
        # writeToFile(item, r)
        writeToDb(dBuser, dBpass, host, db, item, port, r)
    end = time.time()
    totalMinutes = (end - start) / 60
    logger.debug('Completed Load in %1.1f minutes' % totalMinutes)


logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    main()
