import os

from configparser import SafeConfigParser


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
    fields = FieldParser.get(item, 'fields').split(', ')
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
