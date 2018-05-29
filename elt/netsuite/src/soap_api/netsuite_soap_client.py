import os
import requests
import json
import logging
import time

from zeep import Client
from zeep.cache import SqliteCache
from zeep.transports import Transport
from zeep.exceptions  import Fault

from elt.error import Error

from .account import Account
from .currency import Currency
from .department import Department
from .subsidiary import Subsidiary
from .transaction import Transaction
from .expense import Expense
from .transaction_item import TransactionItem
from .application import Application
from .transaction_line import TransactionLine

class NetsuiteClient:
    def __init__(self):
        # The core soap client used to make all the requests
        self.client = self.netsuite_soap_client()

        # Create a type factory for important namespaces used all over the place
        self.core_namespace = self.client.type_factory(
                'urn:core_{}.platform.webservices.netsuite.com'.format(
                    os.getenv("NETSUITE_ENDPOINT")
                )
            )

        self.core_types_namespace = self.client.type_factory(
                'urn:types.core_{}.platform.webservices.netsuite.com'.format(
                    os.getenv("NETSUITE_ENDPOINT")
                )
            )

        self.messages_namespace = self.client.type_factory(
                'urn:messages_{}.platform.webservices.netsuite.com'.format(
                    os.getenv("NETSUITE_ENDPOINT")
                )
            )

        # Create a RecordRef type --> used for almost all calls
        self.RecordRef = self.core_namespace.RecordRef

        # Create the passport and app_info credentials used in all calls
        role = self.RecordRef(internalId=os.getenv("NETSUITE_ROLE"))
        self.passport = self.core_namespace.Passport(email=os.getenv("NETSUITE_EMAIL"),
                        password=os.getenv("NETSUITE_PASSWORD"),
                        account=os.getenv("NETSUITE_ACCOUNT"),
                        role=role)

        self.app_info = self.messages_namespace.ApplicationInfo(applicationId=os.getenv("NETSUITE_APPID"))

        # Create the search preferences for all calls
        self.search_preferences = self.messages_namespace.SearchPreferences(
            bodyFieldsOnly=False,
            returnSearchColumns=False,
            pageSize=100
        )

        # Create the SearchMoreRequest type that will be used for paging by all calls
        self.SearchMoreRequest = self.messages_namespace.SearchMoreRequest

        # Number of failed_login attempts
        self.failed_login_attempts = 0


    def data_center_aware_host_url(self):
        """
        Get the host url for the webservicesDomain used by our Netsuite Account

        Returns the proper host to be used for the NETSUITE_ACCOUNT
        """

        # Let's first try a quick lookup using the REST API
        get_datacenters_url = "https://rest.netsuite.com/rest/datacenterurls?account={}".format(
                                os.getenv("NETSUITE_ACCOUNT")
                              )
        response = requests.get(get_datacenters_url).json()

        try:
            ns_host = response['webservicesDomain']
        except KeyError:
            # If that failed, go the more slow SOAP way
            client = self.netsuite_soap_client(not_data_center_aware=True)

            response = client.service.getDataCenterUrls(os.getenv("NETSUITE_ACCOUNT"))

            if response.body.getDataCenterUrlsResult.status.isSuccess:
                ns_host = response.body.getDataCenterUrlsResult.dataCenterUrls.webservicesDomain
            else:
                # if everything else failed, just use whatever is set for the project
                ns_host = NS_HOST=os.getenv("NETSUITE_HOST")

        return ns_host


    def netsuite_soap_client(self, not_data_center_aware=False):
        if not_data_center_aware:
            ns_host = os.getenv("NETSUITE_HOST")
        else:
            ns_host = self.data_center_aware_host_url()

        # enable cache for a day
        # useful for development and in case of background runners in the future
        cache = SqliteCache(timeout=60*60*24)
        transport = Transport(cache=cache)

        wsdl = "{NS_HOST}/wsdl/v{NS_ENDPOINT}_0/netsuite.wsdl".format(
                    NS_HOST=ns_host,
                    NS_ENDPOINT=os.getenv("NETSUITE_ENDPOINT")
                )
        return Client(wsdl, transport=transport)


    def login(self):
        try:
            login = self.client.service.login(passport=self.passport,
                        _soapheaders={'applicationInfo': self.app_info})

            self.failed_login_attempts = 0

            return login.status
        except Fault as err:
            self.failed_login_attempts += 1

            # Handle concurrent request errors
            if self.failed_login_attempts < 20 \
              and ('exceededConcurrentRequestLimitFault' in err.detail[0].tag \
                   or 'exceededRequestLimitFault' in err.detail[0].tag):
                # NetSuite blocked us due to not allowed concurrent connections
                # Wait for 30 seconds and retry
                logging.info("Login was blocked due to concurrent request limit.")
                logging.info("({}) Sleeping for 30 seconds and trying again.".format(
                                    self.failed_login_attempts)
                )

                time.sleep(30)
                return self.login()

            # Otherwise, report the error and exit
            raise Error("NetSuite login failed: {}".format(err))

    def get_record_by_type(self, type, internal_id):
        """
        Fetch a record given its type as string (e.g. 'account') and its id

        Returns the record or NONE
        """
        record = self.RecordRef(internalId=internal_id, type=type)
        response = self.client.service.get(record,
            _soapheaders={
                'applicationInfo': self.app_info,
                'passport': self.passport,
            }
        )
        r = response.body.readResponse
        if r.status.isSuccess:
            return r.record
        else:
            return None

    def search_incremental(self, search_record):
        """
        Make a simple search request with paging for records of a given type

        The type is provided as a netsuite search type record (e.g. AccountSearch)

        Returns ONLY the records found on the first page of the search results

        The SearchResult envelope is returned (so that status, pageIndex, etc are included)
        """
        result = self.client.service.search(
            searchRecord=search_record,
            _soapheaders={
                'searchPreferences': self.search_preferences,
                'applicationInfo': self.app_info,
                'passport': self.passport,
            }
        )

        return result.body.searchResult

    def search_more(self, searchResult):
        """
        Fetch more search records while doing an incremental search

        Use the search result from an initial search_incremental call
         or a followup search_more call

        The SearchResult envelope is returned (so that status, pageIndex, etc are included)
        """
        result = self.client.service.searchMoreWithId(
            searchId=searchResult.searchId,
            pageIndex=searchResult.pageIndex+1,
            _soapheaders={
                'searchPreferences': self.search_preferences,
                'applicationInfo': self.app_info,
                'passport': self.passport,
            }
        )

        return result.body.searchResult


    def fetch_all_records_for_type(self, search_record):
        """
        Fetch all records of a given type

        The type is provided as a netsuite search type record (e.g. AccountSearch)
        The implementation follows a conservative approach and iterates with a
         small pagesize instead of using the maximum allowed pagesize of 2000

        Returns all the records found as a list
        """
        records = []

        searchResult = self.search_incremental(search_record)

        while searchResult.status.isSuccess:
            records.extend(searchResult.recordList.record)

            if searchResult.pageIndex is None \
              or searchResult.totalPages is None \
              or searchResult.pageIndex >= searchResult.totalPages:
                # NO more pages
                break
            else:
                # There are more pages to be fetched
                searchResult = self.search_more(searchResult)

        return records


    def get_all(self, get_all_record_type):
        """
        Retrieve a list of all records of the specified type.

        Records that support the getAll operation are listed in the GetAllRecordType
        e.g. currency, budgetCategory, state, taxAcct, etc

        The getAllResult envelope is returned so that status and totalRecords are included
        """
        response = self.client.service.getAll(
            record=get_all_record_type,
            _soapheaders={
                'applicationInfo': self.app_info,
                'passport': self.passport,
            }
        )

        return response.body.getAllResult


    def type_factory(self, namespace):
        """
        Helper method for getting a type factory from outside

        Allows consumer classes to write Class.client.type_factory(namespace)
        instead of the ugly Class.client.client.type_factory(namespace)
        """
        return self.client.type_factory(namespace)


    def export_supported_entities(self, only_transactions=False):
        """
        Return a list of initialized objects of main entities

        It is used in the extraction phase in order to dynamically iterate
         over the top level supported entities and extract their data.

        It should be called on an initialized, logged-in NetsuiteClient so that
         all Entities returned are ready to connect and fetch data from the API.

        Support entities are not added in this list as they do not have a
         functional extract operation.
        """
        entities = []

        if only_transactions == False:
            currency = Currency(self)
            entities.append(currency)

            department = Department(self)
            entities.append(department)

            subsidiary = Subsidiary(self)
            entities.append(subsidiary)

            account = Account(self)
            entities.append(account)

        transaction = Transaction(self)
        entities.append(transaction)

        return entities


    def supported_entity_classes():
        """
        Return a list of Classes for all the Entities to be stored in the DW

        It is used in the schema_apply phase in order to dynamically create
         the schema of all supported entities (both top level and support ones)
        """
        entities = [
            Currency,
            Department,
            Subsidiary,
            Account,
            Transaction,
            Expense,
            TransactionItem,
            Application,
            TransactionLine,
        ]

        return entities


    def supported_entity_class_factory(self, class_name):
        """
        Given an Entity's class name return the Entity's Class

        Return None if the Entity / Class name is not supported
        """
        classes = {
            'Currency': Currency,
            'Department': Department,
            'Subsidiary': Subsidiary,
            'Account': Account,
            'Transaction': Transaction,
            'Expense': Expense,
            'TransactionItem': TransactionItem,
            'Application': Application,
            'TransactionLine': TransactionLine,
        }

        if class_name in classes:
            return classes[class_name]
        else:
            return None
