import pytest

from netsuite.src.soap_api.netsuite_soap_client import NetsuiteClient
from netsuite.src.soap_api.currency import Currency
from netsuite.src.soap_api.department import Department


@pytest.fixture(scope="class")
def netsuite_client():
    '''Acquire a NetsuiteClient and fetch the WSDL schema'''
    return NetsuiteClient()


# The Netsuite tests may get stuck for 10+ minutes waiting for a connection
#  when another NetsuiteClient runs at the same time (e.g. a pipeline runs)
# This is due to NetSuite allowing only one concurrent connection
# That's why, even though fully tested, all tests have been marked as skip
@pytest.mark.skipif(True,
                    reason="Test can get stuck for more than 10 minutes")
class TestNetsuiteClient:

    def test_login(self, netsuite_client):
        # Test signing in to Netsuite
        assert netsuite_client.login()


    def test_get_record_by_type(self, netsuite_client):
        # Test get_record_by_type
        result = netsuite_client.get_record_by_type('currency', 1)

        assert result is not None


    def test_get_all(self, netsuite_client):
        # Test get_all() call
        entity = Currency(netsuite_client)
        response = entity.extract_incremental()

        while response is not None and response.status.isSuccess:
            # Transform records
            transform_result = entity.transform(response.recordList.record)
            records = transform_result[0]['data']

            assert len(records) > 0

            for record in records:
                assert 'internal_id' in record
                assert 'name' in record
                assert 'symbol' in record
                assert 'exchange_rate' in record

            response = entity.extract_incremental(searchResult=response)


    def test_search(self, netsuite_client):
        # Test search_incremental and search_more calls
        entity = Department(netsuite_client)

        original_page_size = entity.client.search_preferences['pageSize']
        entity.client.search_preferences['pageSize'] = 10

        response = entity.extract_incremental()

        while response is not None and response.status.isSuccess:
            # Transform records
            transform_result = entity.transform(response.recordList.record)
            records = transform_result[0]['data']

            assert len(records) > 0

            for record in records:
                assert 'internal_id' in record
                assert 'name' in record
                assert 'parent_id' in record
                assert 'parent_name' in record

            response = entity.extract_incremental(searchResult=response)

        entity.client.search_preferences['pageSize'] = original_page_size
