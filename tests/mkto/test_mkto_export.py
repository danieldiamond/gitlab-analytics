import pytest

from mkto.mkto_tools.mkto_leads import describe_schema as describe_leads_schema
from mkto.mkto_tools.mkto_activities import describe_schema as describe_activities_schema
from mkto.mkto_tools.mkto_token import get_token
from mkto.mkto_tools.mkto_bulk import bulk_get_export_jobs, bulk_job_status

@pytest.fixture(scope="class")
def mkto_args():
    class Args: pass
    Args.schema = 'mkto'
    Args.source = None
    Args.table_name= None

    return Args

class TestMarketo:
    def test_describe_leads_schema(self, mkto_args):
        # Test fetching the leads schema from marketo
        mkto_args.source = 'leads'

        schema = describe_leads_schema(mkto_args)

        assert schema.name == 'mkto'

        assert 'leads' in schema.tables

        # Add your assertions about columns expected to be found in mkto.leads
        # Adding some random ones as examples
        assert ('leads', 'id') in schema.columns
        assert ('leads', 'mktoname') in schema.columns
        assert ('leads', 'leadperson') in schema.columns
        assert ('leads', 'leadrole') in schema.columns


    def test_describe_activities_schema(self, mkto_args):
        # Test fetching the activities schema from marketo
        mkto_args.source = 'activities'

        schema = describe_activities_schema(mkto_args)

        assert schema.name == 'mkto'

        assert 'activities' in schema.tables

        # Add assertions about columns expected to be found in mkto.activities
        # Adding some random ones as examples
        assert ('activities', 'leadid') in schema.columns
        assert ('activities', 'activitytypeid') in schema.columns
        assert ('activities', 'campaignid') in schema.columns
        assert ('activities', 'activitydate') in schema.columns


    def test_get_access_token(self):
        # Test fetching an access token
        token = get_token()

        # Simple assertion that a token was returned
        assert token


    def test_bulk_get_export_jobs_and_status(self):
        # Test getting past jobs and their details

        # Fetch past Lead Jobs
        lead_jobs = bulk_get_export_jobs('leads')

        if len(lead_jobs['result']) > 0:
            # Required check in case bulk jobs for fetching leads
            #  have not run in the past couple of days
            latest_lead_job = lead_jobs['result'][0]
            status = bulk_job_status('leads', latest_lead_job['exportId'])
            assert len(status['result']) == 1

        # Fetch past Activity Jobs
        activity_jobs = bulk_get_export_jobs('activities')

        if len(activity_jobs['result']) > 0:
            # Required check in case bulk jobs for fetching activities
            #  have not run in the past couple of days
            latest_activity_job = activity_jobs['result'][0]
            status = bulk_job_status('activities', latest_activity_job['exportId'])
            assert len(status['result']) == 1
