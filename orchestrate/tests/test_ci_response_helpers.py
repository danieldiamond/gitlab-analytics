from orchestrate.ci_response_helpers import *

class TestFailedJobFinder:
    def test_find_existent_failed_job(self):
        pipeline_jobs = [{'id': '1',
                          'status': 'success'},
                          {'id': '2',
                          'status': 'success'},
                          {'id': '3',
                          'status': 'failed'}]
        assert find_failed_job(pipeline_jobs) == '3'

    def test_find_nonexistent_failed_job(self):
        pipeline_jobs = [{'id': '1',
                          'status': 'success'},
                          {'id': '2',
                          'status': 'success'},
                          {'id': '3',
                          'status': 'success'}]
        assert find_failed_job(pipeline_jobs) is None


class TestFindJobByNameAndScope:
    def test_find_one(self):
        scope = ['running']
        project_jobs = [{'id': '1',
                         'name': 'scheduler',
                         'status': 'running'},
                        {'id': '2',
                         'name': 'something',
                         'status': 'success'},
                        {'id': '3',
                         'name': 'scheduler',
                         'status': 'success'}]
        matching_job_list = find_job_by_name_and_scope(project_jobs,
                                                       'scheduler',
                                                       scope)
        assert matching_job_list == ['1']


    def test_find_none(self):
        scope = ['pending']
        project_jobs = [{'id': '1',
                         'name': 'scheduler',
                         'status': 'canceled'},
                        {'id': '2',
                         'name': 'something',
                         'status': 'success'},
                        {'id': '3',
                         'name': 'scheduler',
                         'status': 'success'}]
        matching_job_list = find_job_by_name_and_scope(project_jobs,
                                                       'scheduler',
                                                       scope)
        assert matching_job_list == []


    def test_find_none(self):
        scope = ['running', 'pending']
        project_jobs = [{'id': '1',
                         'name': 'scheduler',
                         'status': 'canceled'},
                        {'id': '2',
                         'name': 'scheduler',
                         'status': 'pending'},
                        {'id': '3',
                         'name': 'scheduler',
                         'status': 'running'}]
        matching_job_list = find_job_by_name_and_scope(project_jobs,
                                                       'scheduler',
                                                       scope)
        assert matching_job_list == ['2', '3']
