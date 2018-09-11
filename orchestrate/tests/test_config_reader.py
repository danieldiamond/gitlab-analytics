from config_reader import *

class TestFormatJobVars:
    def test_single_format_job_vars(self):
        raw_vars = {'Spreadsheet': 'true'}
        formatted_vars = format_job_vars(raw_vars)
        correct_vars = [{'key': 'Spreadsheet',
                         'value': 'true'},
                        {'key': 'ORCHESTRATE_JOB',
                         'value': 'true'}]

        assert formatted_vars == correct_vars


    def test_multiple_format_job_vars(self):
        raw_vars = {'Spreadsheet': 'true',
                    'DBT': 'true'}
        formatted_vars = format_job_vars(raw_vars)
        correct_vars = [{'key': 'Spreadsheet',
                         'value': 'true'},
                        {'key': 'DBT',
                         'value': 'true'},
                        {'key': 'ORCHESTRATE_JOB',
                         'value': 'true'}]

        assert formatted_vars == correct_vars
