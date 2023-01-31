import os
import pathlib
import sys
import tempfile
import unittest.mock as mock

from db_handler import DBHandler
from detect_jira_format_issue_failures import main

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_get_fp_in_uploads(dummyself, date_created_after):
    if date_created_after in ('2019-12-29', '2020-01-29'):
        return {'5465': {'upload_token': 'a', 'uploader_name': 'AMP',
                         'site_id': 'US-AMP', 'upload_date': '2019-12-30'},
                '5466': {'upload_token': 'a', 'uploader_name': 'AMP',
                         'site_id': 'US-AMP', 'upload_date': '2019-12-30'},
                '5467': {'upload_token': 'b', 'uploader_name': 'blue',
                         'site_id': 'US-III', 'upload_date': '2019-12-31'},
                '5468': {'upload_token': 'c', 'uploader_name': 'cat',
                         'site_id': 'US-YYY', 'upload_date': '2019-12-31'},
                '5469': {'upload_token': 'c', 'uploader_name': 'cat',
                         'site_id': 'US-YYY', 'upload_date': '2019-12-31'}}
    if date_created_after in ('2020-03-12', '2020-04-07'):
        return {'5465': {'upload_token': 'a', 'uploader_name': 'AMP',
                         'site_id': 'US-AMP', 'upload_date': '2019-12-30'},
                '5466': {'upload_token': 'a', 'uploader_name': 'AMP',
                         'site_id': 'US-AMP', 'upload_date': '2019-12-30'},
                '5467': {'upload_token': 'b', 'uploader_name': 'blue',
                         'site_id': 'US-III', 'upload_date': '2019-12-31'},
                '5468': {'upload_token': 'c', 'uploader_name': 'cat',
                         'site_id': 'US-YYY', 'upload_date': '2019-12-31'}}


def mock_get_process_ids_from_jira_format_issues(
        dummyself, date_created_after, jira_project):
    if date_created_after in ('2019-12-29', '2020-03-12'):
        return ['1234', '1235', '1236', '1237', '1238']
    if date_created_after in ('2020-01-29', '2020-04-07'):
        return ['1234', '1235', '1236']


def mock_get_format_qaqc_process_attempts(dummyself, date_created_after):
    if date_created_after in ('2019-12-29', '2020-01-29'):
        return {'1234': {'upload_id': '5465'},
                '1235': {'upload_id': '5466'},
                '1236': {'upload_id': '5467'},
                '1237': {'upload_id': '5468'},
                '1238': {'upload_id': '5469'}}
    if date_created_after == '2020-03-12':
        return {'1234': {'upload_id': '5465'},
                '1235': {'upload_id': '5466'},
                '1236': {'upload_id': '5467'},
                '1237': {'upload_id': '5468'},
                '1238': {'upload_id': '5468'}}
    if date_created_after == '2020-04-07':
        return {'1234': {'upload_id': '5465'},
                '1235': {'upload_id': '5466'},
                '1236': {'upload_id': '5467'}}


def run_main_with_monkeypatch(monkeypatch, sys_args):
    with mock.patch.object(sys, 'argv', sys_args):
        monkeypatch.setattr(DBHandler, 'get_fp_in_uploads',
                            mock_get_fp_in_uploads)
        monkeypatch.setattr(DBHandler,
                            'get_process_ids_from_jira_format_issues',
                            mock_get_process_ids_from_jira_format_issues)
        monkeypatch.setattr(DBHandler, 'get_format_qaqc_process_attempts',
                            mock_get_format_qaqc_process_attempts)
        main()


def test_main_failures():
    test_dir_path = pathlib.Path(__file__).parent.absolute()
    resource_path = os.path.join(test_dir_path, 'resources')
    cfg_path1 = os.path.join(resource_path, 'detect_jira_config1.cfg')
    cfg_path2 = os.path.join(resource_path, 'detect_jira_config2.cfg')

    log_name = 'qaqc_process_check_log_{t}.log'

    with tempfile.TemporaryDirectory() as temp_wd:

        test_date = '2020-01-01'
        log_path = os.path.join(temp_wd, log_name.format(t=test_date))

        # Wrong log path
        sys_args = ['prog', '/fake/cfg.cfg', temp_wd, '-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == 'ERROR: Config file path not found.'

        # Missing JIRA section in config
        sys_args = ['prog', cfg_path1, temp_wd, '-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == 'ERROR: JIRA configuration failed.'

        # Missing DB section in config
        sys_args = ['prog', cfg_path2, temp_wd, '-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == 'ERROR: DB configuration failed.'


def test_main_success(monkeypatch):
    test_dir_path = pathlib.Path(__file__).parent.absolute()
    resource_path = os.path.join(test_dir_path, 'resources')
    cfg_path = os.path.join(resource_path, 'detect_jira_config.cfg')

    log_name = 'qaqc_process_check_log_{t}.log'

    with tempfile.TemporaryDirectory() as temp_wd:
        args = ['prog', cfg_path, temp_wd]

        # All good: 2020-01-01, query_date = 2019-12-29
        test_date = '2020-01-01'
        log_path = os.path.join(temp_wd, log_name.format(t=test_date))
        sys_args = args + ['-c', test_date, '-t']
        run_main_with_monkeypatch(monkeypatch, sys_args)
        with open(log_path, 'r') as log_file:
            assert log_file.read() == (
                'Super! All uploads processed. All process '
                'attempts reported in a JIRA issue.')

        # Missing JIRA issues: 2020-02-01, query_date = 2020-01-29
        test_date = '2020-02-01'
        log_path = os.path.join(temp_wd, log_name.format(t=test_date))
        sys_args = args + ['-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            monkeypatch.setattr(DBHandler, 'get_fp_in_uploads',
                                mock_get_fp_in_uploads)
            monkeypatch.setattr(DBHandler,
                                'get_process_ids_from_jira_format_issues',
                                mock_get_process_ids_from_jira_format_issues)
            monkeypatch.setattr(DBHandler, 'get_format_qaqc_process_attempts',
                                mock_get_format_qaqc_process_attempts)
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == (
                'ERROR: 1 upload(s) '
                'processed through Format QA/QC '
                'but missing JIRA Format issues\n'
                '***********************************\n'
                'Upload token c with process_id(s): 1237, 1238\n')

        # More than one process_id for an upload_id: 2020-03-15,
        #    query_date = 2020-03-12
        test_date = '2020-03-15'
        log_path = os.path.join(temp_wd, log_name.format(t=test_date))
        sys_args = args + ['-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            monkeypatch.setattr(DBHandler, 'get_fp_in_uploads',
                                mock_get_fp_in_uploads)
            monkeypatch.setattr(DBHandler,
                                'get_process_ids_from_jira_format_issues',
                                mock_get_process_ids_from_jira_format_issues)
            monkeypatch.setattr(DBHandler, 'get_format_qaqc_process_attempts',
                                mock_get_format_qaqc_process_attempts)
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == (
                'Super! All uploads processed. All process '
                'attempts reported in a JIRA issue.')

        # Missing process_ids: 2020-04-10, query_date = 2020-04-07
        test_date = '2020-04-10'
        log_path = os.path.join(temp_wd, log_name.format(t=test_date))
        sys_args = args + ['-c', test_date, '-t']
        with mock.patch.object(sys, 'argv', sys_args):
            monkeypatch.setattr(DBHandler, 'get_fp_in_uploads',
                                mock_get_fp_in_uploads)
            monkeypatch.setattr(DBHandler,
                                'get_process_ids_from_jira_format_issues',
                                mock_get_process_ids_from_jira_format_issues)
            monkeypatch.setattr(DBHandler, 'get_format_qaqc_process_attempts',
                                mock_get_format_qaqc_process_attempts)
            main()
        with open(log_path, 'r') as log_file:
            assert log_file.read() == (
                'ERROR: 1 upload(s) '
                'not processed through Format QA/QC\n'
                '***********************************\n'
                'Upload id 5468: US-YYY on 2019-12-31 by cat\n'
                '***********************************\n'
                '***********************************\n\n')
