import os
import pickle
import pytest

from datetime import datetime as dt

from data_qaqc_auto_run import DataQAQCAutoRunHandler
from jira_names import JIRANames, JIRATimestamp

__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'


TESTDATA_DIR = os.path.join(
        'test', 'testdata', 'data_qaqc_auto_run')


@pytest.fixture
def handler(monkeypatch):
    return DataQAQCAutoRunHandler()


@pytest.mark.parametrize('ts_jira, ts_format, expected_result',
                         [('2025-09-11T11:19:09.000-0700',
                           JIRATimestamp.jira_dt_api,
                           '2025-09-11T11:19:09.000000-07:00'),
                          ('20250911111909.000',
                           JIRATimestamp.jira_dt_api, None)],
                         ids=['api_ex', 'error'])
def test_translate_date_str_to_datetime(
        ts_jira, ts_format, expected_result, handler):
    if expected_result:
        translated_ts = handler.translate_date_str_to_datetime(
            ts_jira, ts_format)
        assert translated_ts == dt.fromisoformat(expected_result)
    else:
        with pytest.raises(Exception):
            handler.translate_date_str_to_datetime(ts_jira, ts_format)


# Cases:
# No issues in lookback (all issues older) - created
# all issue in lookback (all issues younger) - updated
# invalid field name
# invalid issue type
# half issues younger - format
@pytest.mark.parametrize(
        'lookback_str, test_issue_file, issue_filter, '
        'issue_type, expected_result',
        [('2025-09-20T01:00:00.324578', 'qaqc_issues1.pickle',
         JIRANames.issue_created, None, ['YY-123'])],
        ids=['no_issues'])
def test_find_issues_in_lookback(
        lookback_str, test_issue_file, issue_filter,
        issue_type, expected_result, handler):
    lookback_dt = dt.fromisoformat(lookback_str)

    with open(os.path.join(TESTDATA_DIR, test_issue_file), 'rb') as f:
        jira_test_issues = pickle.load(f)

    result = handler.find_issues_in_lookback(
        issue_filter, jira_test_issues, lookback_dt, issue_type)

    assert result == expected_result


@pytest.mark.parametrize(
    'upload_case, lookback_str, expected_result',
    [('case3', '2025-09-20T01:00:00.324578', {})],
    ids=['perfect_match'])
def test_find_process_run_jira_issue_mismatches(
        upload_case, lookback_str, expected_result, handler):

    with open(os.path.join(TESTDATA_DIR,
                           'mismatches_cases.pickle'), 'rb') as f:
        cases = pickle.load(f)

    sites_with_last_upload = cases[upload_case]
    site_ids_with_last_upload = list(sites_with_last_upload.keys())

    with open(os.path.join(TESTDATA_DIR,
                           'qaqc_issues1.pickle'), 'rb') as f:
        qaqc_issues = pickle.load(f)

    lookback_dt = dt.fromisoformat(lookback_str)

    result = handler.find_process_run_jira_issue_mismatches(
        lookback_dt, site_ids_with_last_upload, qaqc_issues,
        sites_with_last_upload)

    assert result == expected_result


def test_check_all_jira_statuses_resolved():
    pass


def test_assess_qaqc_jira_issues():
    pass


def test_extract_full_record_dates():
    pass


@pytest.mark.parametrize(
    'subissues, expected_result',
    [({'TEST-A': {'customfield_10800': '2018-2019',
                  'status': {'name': 'Discovered'}},
       'TEST-B': {'customfield_10800': 'All',
                  'status': {'name': 'Discovered'}}},
      [2010, 2011, 2012, 2013, 2014, 2015, 2016]),
     ({'TEST-A': {'customfield_10800': '2018,2019,2021',
                  'status': {'name': 'Discovered'}},
       'TEST-B': {'customfield_10800': '2016',
                  'status': {'name': 'Discovered'}}},
      [2018, 2019, 2021, 2016])
     ],
    ids=['mix', 'mix-skip_issue'])
def test_get_years_from_sub_issues(
        subissues, expected_result, monkeypatch, handler):

    def mock_adjust_date_year(
            dummystatic, dummystartdate, dummyenddate):
        return 2010, 2016

    monkeypatch.setattr(DataQAQCAutoRunHandler,
                        'adjust_date_year', mock_adjust_date_year)

    result = handler.get_years_from_sub_issues(
        subissues,
        (dt.fromisoformat('2010-01-01T00:00:00.000'),
         dt.fromisoformat('2016-12-31T23:30:00.000')),)

    assert result == expected_result


def test_get_years_from_uploads():
    pass


def test_get_recent_format_issues():
    pass


def test_assess_sub_issues():
    pass


def test_get_potential_res():
    pass
