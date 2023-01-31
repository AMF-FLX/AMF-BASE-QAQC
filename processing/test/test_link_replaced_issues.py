import datetime as dt
import json
import os
import pytest

from db_handler import DBHandler
from link_replaced_issues import LinkIssues
from utils import TimestampUtil

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


@pytest.fixture
def link_issues():
    return LinkIssues()


def dt_dt(timestamp):
    ts_fmt = TimestampUtil().JIRA_TS_FORMAT
    return dt.datetime.strptime(timestamp, ts_fmt).date()


def load_upload_data(upload_token):
    # assume ~/AMF-QAQC/processing is the cwd
    resource_path = './test/resources/'
    resource_files = os.listdir(resource_path)
    upload_token_filename = None
    for filename in resource_files:
        if upload_token in filename:
            upload_token_filename = filename
    if not upload_token_filename:
        return dict()
    with open(os.path.join(resource_path, upload_token_filename)) as f:
        return json.load(f)


def mock_get_uploaded_files_no_match(dummyself, site_id):
    if site_id == 'US-PFa':
        return [{'process_id': '14696'}]
    if site_id == 'US-Ton':
        return [{'process_id': '8052',
                 'data_file': 'Tonzi-understory-2016.dat',
                 'upload_token': 'token6'},
                {'process_id': '1234', 'start_time': None, 'end_time': None,
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'Tonzi-understory-2017.dat',
                 'upload_token': 'alongtokenstring'}]


def mock_get_uploaded_files(dummyself, site_id):
    if site_id == 'US-Ton':
        return [{'process_id': '8052',
                 'data_file': 'Tonzi-understory-2016.dat',
                 'upload_token': 'token6'},
                {'process_id': '1234', 'start_time': None, 'end_time': None,
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'Tonzi-understory-2016.dat',
                 'upload_token': 'alongtokenstring'}]
    if site_id == 'US-PFa':
        return [{'process_id': '14696',
                 'data_file': '',
                 'upload_token': 'token1'},
                {'process_id': '1234',
                 'start_time': '2018-01-01 00:00:00.000',
                 'end_time': '2019-01-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_201801010000_201901010000.csv',
                 'upload_token': 'alongtokenstring'}]


def mock_get_uploaded_files_multiples(dummyself, site_id):
    if site_id == 'US-PFa':
        return [{'process_id': '14696',
                 'data_file': '',
                 'upload_token': 'token1'},
                {'process_id': '1234',
                 'start_time': '2018-01-01 00:00:00.000',
                 'end_time': '2018-06-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_201801010000_201806010000.csv',
                 'upload_token': 'alongtokenstring'},
                {'process_id': '3456',
                 'start_time': '2019-01-01 00:00:00.000',
                 'end_time': '2020-01-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_201901010000_202001010000.csv',
                 'upload_token': 'notlinkeduploadtoken'},
                {'process_id': '4567',
                 'start_time': '2018-06-01 00:00:00.000',
                 'end_time': '2019-01-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_201806010000_201901010000.csv',
                 'upload_token': 'anotherlongtokenstring'}]


def mock_get_uploaded_files_multiple_processes(dummyself, site_id):
    if site_id == 'US-PFa':
        return [{'process_id': '13278',
                 'data_file': '',
                 'upload_token': 'token2'},
                {'process_id': '13279',
                 'data_file': '',
                 'upload_token': 'token2'},
                {'process_id': '1234',
                 'start_time': '1995-01-01 00:00:00.000',
                 'end_time': '1996-01-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_199501010000_199601010000.csv',
                 'upload_token': 'alongtokenstring'},
                {'process_id': '4567',
                 'start_time': '1996-01-01 00:00:00.000',
                 'end_time': '1997-01-01 00:00:00.000',
                 'autocorrected_start_time': None,
                 'autocorrected_end_time': None,
                 'data_file': 'US-PFa_HR_199601010000_199701010000.csv',
                 'upload_token': 'anotherlongtokenstring'}]


def mock_no_fuzzy_match_filename_to_uploaded_files(dummyvar, dummyvar2):
    return None, dict()


def mock_valid_fuzzy_match_filename_to_uploaded_files(dummyvar, dummyvar2):
    return '7890', {'upload_token': 'dummytoken'}


def mock_get_format_issues(dummyself, jira_project, site_id):
    if site_id == 'US-Ton':
        return {'1234': dict()}
    return {'1234': dict(), '4567': dict()}


# test get_jira_format_issues_for_site
def test_get_jira_format_issues_for_site(monkeypatch):
    link_issue = LinkIssues()
    link_issue.jira_db_handler = DBHandler(None, None, None, None)
    monkeypatch.setattr(DBHandler, 'get_format_issues', mock_get_format_issues)
    # mock db_handler.get_format_issues with missing new_report_key
    assert link_issue.get_jira_format_issues_for_site(
        'US-Ton', '4567') == (dict(), None)
    # mock db_handler.get_format_issues as normal
    assert link_issue.get_jira_format_issues_for_site(
        'US-PFa', '4567') == ({'1234': dict()}, dict())


# test fuzzy filename
@pytest.mark.parametrize(
    'filename, candidate_filename, filename_has_ext, expected_result',
    [
     # MATCH: same name - FP-In standard
     ('US-PFa_HR_199501010000_199601010000.csv',
      'US-PFa_HR_199501010000_199601010000-2021010123314500.csv', True, True),
     # MATCH: same start date - FP-In standard
     ('US-PFa_HR_199501010000_199601010000.csv',
      'US-PFa_HR_199501010000_199701010000-2021010123314500.csv', True, True),
     # NO MATCH: start date in between current file -- FP-In standard
     ('US-PFa_HR_199501010000_199601010000.csv',
      'US-PFa_HR_199506010000_199701010000-2021010123314500.csv', True, False),
     # NO MATCH: completely different dates -- FP-Standard
     ('US-PFa_HR_199501010000_199601010000.csv',
      'US-PFa_HR_199601010000_199701010000-2021010123314500.csv', True, False),
     # MATCH: same name - non standard name
     ('US-Ton_2018.csv', 'US-Ton_2018-2021010123314500.csv', True, True),
     # NO MATCH: not same
     ('US-PFa_2018.csv',
      'US-PFa_HR_201801010000_201901010000-2021010123314500.csv', True, False),
     # MATCH: same name - FP-In standard; no ext
     ('US-PFa_HR_199501010000_199601010000',
      'US-PFa_HR_199501010000_199601010000-2021010123314500.csv', False, True),
    ],
    ids=['MATCH: same name - FP-In standard',
         'MATCH: same start date - FP-In standard',
         'NO MATCH: start date in between current file -- FP-In standard',
         'NO MATCH: completely different dates -- FP-Standard',
         'MATCH: same name - non standard name',
         'NO MATCH: not same', 'MATCH: same name - FP-In standard; no ext'
         ])
def test_fuzzy_match_filename(link_issues, filename, candidate_filename,
                              filename_has_ext, expected_result):
    assert link_issues._fuzzy_match_filename(
        filename, candidate_filename, filename_has_ext) is expected_result


# test fuzzy filename from uploads
DICT_MATCH_RESULT = {
    'process_id': 4567, 'data_file':
    'US-PFa_HR_199501010000_199601010000-2021010123314500.csv'}


@pytest.mark.parametrize(
    'filename, uploaded_files, expected_result',
    [
     # only current upload
     ('US-PFa_HR_199501010000_199601010000.csv',
      [{'process_id': 9999}], (None, dict())),
     # no match: no match in uploads
     ('US-PFa_HR_199501010000_199601010000.csv',
      [{'process_id': 9999},
       {'process_id': 1234, 'data_file':
           'US-PFa_HR_200001010000_200101010000-2021010123314500.csv'}],
      (None, dict())),
     # return most recent: single direct match in uploads
     ('US-PFa_HR_199501010000_199601010000.csv',
      [{'process_id': 9999},
       {'process_id': 1234, 'data_file':
          'US-PFa_HR_200001010000_200101010000-2021010123314500.csv'},
       DICT_MATCH_RESULT],
      (4567, DICT_MATCH_RESULT)),
     # Return most recent: multiple matches in uploads
     ('US-PFa_HR_199501010000_199601010000.csv',
      [{'process_id': 9999},
       {'process_id': 1234, 'data_file':
          'US-PFa_HR_200001010000_200101010000-2021010123314500.csv'},
       DICT_MATCH_RESULT,
       {'process_id': 6789, 'data_file':
          'US-PFa_HR_199501010000_199601010000-2021010123314500.csv'}],
      (4567, DICT_MATCH_RESULT)),
    ],
    ids=['only current upload', 'no match: no match in uploads',
         'Return most recent: single direct match in uploads',
         'Return most recent: multiple matches in uploads'])
def test_fuzzy_match_filename_to_uploaded_files(
        link_issues, filename, uploaded_files, expected_result):
    assert link_issues._fuzzy_match_filename_to_uploaded_files(
        filename, uploaded_files, process_id=9999) == expected_result


# test assess_candidate_file
def test_assess_candidate_uploaded_file(link_issues):
    candidate_upload_token = 'alongtokenstring8765'
    process_file = 'US-PFa_HR_199501010000_199601010000.csv'
    process_date_range = ('19950101', '19960101')
    linked_files = dict()
    candidate_upload_info = {
        'start_time': None, 'end_time': None,
        'autocorrected_start_time': None, 'autocorrected_end_time': None,
        'process_id': 1234,
        'data_file': 'US-PFa_HR_199501010000_199601010000-2021010123314500.csv'
    }
    # upload_token, True: No original or autocorrected date, fuzzy match
    assert link_issues.assess_candidate_uploaded_file(
        candidate_upload_token, candidate_upload_info, process_file,
        linked_files, process_date_range) == (candidate_upload_token, True)
    candidate_upload_info['data_file'] = \
        'US-PFa_HR_199601010000_199701010000-2021010123314500.csv'
    linked_files = dict()
    # None, False: No original or autocorrected date, no fuzzy match
    assert link_issues.assess_candidate_uploaded_file(
        candidate_upload_token, candidate_upload_info, process_file,
        linked_files, process_date_range) == (None, False)
    candidate_upload_info['autocorrected_start_time'] = \
        '1995-01-01 00:00:00.000'
    candidate_upload_info['autocorrected_end_time'] = '1995-06-01 00:00:00.000'
    candidate_upload_info['data_file'] = \
        'US-PFa_HR_199501010000_199501060000-2021010123314500.csv'
    linked_files = dict()
    # upload_token, False:
    #     partial overlap
    assert link_issues.assess_candidate_uploaded_file(
        candidate_upload_token, candidate_upload_info, process_file,
        linked_files, process_date_range) == (candidate_upload_token, False)
    # upload_token, True: full overlap
    candidate_upload_info['start_time'] = '1995-01-01 00:00:00.000'
    candidate_upload_info['end_time'] = '1996-01-01 00:00:00.000'
    candidate_upload_info['autocorrected_end_time'] = '1996-01-01 00:00:00.000'
    candidate_upload_info['data_file'] = \
        'US-PFa_HR_199501010000_199601010000-2021010123314500.csv'
    linked_files = dict()
    assert link_issues.assess_candidate_uploaded_file(
        candidate_upload_token, candidate_upload_info, process_file,
        linked_files, process_date_range) == (candidate_upload_token, True)
    # None, False: no overlap
    candidate_upload_info['start_time'] = '1996-01-01 00:00:00.000'
    candidate_upload_info['end_time'] = '1997-01-01 00:00:00.000'
    candidate_upload_info['autocorrected_start_time'] = \
        '1996-01-01 00:00:00.000'
    candidate_upload_info['autocorrected_end_time'] = '1997-01-01 00:00:00.000'
    candidate_upload_info['data_file'] = \
        'US-PFa_HR_199601010000_199701010000-2021010123314500.csv'
    linked_files = dict()
    assert link_issues.assess_candidate_uploaded_file(
        candidate_upload_token, candidate_upload_info, process_file,
        linked_files, process_date_range) == (None, False)


# test convert_str_to_datetime
def test_convert_str_to_datetime(link_issues):
    def dt_date(value):
        return dt.datetime.strptime(value, '%Y%m%d').date()
    assert link_issues.convert_str_to_datetime(
        timestamp_str=(PROCESS_START, PROCESS_END)) == (dt_date(PROCESS_START),
                                                        dt_date(PROCESS_END))
    assert link_issues.convert_str_to_datetime(
        timestamp_str=[PROCESS_START, PROCESS_END]) == [dt_date(PROCESS_START),
                                                        dt_date(PROCESS_END)]
    assert link_issues.convert_str_to_datetime(
        timestamp_str=[PROCESS_START, PROCESS_END]) == [dt_date(PROCESS_START),
                                                        dt_date(PROCESS_END)]


def test_make_dates_series(link_issues):
    date_range = ['2020-12-29 00:00:00.000', '2020-12-31 23:30:00.000']
    results = [dt_dt('2020-12-29 00:00:00.000'),
               dt_dt('2020-12-30 00:00:00.000'),
               dt_dt('2020-12-31 00:00:00.000')]
    assert link_issues.make_dates_series(date_range) == (
        [results[0], results[-1]], set(results))
    # trim dates are always process dates which are dates not timestamps
    trim_date_range = ['20201229', '20201230']
    assert link_issues.make_dates_series(date_range, trim_date_range) == (
        results[0:2], set(results[0:2]))
    assert link_issues.make_dates_series(
        tuple(date_range), tuple(trim_date_range)) == (results[0:2],
                                                       set(results[0:2]))


# detect gaps
PROCESS_START = '20190101'
PROCESS_END = '20201231'


# test check_replacement_date_coverage
@pytest.mark.parametrize(
    'files_replaced, candidate_date_range, expected_results',
    # same dates
    [({},
      ('2019-01-01 00:00:00.000', '2020-12-31 00:00:00.000'),
      ('1234', True)),
     # same dates; start < 1
     ({},
      ('2018-12-31 00:00:00.000', '2020-12-31 23:30:00.000'), ('1234', True)),
     # same dates; end < 1
     ({},
      ('2019-01-01 00:00:00.000', '2020-12-30 23:30:00.000'), ('1234', True)),
     # fill single gap at end
     ({'7890': {'start_time': '2019-01-01 00:00:00.000',
                'end_time': '2019-06-01 00:00:00.000'}},
      ('2019-06-01 00:00:00.000', '2020-12-30 23:30:00.000'), ('1234', True)),
     # fill single gap at start
     ({'7890': {'start_time': '2019-07-01 00:00:00.000',
                'end_time': '2021-01-01 00:00:00.000'}},
      ('2019-01-01 00:00:00.000', '2020-06-30 23:30:00.000'), ('1234', True)),
     # fill single gap in middle
     ({'7890': {'start_time': '2019-01-01 00:00:00.000',
                'end_time': '2019-03-01 00:00:00.000'},
       '7891': {'start_time': '2019-09-01 00:00:00.000',
                'end_time': '2021-01-01 00:00:00.000'}},
      ('2019-01-01 00:00:00.000', '2020-12-31 23:30:00.000'), ('1234', True)),
     # partial fill single gap in middle
     ({'7890': {'start_time': '2019-01-01 00:00:00.000',
                'end_time': '2019-03-01 00:00:00.000'},
       '7891': {'start_time': '2020-01-01 00:00:00.000',
                'end_time': '2021-01-01 00:00:00.000'}},
      ('2019-03-01 00:00:00.000', '2019-06-01 00:00:00.000'), ('1234', False)),
     # gaps at start and end; fill start
     ({'7890': {'start_time': '2019-06-01 00:00:00.000',
                'end_time': '2019-09-01 00:00:00.000'}},
      ('2019-01-01 00:00:00.000', '2019-06-01 00:00:00.000'), ('1234', False)),
     # gap at start; no overlap
     ({'7890': {'start_time': '2019-07-01 00:00:00.000',
                'end_time': '2021-01-01 00:00:00.000'}},
      ('2018-01-01 00:00:00.000', '2018-12-31 23:30:00.000'), (None, False)),
     # gap at end; no overlap
     ({'7890': {'start_time': '2019-01-01 00:00:00.000',
                'end_time': '2019-06-01 00:00:00.000'}},
      ('2018-01-01 00:00:00.000', '2018-12-31 23:30:00.000'), (None, False)),
     # gap in middle; no overlap
     ({'7890': {'start_time': '2019-01-01 00:00:00.000',
                'end_time': '2019-03-01 00:00:00.000'},
       '7891': {'start_time': '2019-09-01 00:00:00.000',
                'end_time': '2021-01-01 00:00:00.000'}},
      ('2021-01-01 00:00:00.000', '2021-12-31 23:30:00.000'), (None, False)),
     # gap at start and end; no overlap
     ({'7890': {'start_time': '2019-06-01 00:00:00.000',
                'end_time': '2019-09-01 00:00:00.000'}},
      ('2021-01-01 00:00:00.000', '2021-12-31 23:30:00.000'), (None, False)),
     ],
    ids=['same dates', 'same dates; start < 1', 'same dates; end < 1',
         'fill single gap at end', 'fill single gap at start',
         'fill single gap in middle', 'partial fill single gap in middle',
         'gaps at start and end; fill start', 'gap at start; no overlap',
         'gap at end; no overlap', 'gap in middle; no overlap',
         'gap at start and end; no overlap'])
def test_check_replacement_date_coverage(
        link_issues, files_replaced, candidate_date_range, expected_results):
    candidate_file_info = {'process_id': '1234'}
    results = link_issues.check_replacement_date_coverage(
        process_date_range=(PROCESS_START, PROCESS_END),
        files_replaced=files_replaced,
        candidate_date_range=candidate_date_range,
        candidate_file_info=candidate_file_info)
    assert results == expected_results


# test find_files
def test_find_files_replaced_by_upload(monkeypatch):
    monkeypatch.setattr(DBHandler, 'get_upload_file_info_for_site',
                        mock_get_uploaded_files_no_match)
    link_issue = LinkIssues()
    link_issue.qaqc_db_handler = DBHandler(None, None, None, None)
    # process_info not updated, no upload_tokens:
    #    single uploaded_file that has same process run
    upload_token = 'token1'
    upload_info = load_upload_data(upload_token)
    assert link_issue.find_files_replaced_by_upload(
        upload_token, upload_info) == ([], [])
    assert 'files_replaced' not in upload_info['reports']['14696'].keys()
    # process_info not updated, no upload_tokens:
    #    process run with no date; no fuzzy match
    upload_token = 'token6'
    upload_info = load_upload_data(upload_token)
    uploaded_files = mock_get_uploaded_files_no_match('', 'US-Ton')
    assert link_issue.find_files_replaced_by_upload(
        upload_token, upload_info) == (uploaded_files, [])
    assert upload_info['reports']['8052']['files_replaced'] == {}
    # process_info updated, upload_token: process run with no date; fuzzy match
    monkeypatch.setattr(DBHandler, 'get_upload_file_info_for_site',
                        mock_get_uploaded_files)
    upload_token = 'token6'
    upload_info = load_upload_data(upload_token)
    uploaded_files = mock_get_uploaded_files('', 'US-Ton')
    assert link_issue.find_files_replaced_by_upload(
        upload_token, upload_info) == (uploaded_files, ['alongtokenstring'])
    assert upload_info['reports']['8052']['files_replaced'] == {
        '1234': uploaded_files[1]}
    # process_info updated, upload_token:
    #    mock finding candidate with full coverage
    upload_token = 'token1'
    upload_info = load_upload_data(upload_token)
    uploaded_files = mock_get_uploaded_files('', 'US-PFa')
    assert link_issue.find_files_replaced_by_upload(
        upload_token, upload_info) == (uploaded_files, ['alongtokenstring'])
    assert upload_info['reports']['14696']['files_replaced'] == {
        '1234': uploaded_files[1]}
    # process_info updated, 2 upload_tokens:
    #    mock finding 2 candidates to get full coverage
    monkeypatch.setattr(DBHandler, 'get_upload_file_info_for_site',
                        mock_get_uploaded_files_multiples)
    upload_token = 'token1'
    upload_info = load_upload_data(upload_token)
    uploaded_files = mock_get_uploaded_files_multiples('', 'US-PFa')
    result1, result2 = link_issue.find_files_replaced_by_upload(
        upload_token, upload_info)
    assert result1 == uploaded_files
    result2.sort()
    assert result2 == ['alongtokenstring', 'anotherlongtokenstring']
    assert upload_info['reports']['14696']['files_replaced'] == {
        '1234': uploaded_files[1], '4567': uploaded_files[3]}
    # multiple process runs per upload_token
    monkeypatch.setattr(DBHandler, 'get_upload_file_info_for_site',
                        mock_get_uploaded_files_multiple_processes)
    upload_token = 'token2'
    upload_info = load_upload_data(upload_token)
    uploaded_files = mock_get_uploaded_files_multiple_processes('', 'US-PFa')
    result1, result2 = link_issue.find_files_replaced_by_upload(
        upload_token, upload_info)
    assert result1 == uploaded_files
    result2.sort()
    assert result2 == ['alongtokenstring', 'anotherlongtokenstring']
    assert upload_info['reports']['13278']['files_replaced'] == {
        '1234': uploaded_files[2]}
    assert upload_info['reports']['13279']['files_replaced'] == {
        '4567': uploaded_files[3]}
    for process_id in upload_info['reports']:
        if process_id in ['13278', '13279']:
            continue
        assert upload_info['reports'][process_id]['files_replaced'] == {}


# test parse_file_date_range
@pytest.mark.parametrize(
    'file_date_range, expected_result',
    [('None-None', [(None, None)]),
     ('20000101-20010101', [('20000101', '20010101')]),
     ('20000101-20010101 20010101-20020101',
     [('20000101', '20010101'), ('20010101', '20020101')])],
    ids=['None-None', 'single range', 'multiple dates'])
def test_parse_file_date_range(link_issues, file_date_range, expected_result):
    assert link_issues.parse_file_date_range(
        file_date_range) == expected_result


# test link_jira_issues_with_replacement_files
# no current report
# no previous reports but previous uploads
# no previous uploads

# test link_issue

# test update_issue_status
