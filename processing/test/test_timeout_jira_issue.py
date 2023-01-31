import datetime
import json
import os
import pytest

from db_handler import DBHandler
from decimal import Decimal
from email_gen import EmailGen
from jira_interface import JIRAInterface
from jira_names import JIRANames
from logging import Logger
from timeout_jira_issue import TimeOutIssue


log = Logger('test')


def mock_get_organizations(dummy_self_var):
    return {'SiteName': 'ID'}


@pytest.fixture
def timeout_jira_issue(monkeypatch):
    """ Initialize TimeOut Jira Issues"""
    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_get_organizations)
    monkeypatch.setattr(JIRAInterface, 'add_comment',
                        mock_jira_interface_add_comment)
    monkeypatch.setattr(JIRAInterface, 'add_label',
                        mock_jira_interface_label_transition)
    monkeypatch.setattr(JIRAInterface, 'set_issue_state',
                        mock_jira_interface_label_transition)
    monkeypatch.setattr(TimeOutIssue, 'get_jira_issues', mock_get_jira_issues)
    monkeypatch.setattr(TimeOutIssue, 'get_issue_labels',
                        mock_get_issue_labels)
    monkeypatch.setattr(TimeOutIssue, 'get_process_code_version',
                        mock_get_process_code_version)
    timeout = TimeOutIssue()
    timeout.jira_project = None  # enforce that there is no JIRA project
    timeout.current_date = test_resources.get('current_dates')[0]
    return timeout


def test_get_jira_issues():
    """
    Gather timed-out issues from SQL based on status, schedule, age &
    existing labels.

    ToDo:
      will require mock db_handler.get_timeout_issues
      which should be done using sample DB.
      Should Sample DBS include snippets of all involved DB tables? Oof.
      Check that query is formulated as expected &
      that results from sample DB are as expected.
      Test with both has_labels & new_labels together & independently
    """
    pass


def mock_jira_interface_add_comment(dummy_self_var, issue_key,
                                    message, public):
    pass


def mock_jira_interface_label_transition(dummy_self_var, issue_key, value):
    pass


def mock_jira_interface_label_exception(dummy_self_var, issue_key, value):
    raise Exception


def mock_get_upload_data(dummy_self_var, upload_token, code_version=None):
    resource_path = './test/resources/'
    resource_files = os.listdir(resource_path)
    upload_token_filename = None
    for filename in resource_files:
        if upload_token in filename:
            upload_token_filename = filename
    if not upload_token_filename:
        return dict()  # change this eventually to a http 404
    with open(os.path.join(resource_path, upload_token_filename)) as f:
        return json.load(f)


def mock_generate_auto_email_components(dummy_self_var,
                                        upload_info, files_statuses):
    return "<List results>"


def mock_get_jira_issues(dummy_self_var, status, reminder_field_values,
                         max_days_passed, has_labels):
    return test_resources.get('timeout_issues')


def mock_get_issue_labels(dummy_self_var, timeout_store_dict):
    return test_resources.get('label_lookup')


def mock_get_process_code_version(dummy_self_var, timeout_issues):
    return {'63084': '0.4.19', '63096': '0.4.19'}


def mock_db_handler_get_process_code_version(dummy_self_var, process_ids):
    return test_resources.get('process_code_version')


def mock_gen_timeout_comment(dummy_self_var, issue_number, upload_token,
                             code_version, msg_key):
    return 'test message'


def mock_gen_internal_support_msg(dummy_self_var, issue_number,
                                  days_since_status_change, label_lookup):
    return 'test message'


def mock_gen_internal_timeout_msg(dummy_self_var, issue_number,
                                  time_out_length):
    return 'test message'


def get_jira_messages():
    message_json = None
    try:
        with open('./jira_messages.json') as f:
            message_json = json.load(f)
    except Exception:
        print('Could not open jira_messages.json file.')
        pass
    return message_json


def test_get_timeout_issue_store(timeout_jira_issue):
    timeout_issues = test_resources.get('timeout_issues')

    assert timeout_jira_issue.get_timeout_issue_store(
        timeout_issues) == timeout_issue_store


def test_get_process_code_version(monkeypatch):
    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_get_organizations)
    monkeypatch.setattr(DBHandler, 'get_process_code_version',
                        mock_db_handler_get_process_code_version)
    timeout_issues = test_resources.get('timeout_issues')
    timeout = TimeOutIssue()
    timeout.qaqc_db_handler = DBHandler(hostname='fake', user='fake',
                                        password='fake', db_name='fake')
    assert timeout.get_process_code_version(timeout_issues) == \
        mock_get_process_code_version('fake_var', 'fake_var')


def test_gen_timeout_comment(monkeypatch, timeout_jira_issue, caplog):
    monkeypatch.setattr(EmailGen, 'get_upload_data', mock_get_upload_data)
    monkeypatch.setattr(EmailGen, 'generate_auto_email_components',
                        mock_generate_auto_email_components)

    issue_number = test_resources.get('issue_number')
    upload_token = test_resources.get('upload_token')
    code_version = test_resources.get('code_version')

    generated_message = timeout_jira_issue.gen_timeout_comment(
        issue_number, upload_token, code_version, msg_key='timeout1')
    assert generated_message == test_resources.get('success_comment')

    generated_message = timeout_jira_issue.gen_timeout_comment(
        issue_number, upload_token, code_version, msg_key='timeout2')
    error_msg = ('Jira Messages file or message not found. Comment for '
                 f'{issue_number} not added.')
    assert generated_message is None
    assert error_msg in caplog.text


def test_get_msg_text(timeout_jira_issue):
    if not timeout_jira_issue.message_json:
        pass
    for msg_key, msg_text in get_jira_messages().items():
        assert timeout_jira_issue.get_msg_text(msg_key, '100') == msg_text
    assert timeout_jira_issue.get_msg_text('blank', '100') == ''


def test_get_max_day_passed(timeout_jira_issue):
    reminder_schedules = test_resources.get('reminder_schedule')
    assert timeout_jira_issue.get_max_day_passed(reminder_schedules) == 40


def test_get_query_params(timeout_jira_issue):
    reminder_schedules = test_resources.get('reminder_schedule')
    query_params = timeout_jira_issue.get_query_params(
        '10200', 'Waiting for Customer', reminder_schedules)
    assert query_params == {
        'status': 'Waiting for Customer',
        'reminder_field_values': ['10200'],
        'max_days_passed': 40}


def test_has_unexpected_labels(timeout_jira_issue):
    label_lookup = test_resources.get('label_lookup')
    assert timeout_jira_issue.has_unexpected_label(
        label_lookup, '3064', expected_labels=[]) is False
    assert timeout_jira_issue.has_unexpected_label(
        label_lookup, '3103', expected_labels=[]) is True
    assert timeout_jira_issue.has_unexpected_label(
        label_lookup, '3103', expected_labels=['Auto_Reminder_1']) is False
    assert timeout_jira_issue.has_unexpected_label(
        label_lookup, '3107', expected_labels=[]) is False
    assert timeout_jira_issue.has_unexpected_label(
        label_lookup, '3107', expected_labels=['Auto_Reminder_1']) is False


def test_has_new_label(timeout_jira_issue):
    label_lookup = test_resources.get('label_lookup')
    assert timeout_jira_issue.has_new_label(
        '3064', 'Auto_Reminder_1', label_lookup) is False
    assert timeout_jira_issue.has_new_label(
        '3103', 'Auto_Reminder_1', label_lookup) is True
    assert timeout_jira_issue.has_new_label(
        '3103', 'Auto_Reminder_2', label_lookup) is False
    assert timeout_jira_issue.has_new_label(
        '3107', 'Auto_Reminder_1', label_lookup) is False
    assert timeout_jira_issue.has_new_label(
        '310', 'Auto_Reminder_1', label_lookup) is False


def test_calculate_time_diff(timeout_jira_issue):
    assert timeout_jira_issue.calculate_time_diff(datetime.datetime(
        2020, 4, 5, 11, 44, 34, 670000)) == 0
    assert timeout_jira_issue.calculate_time_diff(datetime.datetime(
        2020, 3, 15, 11, 44, 34, 670000)) == 21
    assert timeout_jira_issue.calculate_time_diff(datetime.datetime(
        2020, 3, 15, 9, 44, 34, 670000)) == 21
    assert timeout_jira_issue.calculate_time_diff(datetime.datetime(
        2020, 4, 15, 9, 44, 34, 670000)) == -10


def test_get_reminder_schedule_idx(timeout_jira_issue):
    reminder_schedules = test_resources.get('reminder_schedule')
    assert timeout_jira_issue.get_reminder_schedule_idx(
        0, reminder_schedules) == -1
    assert timeout_jira_issue.get_reminder_schedule_idx(
        3, reminder_schedules) == -1
    assert timeout_jira_issue.get_reminder_schedule_idx(
        7, reminder_schedules) == 0
    assert timeout_jira_issue.get_reminder_schedule_idx(
        9, reminder_schedules) == 0
    assert timeout_jira_issue.get_reminder_schedule_idx(
        15, reminder_schedules) == 1
    assert timeout_jira_issue.get_reminder_schedule_idx(
        31, reminder_schedules) == 2


def test_get_action_params(monkeypatch, timeout_jira_issue):
    monkeypatch.setattr(TimeOutIssue, 'gen_timeout_comment',
                        mock_gen_timeout_comment)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_support_msg',
                        mock_gen_internal_support_msg)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_timeout_msg',
                        mock_gen_internal_timeout_msg)

    reminder_schedules = test_resources.get('reminder_schedule')
    max_reminder_day = max([r.get('days') for r in reminder_schedules])
    label_lookup = test_resources.get('label_lookup')
    transition_key = JIRANames.format_waiting_for_support

    # Auto, 7 < t < 14
    reminder_schedule = reminder_schedules[0]
    days_since_status_change = 9

    # no reminder label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3064', timeout_issue_store['3064'],
        days_since_status_change, max_reminder_day, label_lookup) == {
                'msg_type': reminder_schedule['msg'],
                'msg': 'test message', 'is_public_msg': True,
                'add_label': reminder_schedule['new_label'],
                'transition': None}

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3103', timeout_issue_store['3103'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'], 'msg': None,
               'is_public_msg': False, 'add_label': None, 'transition': None}

    # extra label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3107', timeout_issue_store['3107'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'],
               'msg': 'test message', 'is_public_msg': True,
               'add_label': reminder_schedule['new_label'],
               'transition': None}

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3109', timeout_issue_store['3109'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': 'internal_timeout_support', 'msg': 'test message',
               'is_public_msg': False, 'add_label': None,
               'transition': transition_key}

    # 14 < t < 30, auto schedule
    reminder_schedule = reminder_schedules[1]
    days_since_status_change = 16

    # no reminder label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3064', timeout_issue_store['3064'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'],
               'msg': 'test message', 'is_public_msg': True,
               'add_label': reminder_schedule['new_label'],
               'transition': None}

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3103', timeout_issue_store['3103'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'],
               'msg': 'test message', 'is_public_msg': True,
               'add_label': reminder_schedule['new_label'],
               'transition': None}

    # extra label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3107', timeout_issue_store['3107'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'],
               'msg': 'test message', 'is_public_msg': True,
               'add_label': reminder_schedule['new_label'],
               'transition': None}

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3109', timeout_issue_store['3109'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': reminder_schedule['msg'], 'msg': None,
               'is_public_msg': False, 'add_label': None,
               'transition': None}

    # greater than 30, auto schedule
    reminder_schedule = reminder_schedules[2]
    days_since_status_change = 32

    # no reminder label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3064', timeout_issue_store['3064'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': 'internal_timeout',
               'msg': 'test message', 'is_public_msg': False,
               'add_label': None, 'transition': transition_key}

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3103', timeout_issue_store['3103'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': 'internal_timeout',
               'msg': 'test message', 'is_public_msg': False,
               'add_label': None, 'transition': transition_key}

    # extra label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3107', timeout_issue_store['3107'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': 'internal_timeout',
               'msg': 'test message', 'is_public_msg': False,
               'add_label': None, 'transition': transition_key}

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.get_action_params(
        reminder_schedule, '3109', timeout_issue_store['3109'],
        days_since_status_change, max_reminder_day, label_lookup) == {
               'msg_type': 'internal_timeout',
               'msg': 'test message', 'is_public_msg': False,
               'add_label': None, 'transition': transition_key}


def test_process_timeout_issue(monkeypatch, timeout_jira_issue):
    monkeypatch.setattr(TimeOutIssue, 'gen_timeout_comment',
                        mock_gen_timeout_comment)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_support_msg',
                        mock_gen_internal_support_msg)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_timeout_msg',
                        mock_gen_internal_timeout_msg)
    reminder_schedules = test_resources.get('reminder_schedule')
    max_reminder_day = max([r.get('days') for r in reminder_schedules])
    label_lookup = test_resources.get('label_lookup')
    current_dates = test_resources.get('current_dates')

    # the following test the auto schedule
    # less than 7 days:
    timeout_jira_issue.current_date = current_dates[0]

    # no reminder label
    assert timeout_jira_issue.process_timeout_issue(
        '3064', timeout_issue_store['3064'], reminder_schedules,
        label_lookup, max_reminder_day) == (False, False)

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.process_timeout_issue(
        '3103', timeout_issue_store['3103'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # extra label
    assert timeout_jira_issue.process_timeout_issue(
        '3107', timeout_issue_store['3107'], reminder_schedules,
        label_lookup, max_reminder_day) == (False, False)

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.process_timeout_issue(
        '3109', timeout_issue_store['3109'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # 7 < t < 14 days:
    timeout_jira_issue.current_date = current_dates[1]

    # no reminder label
    assert timeout_jira_issue.process_timeout_issue(
        '3064', timeout_issue_store['3064'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.process_timeout_issue(
        '3103', timeout_issue_store['3103'], reminder_schedules,
        label_lookup, max_reminder_day) == (False, False)

    # extra label
    assert timeout_jira_issue.process_timeout_issue(
        '3107', timeout_issue_store['3107'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.process_timeout_issue(
        '3109', timeout_issue_store['3109'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # 14 < t < 30 days:
    timeout_jira_issue.current_date = current_dates[2]

    # no reminder label
    assert timeout_jira_issue.process_timeout_issue(
        '3064', timeout_issue_store['3064'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.process_timeout_issue(
        '3103', timeout_issue_store['3103'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # extra label
    assert timeout_jira_issue.process_timeout_issue(
        '3107', timeout_issue_store['3107'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.process_timeout_issue(
        '3109', timeout_issue_store['3109'], reminder_schedules,
        label_lookup, max_reminder_day) == (False, False)

    # greater than 30 days:
    timeout_jira_issue.current_date = current_dates[3]

    # no reminder label
    assert timeout_jira_issue.process_timeout_issue(
        '3064', timeout_issue_store['3064'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" label
    assert timeout_jira_issue.process_timeout_issue(
        '3103', timeout_issue_store['3103'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # extra label
    assert timeout_jira_issue.process_timeout_issue(
        '3107', timeout_issue_store['3107'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)

    # "Auto_Reminder_1" and "Auto_Reminder_2" label
    assert timeout_jira_issue.process_timeout_issue(
        '3109', timeout_issue_store['3109'], reminder_schedules,
        label_lookup, max_reminder_day) == (True, True)


def test_run_action(monkeypatch, timeout_jira_issue):
    monkeypatch.setattr(TimeOutIssue, 'gen_timeout_comment',
                        mock_gen_timeout_comment)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_support_msg',
                        mock_gen_internal_support_msg)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_timeout_msg',
                        mock_gen_internal_timeout_msg)

    assert timeout_jira_issue.run_action('3064') is False
    assert timeout_jira_issue.run_action(
        '3064', 'msg_type', 'test message') is True

    monkeypatch.setattr(JIRAInterface, 'add_label',
                        mock_jira_interface_label_exception)

    assert timeout_jira_issue.run_action(
        '3064', 'msg_type', 'test message', add_label='label') is False


def test_execute_reminder_schedule(monkeypatch, timeout_jira_issue):
    monkeypatch.setattr(TimeOutIssue, 'gen_timeout_comment',
                        mock_gen_timeout_comment)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_support_msg',
                        mock_gen_internal_support_msg)
    monkeypatch.setattr(TimeOutIssue, 'gen_internal_timeout_msg',
                        mock_gen_internal_timeout_msg)
    timeout_issues = test_resources.get('timeout_issues')
    reminder_schedule = test_resources.get('reminder_schedule')
    current_dates = test_resources.get('current_dates')

    # the following test the auto schedule
    # less than 7 days:
    timeout_jira_issue.current_date = current_dates[0]
    assert timeout_jira_issue.execute_reminder_schedule(
        reminder_schedule, timeout_issues) == (3, 2, 0)

    # 7 < t < 14 days
    timeout_jira_issue.current_date = current_dates[1]
    assert timeout_jira_issue.execute_reminder_schedule(
        reminder_schedule, timeout_issues) == (4, 1, 0)

    # 14 < t < 30 days
    timeout_jira_issue.current_date = current_dates[2]
    assert timeout_jira_issue.execute_reminder_schedule(
        reminder_schedule, timeout_issues) == (4, 1, 0)

    # greater than 30 days
    timeout_jira_issue.current_date = current_dates[3]
    assert timeout_jira_issue.execute_reminder_schedule(
        reminder_schedule, timeout_issues) == (5, 0, 0)


def test_get_reminder_schedule_info(timeout_jira_issue):
    reminder_schedule_result, reminder_id = \
        timeout_jira_issue.get_reminder_schedule_info('auto')
    assert reminder_schedule_result == test_resources.get('reminder_schedule')
    assert reminder_id == JIRANames.ReminderOptions.auto


test_resources = {
    'issue_number': 'XXX',
    'code_version': '0.4.19',
    'upload_token': 'token2',
    'success_comment': (
        'Dear SiteTeamMember2,\n\nThanks again for uploading data for US-PFa '
        'on Feb 26, 2018 (see complete file list below).\n\nAccording to our '
        'records, we have not received replacement uploads for one or more of '
        'the files below that failed Format QA/QC.\n\nFormat QA/QC results:\n'
        '<List results>\n\nYou can re-upload your data at '
        'https://ameriflux.lbl.gov/data/upload-data/ and/or reply to this '
        'email to discuss with us.\n\nView the status of all your uploaded '
        'files at https://ameriflux.lbl.gov/qaqc-reports-data-team/. \n\n'
        'You can track communications on this '
        'Format QA/QC report at None-XXX using your AmeriFlux account '
        'ID and password to login.\n\nSincerely,\nAMP Data Team'),
    'reminder_schedule': [
        {'days': 7, 'new_label': 'Auto_Reminder_1', 'msg': 'timeout1'},
        {'days': 14, 'new_label': 'Auto_Reminder_2',
         'expected_labels': ['Auto_Reminder_1'], 'msg': 'timeout1'},
        {'days': 30, 'transition': 'format_waiting_for_support',
         'expected_labels': ['Auto_Reminder_1', 'Auto_Reminder_2'],
         'msg': 'internal_timeout'}],
    'process_code_version': [{'processID': 63084, 'codeVersion': '0.4.19'},
                             {'processID': 63096, 'codeVersion': '0.4.19'}],
    'label_lookup': {'3064': ['Results_Sent'],
                     '3065': ['Results_Sent', 'Weekly'],
                     '3103': ['Results_Sent', 'Auto_Reminder_1'],
                     '3107': ['Results_Sent', 'FLX-CA'],
                     '3109': ['Results_Sent', 'Auto_Reminder_1',
                              'Auto_Reminder_2']},
    'timeout_issues': [
        {'issuenum': Decimal('3064'),
         'uploadToken': 'f230ea8b34ec4be3925f29895a273993',
         'processID': '63084\n63085\n63086\n63087\n63088\n63089\n63090\n63091',
         'last_change_date':
             datetime.datetime(2020, 4, 1, 14, 42, 40, 490000)},
        {'issuenum': Decimal('3065'),
         'uploadToken': 'f4cb779533b14a24bce5e16f612a5365',
         'processID': '63096\n63097',
         'last_change_date':
             datetime.datetime(2020, 4, 1, 14, 15, 57, 197000)},
        {'issuenum': Decimal('3103'),
         'uploadToken': 'f4cb779533b14a24bce5e16f612a5365',
         'processID': '63096\n63097',
         'last_change_date':
             datetime.datetime(2020, 4, 2, 11, 44, 34, 670000)},
        {'issuenum': Decimal('3103'),
         'uploadToken': 'f4cb779533b14a24bce5e16f612a5365',
         'processID': '63096\n63097',
         'last_change_date':
             datetime.datetime(2020, 4, 2, 14, 21, 8, 577000)},
        {'issuenum': Decimal('3107'),
         'uploadToken': 'f4cb779533b14a24bce5e16f612a5365',
         'processID': '63096\n63097',
         'last_change_date':
             datetime.datetime(2020, 4, 2, 14, 22, 8, 577000)},
        {'issuenum': Decimal('3109'),
         'uploadToken': 'f4cb779533b14a24bce5e16f612a5365',
         'processID': '63096\n63097',
         'last_change_date':
             datetime.datetime(2020, 4, 2, 16, 21, 8, 577000)}],
    'current_dates': [
        datetime.datetime(2020, 4, 5, 11, 44, 34, 670000),
        datetime.datetime(2020, 4, 10, 11, 44, 34, 670000),
        datetime.datetime(2020, 4, 18, 11, 44, 34, 670000),
        datetime.datetime(2020, 5, 3, 11, 44, 34, 670000)]
    }


timeout_issue_store = {
    '3064': {
        'upload_token': test_resources['timeout_issues'][0]['uploadToken'],
        'code_version': '0.4.19',
        'last_change_date':
            test_resources['timeout_issues'][0]['last_change_date']},
    '3065': {
        'upload_token': test_resources['timeout_issues'][1]['uploadToken'],
        'code_version': '0.4.19',
        'last_change_date':
            test_resources['timeout_issues'][1]['last_change_date']},
    '3103': {
        'upload_token': test_resources['timeout_issues'][2]['uploadToken'],
        'code_version': '0.4.19',
        'last_change_date':
            test_resources['timeout_issues'][2]['last_change_date']},
    '3107': {
        'upload_token': test_resources['timeout_issues'][4]['uploadToken'],
        'code_version': '0.4.19',
        'last_change_date':
            test_resources['timeout_issues'][4]['last_change_date']},
    '3109': {
        'upload_token': test_resources['timeout_issues'][5]['uploadToken'],
        'code_version': '0.4.19',
        'last_change_date':
            test_resources['timeout_issues'][5]['last_change_date']}
    }
