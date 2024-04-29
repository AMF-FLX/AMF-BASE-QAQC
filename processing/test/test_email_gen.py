from email_gen import EmailGen
from jira_interface import JIRAInterface
from jira_names import JIRANames
import json
import os
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


@pytest.fixture
def email_gen(monkeypatch):
    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_jira_get_organization)

    monkeypatch.setattr(JIRAInterface, 'add_comment', mock_jira_add_comment)

    monkeypatch.setattr(EmailGen, 'create_jira_format_issue',
                        mock_create_jira_format_issue)

    return EmailGen()


def mock_jira_get_organization(notsurewhatthisargisfor):
    return {
        'US-Ton': '207',
        'CA-DBB': '322',
        'US-MOz': '6',
        'US-PFa': '166'}


def mock_create_jira_format_issue(
        dummy_jira, dummy_upload_info, dummy_status_result_txt,
        dummy_upload_token, dummy_description_txt, dummy_test_issue):
    return 'QAQC-9999'


def mock_jira_add_comment(issue_key, message, public_comment):
    pass


def get_element_order_in_text_str(text_str, element_list):
    element_index = []
    for element in element_list:
        element_index.append(text_str.find(element))
    return element_index


def test_is_upload_from_zip(email_gen):
    upload_info = {'zip_file': 'US-PFa-19952017-09102017.zip'}
    assert email_gen.is_upload_from_zip(upload_info) is True
    upload_info = {'zip_file': None}
    assert email_gen.is_upload_from_zip(upload_info) is False


def test_get_sorted_file_reports(email_gen):
    upload_info = {
        'reports': {
            '9425': {'upload_file': 'US-PFa_HR_199601010000_199701010000.csv'},
            '9424': {'upload_file': 'US-PFa_HR_199501010000_199601010000.csv'},
            '9426': {'upload_file': 'US-PFa_HR_199701010000_199801010000.csv'}
        }}
    results = [
        {'upload_file': 'US-PFa_HR_199501010000_199601010000.csv'},
        {'upload_file': 'US-PFa_HR_199601010000_199701010000.csv'},
        {'upload_file': 'US-PFa_HR_199701010000_199801010000.csv'}]
    assert email_gen.get_sorted_file_reports(upload_info) == results


def test_has_autocorrect_file(email_gen):
    file_report = {
        'autorepair_qaqc_checks': {'processor': 'WILE$'}}
    assert email_gen.has_autocorrect_file(file_report) is True
    file_report = {
        'autorepair_qaqc_checks': None}
    assert email_gen.has_autocorrect_file(file_report) is False


def test_get_status_code(email_gen):
    assert email_gen.get_status_code('CRITICAL') == -3
    assert email_gen.get_status_code('ERROR') == -2
    assert email_gen.get_status_code('WARNING') == -1
    assert email_gen.get_status_code('OK') == 0


def test_get_worst_status(email_gen):
    assert (email_gen.get_worst_check_status(['WARNING'])) == 'WARNING'
    assert (email_gen.get_worst_check_status(['WARNING', 'OK'])) == 'WARNING'
    assert (email_gen.get_worst_check_status(['WARNING', 'ERROR'])) == 'ERROR'
    assert (email_gen.get_worst_check_status(
        ['WARNING', 'OK', 'CRITICAL'])) == 'CRITICAL'
    assert (email_gen.get_worst_check_status(
        ['WARNING', 'FAIL'])) == 'BAD_STATUS'


def test_get_overall_file_status_code(email_gen):
    has_autocorrect_file = True
    file_report = {
        'qaqc_checks': {'process_confirmation': {'status_code': 'WARNING'}},
        'autorepair_qaqc_checks': {
            'process_confirmation': {'status_code': 'WARNING'}}}
    assert email_gen.get_overall_file_status_code(
        file_report, has_autocorrect_file) == -1
    file_report = {
        'qaqc_checks': {'process_confirmation': {'status_code': 'OK'}},
        'autorepair_qaqc_checks': {
            'process_confirmation': {'status_code': 'WARNING'}}}
    assert email_gen.get_overall_file_status_code(
        file_report, has_autocorrect_file) == -1
    has_autocorrect_file = False
    file_report = {
        'qaqc_checks': {'process_confirmation': {'status_code': 'WARNING'}},
        'autorepair_qaqc_checks': {
            'process_confirmation': {'status_code': 'WARNING'}}}
    assert email_gen.get_overall_file_status_code(
        file_report, has_autocorrect_file) == -1
    file_report = {
        'qaqc_checks': {'process_confirmation': {'status_code': 'OK'}},
        'autorepair_qaqc_checks': {
            'process_confirmation': {'status_code': 'WARNING'}}}
    assert email_gen.get_overall_file_status_code(
        file_report, has_autocorrect_file) == 0


def test_get_file_statuses(email_gen):
    upload_info = {
        'reports': {
            '9425': {
                'upload_file': 'US-PFa_HR_199601010000_199701010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'WARNING'}},
                'autorepair_qaqc_checks': {'process_confirmation':
                                           {'status_code': 'WARNING'}},
                'process_id': '9425'},
            '9424': {
                'upload_file': 'US-PFa_HR_199501010000_199601010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'OK'}},
                'autorepair_qaqc_checks': {'process_confirmation':
                                           {'status_code': 'WARNING'}},
                'process_id': '9424'},
            '9426': {
                'upload_file': 'US-PFa_HR_199701010000_199801010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'CRITICAL'}},
                'autorepair_qaqc_checks': None,
                'process_id': '9426'},
            '9429': {
                'upload_file': 'US-PFa_HR_200101010000_200201010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'CRITICAL'}},
                'autorepair_qaqc_checks': {'process_confirmation':
                                           {'status_code': 'ERROR'}},
                'process_id': '9429'},
            '9428': {
                'upload_file': 'US-PFa_HR_199901010000_200001010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'OK'}},
                'autorepair_qaqc_checks': None,
                'process_id': '9428'},
            '9427': {
                'upload_file': 'US-PFa_HR_199801010000_199901010000.csv',
                'qaqc_checks': {'process_confirmation':
                                {'status_code': 'WARNING'}},
                'autorepair_qaqc_checks': None,
                'process_id': '9427'}
            }}
    stat_pass = email_gen.overall_status_txt['pass']
    stat_warning = email_gen.overall_status_txt['warning']
    stat_fail = email_gen.overall_status_txt['fail']
    act_ok = email_gen.overall_action_txt['ok_no_autocorrect']
    act_warn = email_gen.overall_action_txt['warn_no_autocorrect']
    act_auto = email_gen.overall_action_txt['pass_autocorrect']
    act_fail = email_gen.overall_action_txt['fail']
    b = '*'
    results = {
        '9425': {'has_autocorrect': True, 'overall_code': -1,
                 'overall_txt': f'{b}{stat_warning}{b} | {b}{act_auto}{b}'},
        '9424': {'has_autocorrect': True, 'overall_code': -1,
                 'overall_txt': f'{b}{stat_warning}{b} | {b}{act_auto}{b}'},
        '9426': {'has_autocorrect': False, 'overall_code': -3,
                 'overall_txt': f'{b}{stat_fail}{b} | {b}{act_fail}{b}'},
        '9429': {'has_autocorrect': True, 'overall_code': -2,
                 'overall_txt': f'{b}{stat_fail}{b} | {b}{act_fail}{b}'},
        '9428': {'has_autocorrect': False, 'overall_code': 0,
                 'overall_txt': f'{b}{stat_pass}{b} | {b}{act_ok}{b}'},
        '9427': {'has_autocorrect': False, 'overall_code': -1,
                 'overall_txt': f'{b}{stat_warning}{b} | {b}{act_warn}{b}'}}
    assert email_gen.get_file_statuses(upload_info) == results


def test_get_check_info(email_gen):
    check_result = {'check_name': 'Any Variables with ALL Data Missing?',
                    'status_code': [
                        'WARNING'
                    ],
                    'status_msg': {
                        'WARNING': {
                            'status_prefix': [
                                'These variables have all data missing: '],
                            'status_body': ['CH4_1_1_1, CH4_1_2_1, CH4_1_3_1, '
                                            'FCH4_1_1_1, SCH4_F_1_1_1'],
                            'status_suffix': ['. Previously uploaded data '
                                              'with the same time period will '
                                              'be overwritten.'],
                            'emphasize_prefix': [0],
                            'emphasize_body': [1],
                            'emphasize_suffix': [0],
                            'all_plots': [],
                            'targeted_plots': []}},
                    'sub_status': []}
    assert email_gen.get_check_info(check_result) == ('Verification', (
        'These variables have no data for file\'s entire time period: '
        'CH4_1_1_1, CH4_1_2_1, CH4_1_3_1, FCH4_1_1_1, SCH4_F_1_1_1. '
        'Previously uploaded data with the same time period will be '
        'overwritten.'), 'data_missing', 'WARNING')
    check_result = {'check_name': 'AutoRepair Fixes and/or Error Messages',
                    'status_code': [
                        'WARNING'
                    ],
                    'status_msg': {
                        'WARNING': {
                            'status_prefix': [],
                            'status_body': [
                                ('Filename components fixed: ts-start '
                                 '(start time); ts-end (end time)'),
                                ('File was AutoRepaired and repaired file '
                                 'uploaded.')],
                            'status_suffix': [],
                            'emphasize_prefix': [],
                            'emphasize_body': [0, 0],
                            'emphasize_suffix': [],
                            'all_plots': [],
                            'targeted_plots': []}},
                    'sub_status': []}
    assert email_gen.get_check_info(check_result) == (
        'Passive',
        ('Filename components fixed: ts-start (start time); ts-end '
         '(end time)File was AutoRepaired and repaired file uploaded.'),
        'file_fixer', 'WARNING')
    check_result = {'check_name': 'AutoRepair Fixes and/or Error Messages',
                    'status_code': ['WARNING', 'CRITICAL'],
                    'status_msg': {
                        'WARNING': {
                            'status_prefix': [],
                            'status_body': [
                                'Changed dat extension to CSV.',
                                ('Tried to fix invalid variable name yr with '
                                 'YR'),
                                ('Tried to fix invalid variable name day with '
                                 'DAY'),
                                ('Tried to fix invalid variable name endhour '
                                 'with ENDHOUR')],
                            'status_suffix': [],
                            'emphasize_prefix': [],
                            'emphasize_body': [0, 0, 0, 0],
                            'emphasize_suffix': [],
                            'all_plots': [],
                            'targeted_plots': []},
                        'CRITICAL': {
                            'status_prefix': [],
                            'status_body': [('Unable to repair timestamps. '
                                             'AutoRepair FAILED.')],
                            'status_suffix': [],
                            'emphasize_prefix': [],
                            'emphasize_body': [0, 0],
                            'emphasize_suffix': [],
                            'all_plots': [],
                            'targeted_plots': []},
                    },
                    'sub_status': []}
    assert email_gen.get_check_info(check_result) == (
        'Active', 'Unable to repair timestamps. AutoRepair FAILED.',
        'file_fixer', 'CRITICAL')


def test_get_report_links(email_gen):
    upload_info = {
        'SITE_ID': 'US-PFa',
        'reports': {
            '9425': {
                'process_id': '9425',
                'upload_file': 'US-PFa_HR_199601010000_199701010000.csv'},
            '9424': {
                'upload_file': 'US-PFa_HR_199501010000_199601010000.csv',
                'process_id': '9424'},
            '9426': {
                'upload_file': 'US-PFa_HR_199701010000_199801010000.csv',
                'process_id': '9426'},
            '9429': {
                'upload_file': 'US-PFa_HR_200101010000_200201010000.csv',
                'process_id': '9429'}}}
    assert email_gen.get_report_links(upload_info) == [
        ('US-PFa_HR_199501010000_199601010000.csv: '
         f'{email_gen.construct_report_link("US-PFa", "9424")}'),
        ('US-PFa_HR_199601010000_199701010000.csv: '
         f'{email_gen.construct_report_link("US-PFa", "9425")}'),
        ('US-PFa_HR_199701010000_199801010000.csv: '
         f'{email_gen.construct_report_link("US-PFa", "9426")}'),
        ('US-PFa_HR_200101010000_200201010000.csv: '
         f'{email_gen.construct_report_link("US-PFa", "9429")}')]


def test_get_worst_file_status_overall_upload_status(email_gen):
    file_statuses = {
        '9425': {'has_autocorrect': True, 'overall_code': -1},
        '9426': {'has_autocorrect': False, 'overall_code': -3},
        '9429': {'has_autocorrect': True, 'overall_code': -2},
        '9428': {'has_autocorrect': True, 'overall_code': 0}}
    jira_pass = JIRANames.format_QAQC_ready_for_data
    jira_fail = JIRANames.format_QAQC_sent_qaqc_review
    assert email_gen.get_worst_file_status(file_statuses) == (-3, False)
    assert email_gen.get_overall_upload_status(file_statuses) == \
        email_gen.email_subject_action['action']
    assert email_gen.get_overall_upload_jira_state(file_statuses) == jira_fail
    file_statuses = {
        '9425': {'has_autocorrect': True, 'overall_code': -1},
        '9426': {'has_autocorrect': True, 'overall_code': 0},
        '9429': {'has_autocorrect': True, 'overall_code': -1},
        '9428': {'has_autocorrect': False, 'overall_code': 0}}
    assert email_gen.get_worst_file_status(file_statuses) == (-1, True)
    assert email_gen.get_overall_upload_status(file_statuses) == \
        email_gen.email_subject_action['review']
    assert email_gen.get_overall_upload_jira_state(file_statuses) == jira_pass
    file_statuses = {
        '9426': {'has_autocorrect': True, 'overall_code': 0},
        '9428': {'has_autocorrect': False, 'overall_code': 0}}
    assert email_gen.get_worst_file_status(file_statuses) == (0, True)
    assert email_gen.get_overall_upload_status(file_statuses) == \
        email_gen.email_subject_action['review']
    assert email_gen.get_overall_upload_jira_state(file_statuses) == jira_pass
    file_statuses = {
        '9426': {'has_autocorrect': False, 'overall_code': 0},
        '9428': {'has_autocorrect': True, 'overall_code': 0}}
    assert email_gen.get_worst_file_status(file_statuses) == (0, True)
    assert email_gen.get_overall_upload_status(file_statuses) == \
        email_gen.email_subject_action['review']
    assert email_gen.get_overall_upload_jira_state(file_statuses) == jira_pass
    file_statuses = {
        '9426': {'has_autocorrect': False, 'overall_code': 0},
        '9428': {'has_autocorrect': False, 'overall_code': 0}}
    assert email_gen.get_worst_file_status(file_statuses) == (0, False)
    assert email_gen.get_overall_upload_status(file_statuses) == \
        email_gen.email_subject_action['good']
    assert email_gen.get_overall_upload_jira_state(file_statuses) == jira_pass


def test_generate_auto_email_components(email_gen):
    upload_info = {
        'SITE_ID': 'US-PFa',
        'reports': {
            '9425': {'upload_file': 'US-PFa_HR_199601010000_199701010000.csv',
                     'process_id': '9425'},
            '9424': {'upload_file': 'US-PFa_HR_199501010000_199601010000.csv',
                     'process_id': '9424'},
            '9426': {'upload_file': 'US-PFa_HR_199701010000_199801010000.csv',
                     'process_id': '9426'},
            '9427': {'upload_file': 'US-PFa_HR_199801010000_199901010000.csv',
                     'process_id': '9427'}}}
    stat_pass = email_gen.overall_status_txt['pass']
    stat_fail = email_gen.overall_status_txt['fail']
    act_ok = email_gen.overall_action_txt['ok_no_autocorrect']
    # act_warn = email_gen.overall_action_txt['warn_no_autocorrect']
    act_auto = email_gen.overall_action_txt['pass_autocorrect']
    act_fail = email_gen.overall_action_txt['fail']
    b = '*'
    file_statuses = {
        '9425': {'has_autocorrect': False, 'overall_code': 0,
                 'overall_txt': f'{b}{stat_pass}{b} | {b}{act_ok}{b}'},
        '9424': {'has_autocorrect': True, 'overall_code': -1,
                 'overall_txt': f'{b}{stat_pass}{b} | {b}{act_auto}{b}'},
        '9426': {'has_autocorrect': True, 'overall_code': -3,
                 'overall_txt': f'{b}{stat_fail}{b} | {b}{act_fail}{b}'},
        '9427': {'has_autocorrect': True, 'overall_code': 0,
                 'overall_txt': f'{b}{stat_pass}{b} | {b}{act_auto}{b}'}}
    results = (f'US-PFa_HR_199501010000_199601010000.csv:\n* {b}{stat_pass}'
               f'{b} | {b}{act_auto}{b}\n* Read details in this report: '
               f'https://{link}/qaqc-report/?site_id=US-PFa&report_id=9424\n\n'
               f'US-PFa_HR_199601010000_199701010000.csv:\n* {b}{stat_pass}'
               f'{b} | {b}{act_ok}{b}\n* Read details in this report: '
               f'https://{link}/qaqc-report/?site_id=US-PFa&report_id=9425\n\n'
               f'US-PFa_HR_199701010000_199801010000.csv:\n* {b}{stat_fail}'
               f'{b} | {b}{act_fail}{b}\n* Read details in this report: '
               f'https://{link}/qaqc-report/?site_id=US-PFa&report_id=9426\n\n'
               f'US-PFa_HR_199801010000_199901010000.csv:\n* {b}{stat_pass}'
               f'{b} | {b}{act_auto}{b}\n* Read details in this report: '
               f'https://{link}/qaqc-report/?site_id=US-PFa&report_id=9427')
    assert email_gen.generate_auto_email_components(
        upload_info, file_statuses) == results


def mock_get_upload_data(dummy_self_var, upload_token):
    # assume ~/AMF-QAQC/processing is the cwd
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


def get_upload_data(monkeypatch, upload_token):

    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_jira_get_organization)

    monkeypatch.setattr(JIRAInterface, 'add_comment', mock_jira_add_comment)

    monkeypatch.setattr(EmailGen, 'get_upload_data',
                        mock_get_upload_data)

    monkeypatch.setattr(EmailGen, 'create_jira_format_issue',
                        mock_create_jira_format_issue)

    email_gen = EmailGen()
    email_gen.test_issue = False
    email_gen.test_mode = True
    email_gen.code_version = detail_results[upload_token]['code_version']
    upload_info = email_gen.get_upload_data(upload_token)
    return upload_info


def test_get_jira_format_issue_inputs(monkeypatch, email_gen):

    status_result = {
        'token1':
            email_gen.email_subject_action['good'],
        'token2':
            email_gen.email_subject_action['review'],
        'token3':
            email_gen.email_subject_action['good'],
        'token4':
            email_gen.email_subject_action['review'],
        'token5':
            email_gen.email_subject_action['action'],
        'token6':
            email_gen.email_subject_action['action']}

    for upload_token, known_results in detail_results.items():
        upload_info = get_upload_data(monkeypatch, upload_token)
        assert list(email_gen.get_jira_format_issue_inputs(
            upload_info, status_result[upload_token], test_issue=False)) == \
            known_results.get('jira_issue_info_parsed')


def test_get_formated_upload_datetime(email_gen):
    upload_info = {'datetime': '2019-01-02T11:00:01.0000002'}
    assert email_gen.get_formated_upload_datetime(upload_info) == \
        'Jan 02, 2019'


def test_all_archival_files(email_gen):

    upload_reports_response = {
        'SITE_ID': '',
        'datetime': '',
        'reports': {},
        'upload_comment': '',
        'uploader': '',
        'uploader_email': '',
        'uploader_id': '',
        'zip_file': None
    }

    assert email_gen.all_upload_files_archival(
        upload_reports_response) == 'No file reports found.'

    qaqc_checks = {'checks': [
        {
            'check_name': email_gen.fixer_check_name,
            'status_msg': {
                'WARNING': {
                    'status_body': [email_gen.fixer_archival_file_txt]
                }
            }
        }
    ]}

    upload_reports_response.update(
        reports={'1': {'qaqc_checks': qaqc_checks,
                       'upload_file': 'data.csv'}})
    assert email_gen.all_upload_files_archival(
        upload_reports_response) == 'All uploaded files are archival.'

    qaqc_checks = {'checks': [
        {
            'check_name': email_gen.fixer_check_name,
            'status_msg': {
                'WARNING': {
                    'status_body': ['Fixes made.']
                }
            }
        }
    ]}

    upload_reports_response.update(
        reports={'1': {'qaqc_checks': qaqc_checks,
                       'upload_file': 'data.csv'}})
    assert email_gen.all_upload_files_archival(
        upload_reports_response) is None


def test_craft_email(monkeypatch):
    """
    Using driver to create and then test craft_email
    """

    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_jira_get_organization)

    monkeypatch.setattr(JIRAInterface, 'add_comment', mock_jira_add_comment)

    monkeypatch.setattr(EmailGen, 'get_upload_data',
                        mock_get_upload_data)

    monkeypatch.setattr(EmailGen, 'create_jira_format_issue',
                        mock_create_jira_format_issue)

    for upload_token, known_results in detail_results.items():
        email_gen = EmailGen()
        email_gen.test_issue = False
        email_gen.test_mode = True
        msg_body = email_gen.driver(upload_token, detailed_email=True)
        known_msg_body = known_results.get('msg_body')
        compare_msg_body(known_msg_body, msg_body)


def compare_msg_body(known_msg_body, msg_body):
    if len(known_msg_body) == 1:
        # If the upload does not have multiple files and issues that
        #     can be reported in random order, do a direct comparison
        assert msg_body == known_msg_body[0]
    elif len(''.join(known_msg_body)) == len(msg_body):
        # Otherwise if the msg_body lengths are equal, check content.
        # First, find the position of the parts in the result msg body
        known_msg_body_index = \
            get_element_order_in_text_str(text_str=msg_body,
                                          element_list=known_msg_body)
        content_is_present = \
            all([i > -1 for i in known_msg_body_index])
        if content_is_present:
            # And if all the pieces match,
            # rebuild the body msg in the correct order for final compare
            known_msg_body_reordered = []
            for index in sorted(known_msg_body_index):
                part_position = known_msg_body_index.index(index)
                known_msg_body_reordered.append(
                    known_msg_body[part_position])
            assert ''.join(known_msg_body_reordered) == msg_body
        else:
            # Otherwise do more rigorous checking
            for i, index in enumerate(known_msg_body_index):
                if index > -1:
                    continue
                known_msg_part = known_msg_body[i]
                if known_msg_part[0:5] != 'These':
                    continue
                # Get the pre random file order part:
                #    Find the position of first instance of 'files'
                #    Add 5 index positions to get the position of the
                #        end of the stable start text (5 = 'iles ')
                known_msg_part_len = len(known_msg_part)
                end_index_start_txt = known_msg_part.find('files') + 5
                start_index_end_txt = known_msg_part.find(':')
                if not (end_index_start_txt and
                        start_index_end_txt):
                    continue
                known_start_text = known_msg_part[0:end_index_start_txt]
                known_end_text = known_msg_part[start_index_end_txt:]
                known_filename_txt = known_msg_part[
                                   end_index_start_txt + 1:
                                   start_index_end_txt - 1]
                # find the same piece part in the result text
                msg_body_index = msg_body.find(known_end_text)
                if msg_body_index < 0:
                    continue
                msg_body_end_index = (msg_body_index
                                      + len(known_end_text) - 1)
                msg_body_start_index = (msg_body_end_index
                                        - known_msg_part_len)
                msg_body_part = msg_body[msg_body_start_index:
                                         msg_body_end_index]
                pieces_found = True
                # look for all known filename pieces in the msg_body_part
                for filename in known_filename_txt.split(','):
                    if filename not in msg_body_part:
                        pieces_found = False
                if known_start_text not in msg_body_part:
                    pieces_found = False
                if pieces_found:
                    known_msg_body_index[i] = msg_body_start_index
            comparison = \
                all([i > -1 for i in known_msg_body_index])
            assert comparison is True
    else:
        assert len(''.join(known_msg_body)) == len(msg_body)


def get_report_link_base():
    email_gen_obj = EmailGen()
    url_pieces = email_gen_obj.report_link_txt.split('/')
    return url_pieces[2]


def get_review_text():
    email_gen_obj = EmailGen()
    return email_gen_obj.email_subject_action


def get_ui_base_url():
    email_gen_obj = EmailGen()
    return email_gen_obj.ui_prefix


ui_url = get_ui_base_url()
link = get_report_link_base()
summary_text = get_review_text()
detail_results = {
    # All Good, processID = 14696, ver = 0_4_19, updateID =
    'token1': {
        'code_version': '0.4.19',
        'jira_issue_info_parsed': [
            'US-PFa', '14696', '20180101-20190101',
            'siteteammember1',
            (f'Format Results - {summary_text["good"]} | US-PFa data '
                'uploaded on Jul 16, 2018'),
            'Quarterly update PFA Jul 2018'],
        'description_txt': (
            'QAQC completed with the following results:\n'
            'US-PFa_HR_201801010000_201901010000.csv: critical(0), '
            'error(0), warning(0), ok(18)'),
        'msg_body': [
            'Dear SiteTeamMember1,\n\nThank you for uploading data for US-PFa '
            'on Jul 16, 2018 (see complete file list below).\n\nFormat QA/QC '
            'assesses the compliance of your data submission with AmeriFlux '
            'FP-In format. This step is critical to ensure that your data '
            'will be processed correctly. Details about the format '
            f'requirements can be found at {ui_url}'
            'half-hourly-hourly-data-upload-format/. Data that passes the '
            'Format QA/QC checks will be automatically queued for Data QA/QC, '
            'the next step in the AmeriFlux data processing pipeline.\n\nWe '
            'have processed your data through our Format QA/QC scheme. The '
            'results are listed below.\n\n\n*ALL IS GOOD*\nYour uploaded '
            'data comply with AmeriFlux FP-In format. Awesome! Data QA/QC '
            'will be run next to determine if there are any issues with the '
            'data in the file(s). Data QA/QC results will be sent in a '
            'separate email.\n\n\nView the status of your uploaded files at '
            f'{ui_url}qaqc-reports-data-team/. '
            'Links to view the Format QA/QC report for each file are at the '
            'end of this email.\n\nWe appreciate your help with standardizing '
            'the data submission format. Please reply to this email with any '
            'questions. You can track communications on this Format QA/QC '
            'report at QAQC-9999 using your AmeriFlux account ID and '
            'password to login.\n\nSincerely,\nAMP Data Team\n\n*List of '
            'uploaded file(s) and corresponding Format QA/QC Report link:'
            f'*\nUS-PFa_HR_201801010000_201901010000.csv: https://{link}'
            '/qaqc-report/?site_id=US-PFa&report_id=14696']
        },
    # WARNING no AutoCorrect, processID = 13278, ver = 0_4_16, updateID =
    'token2': {
        'code_version': '0.4.16',
        'jira_issue_info_parsed': [
            'US-PFa',
            ('13278\n13279\n13280\n13281\n13282\n13283\n13284\n13285\n13286\n'
                '13287\n13288\n13289\n13290\n13291\n13292\n13293\n13294\n'
                '13295\n13296\n13297\n13298\n13299\n13300'),
            ('19950101-19960101\n19960101-19970101\n19970101-19980101\n'
                '19980101-19990101\n19990101-20000101\n20000101-20010101\n'
                '20010101-20020101\n20020101-20030101\n20030101-20040101\n'
                '20040101-20050101\n20050101-20060101\n20060101-20070101\n'
                '20070101-20080101\n20080101-20090101\n20090101-20100101\n'
                '20100101-20110101\n20110101-20120101\n20120101-20130101\n'
                '20130101-20140101\n20140101-20150101\n20150101-20160101\n'
                '20160101-20170101\n20170101-20180101'),
            'siteteammember2',
            (f'Format Results - {summary_text["review"]} | US-PFa data '
                'uploaded on Feb 26, 2018'),
            ('1. [No non-gap-filled PPFD_IN, P, SC] - these values do have '
                'some gaps, so I removed the _F label. We merge some data '
                'in, but it is not gap filled.\n2. [Lower upper bound in '
                'PPFD_IN_F_1_1_1 in last 2 years] - I have developed a new '
                'PAR product based on updated calibrations that I think '
                'handles a low bias in all years. Hopefully it is better!')],
        'description_txt': (
            'Files were uploaded from archival file: '
            'US-PFa-19952017-02262018.zip.\n\n'
            'QAQC completed with the following results:\n'
            'US-PFa_HR_199501010000_199601010000.csv: critical(0), '
            'error(0), warning(2), ok(16)\nUS-PFa_HR_199601010000_'
            '199701010000.csv: critical(0), error(0), warning(2), '
            'ok(16)\nUS-PFa_HR_199701010000_199801010000.csv: '
            'critical(0), error(0), warning(2), ok(16)\nUS-PFa_HR_'
            '199801010000_199901010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_199901010000_200001010000.'
            'csv: critical(0), error(0), warning(2), ok(16)\nUS-PFa_HR_'
            '200001010000_200101010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_200101010000_200201010000.'
            'csv: critical(0), error(0), warning(2), ok(16)\nUS-PFa_HR_'
            '200201010000_200301010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_200301010000_200401010000.'
            'csv: critical(0), error(0), warning(2), ok(16)\nUS-PFa_HR_'
            '200401010000_200501010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_200501010000_200601010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200601010000_200701010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200701010000_200801010000.'
            'csv: critical(0), error(0), warning(2), ok(16)\nUS-PFa_HR_'
            '200801010000_200901010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_200901010000_201001010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201001010000_201101010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_201101010000_201201010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201201010000_201301010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_201301010000_201401010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201401010000_201501010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_201501010000_201601010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201601010000_201701010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_201701010000_201801010000.'
            'csv: critical(0), error(0), warning(1), ok(17)'),
        'msg_body': [
            ('Dear SiteTeamMember2,\n\nThank you for uploading data for '
                'US-PFa on Feb 26, 2018 (see complete file list below).\n\n'
                'Format QA/QC assesses the compliance of your data submission'
                ' with AmeriFlux FP-In format. This step is critical to '
                'ensure that your data will be processed correctly. Details '
                'about the format requirements can be found at '
                f'{ui_url}half-hourly-hourly-data-upload-format/. Data '
                'that passes the Format QA/QC checks will be automatically '
                'queued for Data QA/QC, the next step in the AmeriFlux data '
                'processing pipeline.\n\nWe have processed your data through '
                'our Format QA/QC scheme. The results are listed below.\n\n'
                'The data in these Format QA/QC results were uploaded in an '
                'archival file format (e.g., zip, 7z). See file list at end '
                'of email to verify that all expected files are '
                'included.\n\n\n*REVIEW REQUESTED*\nINSERT ISSUE-SPECIFIC '
                'TEXT. You can re-upload your data at '
                f'{ui_url}data/upload-data/ and/or reply to this email to '
                'discuss with us. We fixed issues where possible as detailed '
                'below to attempt to prepare the file for Data QA/QC. Data '
                'QA/QC results will be sent in a separate email.\n\n\n'),
            ('These potential issues were encountered in the following files '
                'US-PFa_HR_199801010000_199901010000.csv, US-PFa_HR_'
                '199901010000_200001010000.csv, US-PFa_HR_201301010000_'
                '201401010000.csv, US-PFa_HR_200201010000_200301010000.csv, '
                'US-PFa_HR_199501010000_199601010000.csv, US-PFa_HR_'
                '200101010000_200201010000.csv, US-PFa_HR_200701010000_'
                '200801010000.csv, US-PFa_HR_201401010000_201501010000.csv, '
                'US-PFa_HR_201201010000_201301010000.csv, US-PFa_HR_'
                '199701010000_199801010000.csv, US-PFa_HR_201701010000_'
                '201801010000.csv, US-PFa_HR_200001010000_200101010000.'
                'csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: PPFD_IN_1_1_1, P. If '
                'these variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables are '
                'accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-PFa_HR_'
                '199501010000_199601010000.csv:\n* These variables have no '
                'data for file\'s entire time period: CH4_1_1_1, CH4_1_2_1, '
                'CH4_1_3_1, FC_1_1_1, FC_1_2_1, FC_1_3_1, FCH4_1_1_1, '
                'SC_1_1_1, SC_1_2_1, SC_1_3_1, SCH4_1_1_1, H, H_1_1_1, '
                'H_1_2_1, H_1_3_1, LE, LE_1_1_1, LE_1_2_1, LE_1_3_1, '
                'SH_1_1_1, SH_1_2_1, SH_1_3_1, SLE_1_1_1, SLE_1_2_1, '
                'SLE_1_3_1, USTAR_1_1_1, USTAR_1_2_1, USTAR_1_3_1, '
                'SWC_1_1_1, NEE, NEE_F, NEE_1_1_1, NEE_1_2_1, NEE_1_3_1, '
                'RECO_F, GPP_F. Previously uploaded data with the same time '
                'period will be overwritten.\n\n'),
            ('These potential issues were '
                'encountered in the following files US-PFa_HR_201001010000_'
                '201101010000.csv, US-PFa_HR_200301010000_200401010000.csv, '
                'US-PFa_HR_201101010000_201201010000.csv, US-PFa_HR_'
                '199601010000_199701010000.csv, US-PFa_HR_201601010000_'
                '201701010000.csv:\n* These variables are suspected to be '
                'gap-filled because they have no missing values: '
                'PPFD_IN_1_1_1. If these variables are gap-filled, please '
                'use the _F variable qualifier. While gap-filled versions of '
                'these variables are accepted, non-filled data must be '
                'submitted for primary flux variables (FC, FCH4, LE, H). '
                'Please also consider submitting non-filled data for all '
                'other variables.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_199801010000_199901010000.'
                'csv, US-PFa_HR_199901010000_200001010000.csv, US-PFa_HR_'
                '199601010000_199701010000.csv, US-PFa_HR_200101010000_'
                '200201010000.csv, US-PFa_HR_199701010000_199801010000.csv, '
                'US-PFa_HR_200001010000_200101010000.csv:\n* These variables '
                'have no data for file\'s entire time period: CH4_1_1_1, '
                'CH4_1_2_1, CH4_1_3_1, FCH4_1_1_1, SCH4_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_200301010000_200401010000.'
                'csv, US-PFa_HR_200201010000_200301010000.csv, US-PFa_HR_'
                '200401010000_200501010000.csv, US-PFa_HR_200601010000_'
                '200701010000.csv, US-PFa_HR_200701010000_200801010000.csv, '
                'US-PFa_HR_200901010000_201001010000.csv, US-PFa_HR_'
                '200801010000_200901010000.csv:\n* These variables have no '
                'data for file\'s entire time period: CH4_1_1_1, CH4_1_2_1, '
                'CH4_1_3_1, FCH4_1_1_1, SCH4_1_1_1, SWC_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_200401010000_200501010000.'
                'csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: P. If these variables '
                'are gap-filled, please use the _F variable qualifier. While '
                'gap-filled versions of these variables are accepted, '
                'non-filled data must be submitted for primary flux '
                'variables (FC, FCH4, LE, H). Please also consider submitting '
                'non-filled data for all other variables.\n\nThese potential '
                'issues were encountered in the following files US-PFa_HR_'
                '200501010000_200601010000.csv:\n* These variables have no '
                'data for file\'s entire time period: H2O_1_1_1, CH4_1_1_1, '
                'CH4_1_2_1, CH4_1_3_1, FCH4_1_1_1, SCH4_1_1_1, LE_1_1_1, '
                'WD_1_2_1, WD_1_3_1, TA_1_1_1, SWC_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_200801010000_200901010000.'
                'csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: SC_1_1_1, SC_1_2_1. If '
                'these variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables are '
                'accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-PFa_HR_'
                '201001010000_201101010000.csv:\n* These variables have no '
                'data for file\'s entire time period: SWC_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_201201010000_201301010000.'
                'csv:\n* These variables have no data for file\'s entire '
                'time period: WD_1_1_1. Previously uploaded data with the '
                'same time period will be overwritten.\n\n'),
            ('These potential issues were encountered '
                'in the following files US-PFa_HR_'
                '201501010000_201601010000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: H2O_1_1_1, H2O_1_2_1, H2O_1_3_1, SC_1_1_1, '
                'SC_1_2_1, SC_1_3_1, SH_1_3_1, TA_1_1_1, TA_1_2_1, TA_1_3_1. '
                'If these variables are gap-filled, please use the _F '
                'variable qualifier. While gap-filled versions of these '
                'variables are accepted, non-filled data must be submitted '
                'for primary flux variables (FC, FCH4, LE, H). Please also '
                'consider submitting non-filled data for all other '
                'variables.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_201601010000_201701010000.'
                'csv:\n* These variables have no data for file\'s entire '
                'time period: FC_1_1_1, H_1_1_1, LE_1_1_1, WD_1_1_1, '
                'WS_1_1_1, USTAR_1_1_1, NEE_1_1_1. Previously uploaded data '
                'with the same time period will be overwritten.\n\n'),
            ('View the '
                'status of your uploaded files at '
                f'{ui_url}qaqc-reports-data-team/. Links to view the '
                'Format QA/QC report for each file are at the end of this '
                'email.\n\nWe appreciate your help with standardizing the '
                'data submission format. We hope that fixing any identified '
                'issues will not take too much time from your work, but it is '
                'necessary to enable timely data processing. Please reply to '
                'this email with any questions. You can track communications '
                'on this Format QA/QC report at QAQC-9999 using your '
                'AmeriFlux account ID and password to login.\n\nSincerely,\n'
                'AMP Data Team\n\n*List of uploaded file(s) and corresponding '
                'Format QA/QC Report link:*\nThese files were extracted from '
                'the uploaded archival file: US-PFa-19952017-02262018.zip.\n'
                f'US-PFa_HR_199501010000_199601010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13278\n'
                f'US-PFa_HR_199601010000_199701010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13279\n'
                f'US-PFa_HR_199701010000_199801010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13280\n'
                f'US-PFa_HR_199801010000_199901010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13281\n'
                f'US-PFa_HR_199901010000_200001010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13282\n'
                f'US-PFa_HR_200001010000_200101010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13283\n'
                f'US-PFa_HR_200101010000_200201010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13284\n'
                f'US-PFa_HR_200201010000_200301010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13285\n'
                f'US-PFa_HR_200301010000_200401010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13286\n'
                f'US-PFa_HR_200401010000_200501010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13287\n'
                f'US-PFa_HR_200501010000_200601010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13288\n'
                f'US-PFa_HR_200601010000_200701010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13289\n'
                f'US-PFa_HR_200701010000_200801010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13290\n'
                f'US-PFa_HR_200801010000_200901010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13291\n'
                f'US-PFa_HR_200901010000_201001010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13292\n'
                f'US-PFa_HR_201001010000_201101010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13293\n'
                f'US-PFa_HR_201101010000_201201010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13294\n'
                f'US-PFa_HR_201201010000_201301010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13295\n'
                f'US-PFa_HR_201301010000_201401010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13296\n'
                f'US-PFa_HR_201401010000_201501010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13297\n'
                f'US-PFa_HR_201501010000_201601010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13298\n'
                f'US-PFa_HR_201601010000_201701010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13299\n'
                f'US-PFa_HR_201701010000_201801010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=13300')]
        },
    # ZIP All Good, processID = , ver = 0_4_6, updateID =
    'token3': {
        'code_version': '0.4.6',
        'jira_issue_info_parsed': [
            'US-PFa',
            ('9424\n9425\n9426\n9427\n9428\n9429\n9430\n9431\n9432\n9433\n'
                '9434\n9435\n9436\n9437\n9438\n9439\n9440\n9441\n9442\n'
                '9443\n9444\n9445\n9446'),
            ('19950101-19960101\n19960101-19970101\n19970101-19980101\n'
                '19980101-19990101\n19990101-20000101\n20000101-20010101\n'
                '20010101-20020101\n20020101-20030101\n20030101-20040101\n'
                '20040101-20050101\n20050101-20060101\n20060101-20070101\n'
                '20070101-20080101\n20080101-20090101\n20090101-20100101\n'
                '20100101-20110101\n20110101-20120101\n20120101-20130101\n'
                '20130101-20140101\n20140101-20150101\n20150101-20160101\n'
                '20160101-20170101\n20170101-20180101'),
            'siteteammember3',
            (f'Format Results - {summary_text["good"]} | US-PFa data '
                'uploaded on Sep 11, 2017'),
            ('1995 to 2017 fluxes, corrections based on AMP QA/QC '
                'report, plus data update to Sep 2017.')],
        'description_txt': (
            'Files were uploaded from archival file: '
            'US-PFa-19952017-09102017.zip.\n\nQAQC completed with the '
            'following results:\nUS-PFa_HR_199501010000_199601010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\n'
            'US-PFa_HR_199601010000_199701010000.csv: critical(0), '
            'error(0), warning(1), ok(17)\nUS-PFa_HR_199701010000_'
            '199801010000.csv: critical(0), error(0), warning(1), '
            'ok(17)\nUS-PFa_HR_199801010000_199901010000.csv: '
            'critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '199901010000_200001010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200001010000_200101010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200101010000_200201010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200201010000_200301010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200301010000_200401010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200401010000_200501010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200501010000_200601010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200601010000_200701010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200701010000_200801010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_200801010000_200901010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '200901010000_201001010000.csv: critical(0), error(0), '
            'warning(1), ok(17)\nUS-PFa_HR_201001010000_201101010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201101010000_201201010000.csv: critical(0), error(0), '
            'warning(0), ok(18)\nUS-PFa_HR_201201010000_201301010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201301010000_201401010000.csv: critical(0), error(0), '
            'warning(0), ok(18)\nUS-PFa_HR_201401010000_201501010000.'
            'csv: critical(0), error(0), warning(0), ok(18)\nUS-PFa_HR_'
            '201501010000_201601010000.csv: critical(0), error(0), '
            'warning(2), ok(16)\nUS-PFa_HR_201601010000_201701010000.'
            'csv: critical(0), error(0), warning(1), ok(17)\nUS-PFa_HR_'
            '201701010000_201801010000.csv: critical(0), error(0), '
            'warning(0), ok(18)'),
        'msg_body': [
            ('Dear SiteTeamMember3,\n\nThank you for uploading data for '
                'US-PFa on Sep 11, 2017 (see complete file list below).\n\n'
                'Format QA/QC assesses the compliance of your data submission'
                ' with AmeriFlux FP-In format. This step is critical to '
                'ensure that your data will be processed correctly. Details '
                'about the format requirements can be found at '
                f'{ui_url}half-hourly-hourly-data-upload-format/. Data '
                'that passes the Format QA/QC checks will be automatically '
                'queued for Data QA/QC, the next step in the AmeriFlux data '
                'processing pipeline.\n\nWe have processed your data through '
                'our Format QA/QC scheme. The results are listed below.\n\n'
                'The data in these Format QA/QC results were uploaded in an '
                'archival file format (e.g., zip, 7z). See file list at end '
                'of email to verify that all expected files are '
                'included.\n\n\n*REVIEW REQUESTED*\nINSERT ISSUE-SPECIFIC '
                'TEXT. You can re-upload your data at '
                f'{ui_url}data/upload-data/ and/or reply to this email to '
                'discuss with us. We fixed issues where possible as detailed '
                'below to attempt to prepare the file for Data QA/QC. Data '
                'QA/QC results will be sent in a separate email.\n\n\n'),
            ('These potential issues were encountered in the following files '
                'US-PFa_HR_199501010000_199601010000.csv:\n* These variables '
                'have no data for file\'s entire time period: CH4_1_1_1, '
                'CH4_1_2_1, CH4_1_3_1, FC_1_1_1, FC_1_2_1, FC_1_3_1, '
                'FCH4_1_1_1, SC_F_1_1_1, SC_F_1_2_1, SC_F_1_3_1, '
                'SCH4_F_1_1_1, H, H_1_1_1, H_1_2_1, H_1_3_1, LE, LE_1_1_1, '
                'LE_1_2_1, LE_1_3_1, SH_F_1_1_1, SH_F_1_2_1, SH_F_1_3_1, '
                'SLE_F_1_1_1, SLE_F_1_2_1, SLE_F_1_3_1, USTAR_1_1_1, '
                'USTAR_1_2_1, USTAR_1_3_1, SWC_1_1_1, NEE, NEE_F, NEE_1_1_1, '
                'NEE_1_2_1, NEE_1_3_1, RECO_F, GPP_F. Previously uploaded '
                'data with the same time period will be overwritten.\n\n'),
            ('These potential issues were encountered in the following '
                'files US-PFa_HR_199701010000_199801010000.csv, US-PFa_HR_'
                '200101010000_200201010000.csv, US-PFa_HR_199901010000_'
                '200001010000.csv, US-PFa_HR_200001010000_200101010000.csv, '
                'US-PFa_HR_199801010000_199901010000.csv, US-PFa_HR_'
                '199601010000_199701010000.csv:\n* These variables have no '
                'data for file\'s entire time period: CH4_1_1_1, CH4_1_2_1, '
                'CH4_1_3_1, FCH4_1_1_1, SCH4_F_1_1_1. Previously uploaded '
                'data with the same time period will be overwritten.\n\n'),
            ('These potential issues were encountered in the following '
                'files US-PFa_HR_200801010000_200901010000.csv, US-PFa_HR_'
                '200301010000_200401010000.csv, US-PFa_HR_200601010000_'
                '200701010000.csv, US-PFa_HR_200901010000_201001010000.csv, '
                'US-PFa_HR_200701010000_200801010000.csv, US-PFa_HR_'
                '200201010000_200301010000.csv, US-PFa_HR_200401010000_'
                '200501010000.csv:\n* These variables have no data for '
                'file\'s entire time period: CH4_1_1_1, CH4_1_2_1, '
                'CH4_1_3_1, FCH4_1_1_1, SCH4_F_1_1_1, SWC_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_200501010000_200601010000.'
                'csv:\n* These variables have no data for file\'s entire '
                'time period: H2O_1_1_1, CH4_1_1_1, CH4_1_2_1, CH4_1_3_1, '
                'FCH4_1_1_1, SCH4_F_1_1_1, LE_1_1_1, WD_1_2_1, WD_1_3_1, '
                'TA_1_1_1, SWC_1_1_1. Previously uploaded data with the same '
                'time period will be overwritten.\n\n'),
            ('These potential issues '
                'were encountered in the following files US-PFa_HR_'
                '201001010000_201101010000.csv:\n* These variables have no '
                'data for file\'s entire time period: SWC_1_1_1. Previously '
                'uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-PFa_HR_201201010000_201301010000.'
                'csv:\n* These variables have no data for file\'s entire '
                'time period: WD_1_1_1. Previously uploaded data with the '
                'same time period will be overwritten.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-PFa_HR_'
                '201501010000_201601010000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: H2O_1_1_1, H2O_1_2_1, H2O_1_3_1, TA_1_1_1, '
                'TA_1_2_1, TA_1_3_1. If these variables are gap-filled, '
                'please use the _F variable qualifier. While gap-filled '
                'versions of these variables are accepted, non-filled data '
                'must be submitted for primary flux variables (FC, FCH4, LE, '
                'H). Please also consider submitting non-filled data for all '
                'other variables.\n* These variables have no data for '
                'file\'s entire time period: WS_F_1_3_1. Previously uploaded '
                'data with the same time period will be overwritten.\n\n'),
            ('These potential issues were encountered in the following '
                'files US-PFa_HR_201601010000_201701010000.csv:\n* These '
                'variables have no data for file\'s entire time period: '
                'FC_1_1_1, H_1_1_1, LE_1_1_1, WD_1_1_1, WS_1_1_1, '
                'USTAR_1_1_1, NEE_1_1_1. Previously uploaded data with the '
                'same time period will be overwritten.\n\n'),
            ('View the status of '
                f'your uploaded files at {ui_url}'
                'qaqc-reports-data-team/. Links to view the Format QA/QC '
                'report for each file are at the end of this email.\n\nWe '
                'appreciate your help with standardizing the data submission '
                'format. We hope that fixing any identified issues will not '
                'take too much time from your work, but it is necessary to '
                'enable timely data processing. Please reply to this email '
                'with any questions. You can track communications on this '
                'Format QA/QC report at QAQC-9999 using your AmeriFlux '
                'account ID and password to login.\n\nSincerely,\nAMP Data '
                'Team\n\n*List of uploaded file(s) and corresponding Format '
                'QA/QC Report link:*\nThese files were extracted from the '
                'uploaded archival file: US-PFa-19952017-09102017.zip.\n'
                f'US-PFa_HR_199501010000_199601010000.csv: https://{link}'
                '/qaqc-report/?site_id=US-PFa&report_id=9424\nUS-PFa_'
                f'HR_199601010000_199701010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9425\nUS-PFa_HR_'
                f'199701010000_199801010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9426\nUS-PFa_HR_'
                f'199801010000_199901010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9427\nUS-PFa_HR_'
                f'199901010000_200001010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9428\nUS-PFa_HR_'
                f'200001010000_200101010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9429\nUS-PFa_HR_'
                f'200101010000_200201010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9430\nUS-PFa_HR_'
                f'200201010000_200301010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9431\nUS-PFa_HR_'
                f'200301010000_200401010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9432\nUS-PFa_HR_'
                f'200401010000_200501010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9433\nUS-PFa_HR_'
                f'200501010000_200601010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9434\nUS-PFa_HR_'
                f'200601010000_200701010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9435\nUS-PFa_HR_'
                f'200701010000_200801010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9436\nUS-PFa_HR_'
                f'200801010000_200901010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9437\nUS-PFa_HR_'
                f'200901010000_201001010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9438\nUS-PFa_HR_'
                f'201001010000_201101010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9439\nUS-PFa_HR_'
                f'201101010000_201201010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9440\nUS-PFa_HR_'
                f'201201010000_201301010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9441\nUS-PFa_HR_'
                f'201301010000_201401010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9442\nUS-PFa_HR_'
                f'201401010000_201501010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9443\nUS-PFa_HR_'
                f'201501010000_201601010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9444\nUS-PFa_HR_'
                f'201601010000_201701010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9445\nUS-PFa_HR_'
                f'201701010000_201801010000.csv: https://{link}/'
                'qaqc-report/?site_id=US-PFa&report_id=9446')]
        },
    # WARNING AutoCorrect, processID = 26761, ver = 0_4_23, updateID =
    'token4': {
        'code_version': '0.4.23',
        'jira_issue_info_parsed': [
            'US-MOz',
            ('26761\n26762\n26763\n26764\n26765\n26766\n26767\n26768\n26769\n'
                '26770\n26771\n26772\n26773\n26774'),
            ('20050101-20060101\n20040101-20050101\n20090101-20100101\n'
                '20070101-20080101\n20060101-20070101\n20100101-20110101\n'
                '20130101-20140101\n20120101-20130101\n20080101-20090101\n'
                '20160101-20170101\n20110101-20120101\n20150101-20160101\n'
                '20170101-20180101\n20140101-20150101'),
            'siteteammember4',
            (f'Format Results - {summary_text["review"]} | US-MOz data '
                'uploaded on Aug 15, 2018'),
            'Re-upload of all data'],
        'description_txt':
            ('QAQC completed with the following results:\nUS-MOz_HH_'
             '20040101000000_20050101000000.csv: critical(0), error(1), '
             'warning(3), ok(16)\nAutomated fix for US-MOz_HH_'
             '20040101000000_20050101000000.csv: critical(0), error(0), '
             'warning(1), ok(17)\nUS-MOz_HH_20050101000000_'
             '20060101000000.csv: critical(0), error(1), warning(4), '
             'ok(15)\nAutomated fix for US-MOz_HH_20050101000000_'
             '20060101000000.csv: critical(0), error(0), warning(2), '
             'ok(16)\nUS-MOz_HH_20060101000000_20070101000000.csv: '
             'critical(0), error(1), warning(3), ok(16)\nAutomated fix '
             'for US-MOz_HH_20060101000000_20070101000000.csv: '
             'critical(0), error(0), warning(1), ok(17)\nUS-MOz_HH_'
             '20070101000000_20080101000000.csv: critical(0), error(1), '
             'warning(3), ok(16)\nAutomated fix for US-MOz_HH_'
             '20070101000000_20080101000000.csv: critical(0), error(0), '
             'warning(1), ok(17)\nUS-MOz_HH_20080101000000_'
             '20090101000000.csv: critical(0), error(1), warning(2), '
             'ok(17)\nAutomated fix for US-MOz_HH_20080101000000_'
             '20090101000000.csv: critical(0), error(0), warning(0), '
             'ok(18)\nUS-MOz_HH_20090101000000_20100101000000.csv: '
             'critical(0), error(1), warning(3), ok(16)\nAutomated fix '
             'for US-MOz_HH_20090101000000_20100101000000.csv: '
             'critical(0), error(0), warning(1), ok(17)\nUS-MOz_HH_'
             '20100101000000_20110101000000.csv: critical(0), error(1), '
             'warning(3), ok(16)\nAutomated fix for US-MOz_HH_'
             '20100101000000_20110101000000.csv: critical(0), error(0), '
             'warning(1), ok(17)\nUS-MOz_HH_20110101000000_'
             '20120101000000.csv: critical(0), error(1), warning(2), '
             'ok(17)\nAutomated fix for US-MOz_HH_20110101000000_'
             '20120101000000.csv: critical(0), error(0), warning(0), '
             'ok(18)\nUS-MOz_HH_20120101000000_20130101000000.csv: '
             'critical(0), error(1), warning(2), ok(17)\nAutomated fix '
             'for US-MOz_HH_20120101000000_20130101000000.csv: '
             'critical(0), error(0), warning(0), ok(18)\nUS-MOz_HH_'
             '20130101000000_20140101000000.csv: critical(0), error(1), '
             'warning(2), ok(17)\nAutomated fix for US-MOz_HH_'
             '20130101000000_20140101000000.csv: critical(0), error(0), '
             'warning(0), ok(18)\nUS-MOz_HH_20140101000000_'
             '20150101000000.csv: critical(0), error(1), warning(3), '
             'ok(16)\nAutomated fix for US-MOz_HH_20140101000000_'
             '20150101000000.csv: critical(0), error(0), warning(1), '
             'ok(17)\nUS-MOz_HH_20150101000000_20160101000000.csv: '
             'critical(0), error(1), warning(3), ok(16)\nAutomated fix '
             'for US-MOz_HH_20150101000000_20160101000000.csv: '
             'critical(0), error(0), warning(1), ok(17)\nUS-MOz_HH_'
             '20160101000000_20170101000000.csv: critical(0), error(1), '
             'warning(3), ok(16)\nAutomated fix for US-MOz_HH_'
             '20160101000000_20170101000000.csv: critical(0), error(0), '
             'warning(1), ok(17)\nUS-MOz_HH_20170101000000_'
             '20180101000000.csv: critical(0), error(1), warning(3), '
             'ok(16)\nAutomated fix for US-MOz_HH_20170101000000_'
             '20180101000000.csv: critical(0), error(0), warning(1), '
             'ok(17)'),
        'msg_body': [
            ('Dear SiteTeamMember4,\n\nThank you for uploading data for '
                'US-MOz on Aug 15, 2018 (see complete file list below).\n\n'
                'Format QA/QC assesses the compliance of your data '
                'submission with AmeriFlux FP-In format. '
                'This step is critical to '
                'ensure that your data will be processed correctly. '
                'Details about the format requirements can be found '
                f'at {ui_url}half-hourly-hourly-data-upload-format/. Data '
                'that passes the Format QA/QC checks will be automatically '
                'queued for Data QA/QC, the next step in the AmeriFlux data '
                'processing pipeline.\n\nWe have processed your data through '
                'our Format QA/QC scheme. The results are listed below.\n\n\n'
                '*REVIEW REQUESTED*\nINSERT ISSUE-SPECIFIC TEXT. You can '
                f're-upload your data at {ui_url}'
                'data/upload-data/ and/or reply to this email to discuss with '
                'us. We fixed issues where possible as detailed below to '
                'attempt to prepare the file for Data QA/QC. Data QA/QC '
                'results will be sent in a separate email.\n\n\n'),
            ('These potential issues '
                'were encountered in the following files US-MOz_HH_'
                '20050101000000_20060101000000.csv, US-MOz_HH_20040101000000_'
                '20050101000000.csv:\n* These variables have no data for '
                'file\'s entire time period: FC_1_1_1, LE_1_1_1, H_1_1_1. '
                'Previously uploaded data with the same time period will be '
                'overwritten.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-MOz_HH_20050101000000_20060101000000'
                '.csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: P_1_1_1. If these '
                'variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables '
                'are accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20060101000000_20070101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: P_1_1_1, PPFD_IN_1_1_1, PPFD_OUT_1_1_1, TA_1_1_1, '
                'RH_1_1_1, WS_1_1_1, TS_1_1_1, PA_1_1_1. If these variables '
                'are gap-filled, please use the _F variable qualifier. While '
                'gap-filled versions of these variables are accepted, '
                'non-filled data must be submitted for primary flux '
                'variables (FC, FCH4, LE, H). Please also consider submitting '
                'non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20070101000000_20080101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: TS_1_1_1. If these variables are gap-filled, please '
                'use the _F variable qualifier. While gap-filled versions of '
                'these variables are accepted, non-filled data must be '
                'submitted for primary flux variables (FC, FCH4, LE, H). '
                'Please also consider submitting non-filled data for all '
                'other variables.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-MOz_HH_20090101000000_20100101000000.'
                'csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: P_1_1_1, '
                'PPFD_IN_1_1_1, PPFD_OUT_1_1_1, TA_1_1_1, RH_1_1_1, '
                'WS_1_1_1. If these variables are gap-filled, please use the '
                '_F variable qualifier. While gap-filled versions of these '
                'variables are accepted, non-filled data must be submitted '
                'for primary flux variables (FC, FCH4, LE, H). Please also '
                'consider submitting non-filled data for all other '
                'variables.\n\n'),
            ('These potential issues were encountered in '
                'the following files US-MOz_HH_20100101000000_20110101000000.'
                'csv:\n* These variables are suspected to be gap-filled '
                'because they have no missing values: P_1_1_1, TS_1_1_1. If '
                'these variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables '
                'are accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20140101000000_20150101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: P_1_1_1, SWC_1_1_1, G_1_1_1. If these variables '
                'are gap-filled, please use the _F variable qualifier. '
                'While gap-filled versions of these variables are accepted, '
                'non-filled data must be submitted for primary flux '
                'variables (FC, FCH4, LE, H). Please also consider submitting '
                'non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20150101000000_20160101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: TS_1_1_1, SWC_1_1_1, G_1_1_1, PA_1_1_1. If these '
                'variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables '
                'are accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20160101000000_20170101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: P_1_1_1, TS_1_1_1, SWC_1_1_1, G_1_1_1. If these '
                'variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables '
                'are accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n\n'),
            ('These potential '
                'issues were encountered in the following files US-MOz_HH_'
                '20170101000000_20180101000000.csv:\n* These variables are '
                'suspected to be gap-filled because they have no missing '
                'values: SWC_1_1_1, G_1_1_1, PA_1_1_1. If these variables '
                'are gap-filled, please use the _F variable qualifier. While '
                'gap-filled versions of these variables are accepted, '
                'non-filled data must be submitted for primary flux '
                'variables (FC, FCH4, LE, H). Please also consider submitting '
                'non-filled data for all other variables.\n\n'),
            ('These automatic '
                'fixes were attempted to address issues encountered in the '
                'following files US-MOz_HH_20160101000000_20170101000000.'
                'csv, US-MOz_HH_20120101000000_20130101000000.csv, '
                'US-MOz_HH_20040101000000_20050101000000.csv, US-MOz_HH_'
                '20140101000000_20150101000000.csv, US-MOz_HH_20070101000000_'
                '20080101000000.csv, US-MOz_HH_20100101000000_20110101000000'
                '.csv, US-MOz_HH_20110101000000_20120101000000.csv, US-MOz_'
                'HH_20090101000000_20100101000000.csv, US-MOz_HH_'
                '20170101000000_20180101000000.csv, US-MOz_HH_20050101000000_'
                '20060101000000.csv, US-MOz_HH_20130101000000_20140101000000'
                '.csv, US-MOz_HH_20150101000000_20160101000000.csv, US-MOz_'
                'HH_20080101000000_20090101000000.csv, US-MOz_HH_'
                '20060101000000_20070101000000.csv:\n* Filename components '
                'fixed: ts-start (start time); ts-end (end time)\n\nPlease '
                'correct these issues in subsequent data submissions.\n\n\n'),
            ('View the status of your uploaded files at '
                f'{ui_url}qaqc-reports-data-team/. Links to view the '
                'Format QA/QC report for each file are at the end of this '
                'email.\n\nWe appreciate your help with standardizing the '
                'data submission format. We hope that fixing any identified '
                'issues will not take too much time from your work, but '
                'it is necessary to enable timely data processing. Please '
                'reply to this email with any questions. You can track '
                'communications on this Format QA/QC report at QAQC-9999 '
                'using your AmeriFlux account ID and password to login.\n\n'
                'Sincerely,\nAMP Data Team\n\n*List of uploaded file(s) and '
                'corresponding Format QA/QC Report link:*\nUS-MOz_HH_'
                f'20040101000000_20050101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26762\nUS-MOz_HH_'
                f'20050101000000_20060101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26761\nUS-MOz_HH_'
                f'20060101000000_20070101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26765\nUS-MOz_HH_'
                f'20070101000000_20080101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26764\nUS-MOz_HH_'
                f'20080101000000_20090101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26769\nUS-MOz_HH_'
                f'20090101000000_20100101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26763\nUS-MOz_HH_'
                f'20100101000000_20110101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26766\nUS-MOz_HH_'
                f'20110101000000_20120101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26771\nUS-MOz_HH_'
                f'20120101000000_20130101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26768\nUS-MOz_HH_'
                f'20130101000000_20140101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26767\nUS-MOz_HH_'
                f'20140101000000_20150101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26774\nUS-MOz_HH_'
                f'20150101000000_20160101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26772\nUS-MOz_HH_'
                f'20160101000000_20170101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26770\nUS-MOz_HH_'
                f'20170101000000_20180101000000.csv: https://{link}'
                '/qaqc-report/?site_id=US-MOz&report_id=26773')]
        },
    # FAIL AutoCorrect, processID = 36915, ver = 0_4_32, updateID =
    'token5': {
        'code_version': '0.4.32',
        'jira_issue_info_parsed': [
            'CA-DBB', '36915', 'None-None',
            'siteteammember5',
            (f'Format Results - {summary_text["action"]} | CA-DBB data '
                'uploaded on May 29, 2019'),
            ('We have uploaded a revised version to correct the issues '
                'raised during the initial data QC/QA process.')],
        'description_txt': (
            'QAQC completed with the following results:\nCA-DBB_HH_'
            '201406010000_201808312330_Version3-noCH4-noPAR-QC.csv: '
            'critical(1), error(1), warning(3), ok(6)\nAutomated fix '
            'for CA-DBB_HH_201406010000_201808312330_Version3-noCH4-'
            'noPAR-QC.csv: critical(1), error(0), warning(0), ok(1)'),
        'msg_body': [
            ('Dear SiteTeamMember5,\n\nThank you for uploading data for '
                'CA-DBB on May 29, 2019 (see complete file list below).\n\n'
                'Format QA/QC assesses the compliance of your data submission'
                ' with AmeriFlux FP-In format. This step is critical to '
                'ensure that your data will be processed correctly. Details '
                'about the format requirements can be found at '
                f'{ui_url}half-hourly-hourly-data-upload-format/. Data '
                'that passes the Format QA/QC checks will be automatically '
                'queued for Data QA/QC, the next step in the AmeriFlux data '
                'processing pipeline.\n\nWe have processed your data through '
                'our Format QA/QC scheme. The results are listed below.\n\n\n'
                '*REVIEW REQUESTED*\nINSERT ISSUE-SPECIFIC TEXT. You can '
                f're-upload your data at {ui_url}'
                'data/upload-data/ and/or reply to this email to discuss with '
                'us. We fixed issues where possible as detailed below to '
                'attempt to prepare the file for Data QA/QC. Data QA/QC '
                'results will be sent in a separate email.\n\n\nThese issues '
                'were encountered in the following files CA-DBB_HH_2014060100'
                '00_201808312330_Version3-noCH4-noPAR-QC.csv. If left '
                'unaddressed in subsequent data submissions, an attempt to '
                'automatically fix these issues will be made:\n* Some '
                'filename components are not in the standard AmeriFlux '
                'format: unexpected optional parameter.\n* Error reading '
                'data from the file. .\n* Timestamp variables are not in '
                'standard AmeriFlux format. TIMESTAMP_START and '
                'TIMESTAMP_END should be in the first two columns.\n* '
                'Variable names , ï..TIMESTAMP_START, FC_PI_F, H_PI_F, '
                'LE_PI_F are not in the standard AmeriFlux format. They will '
                'not be included in the standard AmeriFlux data product. '
                'Re-upload data with corrected variable names if '
                'appropriate. Non-standard variables will be saved for a '
                'non-standard data product that will be available in future. '
                'Reply to this email to request that a variable be added to '
                'AmeriFlux FP Standard.\n\nPlease correct these issues in '
                'subsequent data submissions.\n\n\nView the status of your '
                f'uploaded files at {ui_url}qaqc-'
                'reports-data-team/. Links to view the Format QA/QC report '
                'for each file are at the end of this email.\n\nWe appreciate '
                'your help with standardizing the data submission format. We '
                'hope that fixing any identified issues will not take too '
                'much time from your work, but it is necessary to enable '
                'timely data processing. Please reply to this email with any '
                'questions. You can track communications on this Format '
                'QA/QC report at QAQC-9999 using your AmeriFlux account ID '
                'and password to login.\n\nSincerely,\nAMP Data Team\n\n'
                '*List of uploaded file(s) and corresponding Format QA/QC '
                'Report link:*\nCA-DBB_HH_201406010000_201808312330_Version3-'
                f'noCH4-noPAR-QC.csv: https://{link}/qaqc-report/'
                '?site_id=CA-DBB&report_id=36915')]
        },
    # FAIL Original, processID = 8052, ver = 0_4_5, updateID =
    'token6': {
        'code_version': '0.4.5',
        'jira_issue_info_parsed': [
            'US-Ton', '8052', 'None-None',
            'siteteammember6',
            (f'Format Results - {summary_text["action"]} | US-Ton data '
                'uploaded on Jan 19, 2017'),
            ('no gap filled, 2016 flux and met data from the tower below '
                'tree canopy, named as "understory", at Tonzi Ranch')],
        'description_txt': (
            'QAQC completed with the following results:\n'
            'Tonzi-understory-2016.dat: critical(3), error(2), '
            'warning(3), ok(10)'),
        'msg_body': [
            ('Dear SiteTeamMember6,\n\nThank you for uploading data for '
                'US-Ton on Jan 19, 2017 (see complete file list below).\n\n'
                'Format QA/QC assesses the compliance of your data submission '
                'with AmeriFlux FP-In format. This step is critical to '
                'ensure that your data will be processed correctly. Details '
                'about the format requirements can be found at '
                f'{ui_url}half-hourly-hourly-data-upload-format/. Data '
                'that passes the Format QA/QC checks will be automatically '
                'queued for Data QA/QC, the next step in the AmeriFlux data '
                'processing pipeline.\n\nWe have processed your data through '
                'our Format QA/QC scheme. The results are listed below.\n\n'
                '*ACTION REQUIRED*\nWe are unable to process your data '
                'further. Please address the following issues by '
                f're-uploading your data at {ui_url}'
                'data/upload-data/ and/or by replying to this email. See '
                f'{ui_url}half-hourly-hourly-data-'
                'upload-format/ for AmeriFlux FP-In format instructions.\n\n\n'
                'These potential issues were encountered in the following '
                'files Tonzi-understory-2016.dat:\n* Expected timestamp '
                'variables TIMESTAMP_START, TIMESTAMP_END are not present.\n* '
                'Unable to repair timestamps. AutoRepair FAILED.\n\n*REVIEW '
                'REQUESTED*\nWe found the following potential issues. Some '
                'issues may impact Data QA/QC results if left '
                'unaddressed.\n\n\nThese potential issues were encountered '
                'in the following files Tonzi-understory-2016.dat:\n* '
                'Variable names yr, day, endhour, endmin, DOY, FC_WPL_2D, '
                'fc_flag, WC_2D, CO2_LI7500, RHOC, CO2_var, CO2_skewness, '
                'CO2_kurtosis, RHOQ, q_var, q_skewness, q_kurtosis, Tsonic, '
                'Tsonic_var, Tsonic_skewness, Tsonic_kurtosis, '
                'Wind_Direction, Wind_Velocity, Friction_Velocity, stdw, '
                'wbar, w_var, w_kurtosis, u2D_var, v2D_var, Tair, '
                'absolute_humidity, Vapor_pressure_deficit, Relhumidity, '
                'Pressure, TSOIL2, TSOIL4, TSOIL8, TSOIL16, TSOIL32, '
                'soil_moisture_00cm, soil_moisture_20cm, soil_moisture_50cm, '
                'precipitation are not in the standard AmeriFlux format. '
                'They will not be included in the standard AmeriFlux data '
                'product. Re-upload data with corrected variable names if '
                'appropriate. Non-standard variables will be saved for a '
                'non-standard data product that will be available in future. '
                'Reply to this email to request that a variable be added to '
                'AmeriFlux FP Standard.\n* These variables are suspected to '
                'be gap-filled because they have no missing values: yr, day, '
                'endhour, endmin, DOY, fc_flag, precipitation. If these '
                'variables are gap-filled, please use the _F variable '
                'qualifier. While gap-filled versions of these variables '
                'are accepted, non-filled data must be submitted for primary '
                'flux variables (FC, FCH4, LE, H). Please also consider '
                'submitting non-filled data for all other variables.\n* These '
                'variables have no data for file\'s entire time period: RHOQ, '
                'w_kurtosis. Previously uploaded data with the same time '
                'period will be overwritten.\n* Some filename components are '
                'not in the standard AmeriFlux format: incorrect number of '
                'components (expect timestamp errors), extension is not '
                'csv.\n* Timestamp variables are not in standard AmeriFlux '
                'format. TIMESTAMP_START and TIMESTAMP_END should be in the '
                'first two columns.\n\nView the status of your uploaded '
                f'files at {ui_url}qaqc-reports-'
                'data-team/. Links to view the Format QA/QC report for each '
                'file are at the end of this email.\n\nWe appreciate your '
                'help with standardizing the data submission format. We hope '
                'that fixing any identified issues will not take too much '
                'time from your work, but it is necessary to enable timely '
                'data processing. Please reply to this email with any '
                'questions. You can track communications on this Format QA/QC '
                'report at QAQC-9999 using your AmeriFlux account ID and '
                'password to login.\n\nSincerely,\nAMP Data Team\n\n*List of '
                'uploaded file(s) and corresponding Format QA/QC Report '
                f'link:*\nTonzi-understory-2016.dat: https://{link}/'
                'qaqc-report/?site_id=US-Ton&report_id=8052')]
        }
}
