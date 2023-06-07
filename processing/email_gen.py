#!/usr/bin/env python

import argparse
import collections
import json
import os
import time

from configparser import ConfigParser
from datetime import datetime as dt
from http import HTTPStatus
from urllib.error import HTTPError
from urllib import request

from jira_interface import JIRAInterface
from jira_names import JIRANames
from logger import Logger
from messages import Messages
from status import StatusCode

__author__ = 'Norm Beekwilder, Danielle Christianson'
__email__ = 'norm.beekwilder@gmail.com, dschristianson@lbl.gov'

_log = Logger().getLogger(__name__)


class EmailGenError(Exception):
    pass


class EmailGen:
    def __init__(self):
        self.LineInfo = collections.namedtuple(
            'LineInfo', 'line action files order')
        self.reports = {}
        self.report_dict = {}
        self.message_detail_parts = []
        self.auto_repair_detail_parts = []
        self.upload_reports_ws, self.code_version, self.test_issue, \
            self.report_link_txt, self.jira_tester, \
            self.test_site, self.ui_prefix = self._init_from_cfg()
        self.action_opener = (
            '*ACTION REQUIRED*\nWe are unable to process your data further. '
            'Please address the following issues by re-uploading your data '
            f'at {self.ui_prefix}data/upload-data/ and/or by '
            'replying to this email. See '
            f'{self.ui_prefix}half-hourly-hourly-data-upload-format/ '
            'for AmeriFlux FP-In format instructions.\n')
        self.post_action_opener = (
            '\n*REVIEW REQUESTED*\nWe found the following potential issues. '
            'Some issues may impact Data QA/QC results if left unaddressed.\n')
        self.verification_opener = (
            '*REVIEW REQUESTED*\nINSERT ISSUE-SPECIFIC TEXT. You can re-'
            f'upload your data at {self.ui_prefix}data/upload-data/ '
            'and/or reply to this email to discuss with us. We fixed issues '
            'where possible as detailed below to attempt to prepare the file '
            'for Data QA/QC. Data QA/QC results will be sent in a separate '
            'email.\n')
        self.verification_closer = (
            '\nPlease correct these issues in subsequent data submissions.\n')
        self.all_passed_text = (
            '*ALL IS GOOD*\nYour uploaded data comply with AmeriFlux FP-In '
            'format. Awesome! Data QA/QC will be run next to determine if '
            'there are any issues with the data in the file(s). Data QA/QC '
            'results will be sent in a separate email.\n')
        self.ts_warning = (
            'Please note: In the future please include  TIMESTAMP_START and '
            'TIMESTAMP_END in your uploaded data. *The automated generation '
            'of these variables is only available temporarily*.')
        self.fixer_check_name = 'AutoRepair Fixes and/or Error Messages'
        self.fixer_archival_file_txt = (
            'file contains multiple files. Created new upload and retired')
        self.overall_status_txt = {'pass': 'PASS', 'warning': 'WARNING',
                                   'fail': 'FAIL'}
        self.overall_action_txt = {
            'ok_no_autocorrect': 'Ready for Data QA/QC.',
            'warn_no_autocorrect': 'Review Recommended.',
            'pass_autocorrect': 'Autocorrections made. Review Recommended.',
            'fail': 'Replacement file required.'}
        self.email_subject_action = {
            'action': 'ACTION REQUIRED',
            'review': 'Review recommended',
            'review_requested': 'Review requested',
            'good': 'All is good'
        }
        self.auto_email_opener = (
            'Files marked PASS or WARNING will be queued for Data QA/QC '
            'unless a replacement file is uploaded. Files marked FAIL require '
            'a replacement file. Review online report for details.\n\n'
            '*Format QA/QC results*\n'
            '-----------------------------------------------------------\n')
        self.auto_email_closer = (
            '-----------------------------------------------------------\n\n'
            'Format QA/QC assesses the compliance of '
            'your data submission with AmeriFlux FP-In format '
            f'({self.ui_prefix}half-hourly-hourly-data-upload-format/. If '
            f'needed, you can re-upload your data at {self.ui_prefix}'
            'data/upload-data/ and/or reply to this email to discuss '
            'with us.\n\nView the status of all your uploaded files at '
            f'{self.ui_prefix}qaqc-reports-data-team/.\n\nIf all '
            'files passed Format QA/QC (PASS or WARNING) and '
            'there are no pending issues for '
            'your site, Data QA/QC will be run. You can track communications '
            'on this Format QA/QC report at {key} using your AmeriFlux '
            'account ID and password to login.\n\nSincerely,\nAMP Data Team')
        self.unfixable = ('gap_fill_test', 'mand_nonfill',
                          'all_data_missing', 'data_missing')
        self.test_mode = False
        self.bad_status_value = 9999
        self.bad_status_txt = 'BAD_STATUS'

    def _init_from_cfg(self, cfg_filename='qaqc.cfg'):
        """
        Load info from configuration file qaqc.cfg
        :param cfg_filename: str, filename of config file
        :return: (assign class variables)
        """
        jira_tester = None
        test_site = None
        config = ConfigParser()
        cwd = os.getcwd()
        cfg_path = os.path.join(cwd, cfg_filename)
        if not os.path.exists(cfg_path):
            err_msg = 'Unable to find credentials configuration file.'
            raise Exception(err_msg)
        with open(cfg_path) as cfg:
            config.read_file(cfg)
            cfg_section = 'WEBSERVICES'
            if config.has_section(cfg_section):
                upload_reports_ws = config.get(
                    cfg_section, 'upload_reports')
            else:
                err_msg = ('Unable to find upload report webservice in '
                           'the configuration file.')
                raise Exception(err_msg)
            cfg_section = 'VERSION'
            if config.has_section(cfg_section):
                code_version = config.get(
                    cfg_section, 'code_version').replace('.', '_')
                test_issue = config.getboolean(
                    cfg_section, 'test')
            else:
                err_msg = ('Unable to find version and test status in '
                           'the configuration file.')
                raise Exception(err_msg)
            cfg_section = 'UI'
            if config.has_section(cfg_section):
                ui_prefix = config.get(cfg_section, 'ui_prefix')
            else:
                err_msg = 'Unable to find ui prefix in the config file.'
                raise Exception(err_msg)
            report_link_txt = ('{ui_prefix}qaqc-report/?site_id={s}'
                               '&report_id={p}')
            cfg_section = 'REPORT_EMAIL'
            if config.has_section(cfg_section):
                report_link_txt = config.get(
                    cfg_section, 'report_link')
            cfg_section = 'TEST_INFO'
            if config.has_section(cfg_section):
                jira_tester = config.get(cfg_section, 'tester_jira_user')
                test_site = config.get(cfg_section, 'test_site')

        return upload_reports_ws, code_version, test_issue, report_link_txt, \
            jira_tester, test_site, ui_prefix

    def is_upload_from_zip(self, upload_info):
        """
        Determine if uploaded files were from archival upload
        :param upload_info: dict, upload info
        :return: boolean, True = files from archival upload
        """
        return True if upload_info['zip_file'] is not None else False

    def is_file_archival(self, file_report):
        """
        Determine if the uploaded file is an archival file format
        :param file_report: dict, uploaded file report
        :return: boolean, True = archival file type
        """
        fixer_check = next(
            iter([c for c in file_report['qaqc_checks']['checks']
                  if c['check_name'] == self.fixer_check_name]), None)
        if fixer_check is None:
            return False
        elif (self.fixer_archival_file_txt in fixer_check['status_msg'].get(
                'WARNING', {}).get('status_body', [''])[0]):
            return True
        return False

    def all_upload_files_archival(self, upload_info):
        """
        Determine if all uploaded files are archival
        :param upload_info:
        :return: boolean, True = all uploaded files are archival
        """
        for file_report in self.get_file_reports(upload_info):
            if not self.is_file_archival(file_report):
                return False
        return True

    def add_original_file_process_id_to_report(self, upload_info):
        """
        Modify the file report to include the originally uploaded
        file's process_id
        :param upload_info: dict, upload_info
        """
        for process_id, file_report in upload_info['reports'].items():
            file_report['process_id'] = process_id

    def get_process_ids(self, upload_info):
        """
        Get process_ids from SORTED list of file reports. These are the keys
        of the reports in the upload_info['reports'] dict
        :param upload_info: dict, upload_info
        :return: sorted process ids
        """
        sorted_reports = self.get_file_reports(upload_info)
        return [file_report['process_id'] for file_report in sorted_reports]

    def get_file_reports(self, upload_info):
        """
        Get a SORTED list of uploaded file reports from the upload info dict.
        Sort by uploaded filename.
        :param upload_info: dict
        :return: list
        """
        return sorted(upload_info['reports'].values(),
                      key=lambda report: report['upload_file'])

    def has_autocorrect_file(self, file_report):
        """
        Determine if the uploaded file has an associated autocorrected file
        :param file_report: dict, upload file report
        :return: boolean, True = has autocorrected file
        """
        return True if file_report['autorepair_qaqc_checks'] is not None \
            else False

    def get_status_code(self, check_status):
        """
        Convert status txt to the status numerical code
        :param check_status: str
        :return: int
        """
        status_map = StatusCode().status_map
        for status_code, status_txt in status_map.items():
            if status_txt == check_status:
                return status_code
        return self.bad_status_value

    def get_worst_check_status(self, check_statuses):
        """
        Get the worst status from a list of statuses (str)
        :param check_statuses: list of statuses from a check
        :return: the worst status
        """
        worst_status = status_code = self.bad_status_value
        for check_status in check_statuses:
            status_code = self.get_status_code(check_status)
            if status_code < worst_status:
                worst_status = status_code
        if status_code > 0:
            return self.bad_status_txt
        return StatusCode().status_map[worst_status]

    def get_worst_file_status(self, file_statuses):
        """
        Get worst status for file_status dictionary
        :param file_statuses: dict, overall status and txt for each process_id
        :return: int, worst status code
        """
        worst_status = status_code = self.bad_status_value
        has_autocorrect_file_for_worst_status = False
        for process_id, file_status in file_statuses.items():
            status_code = file_status['overall_code']
            if status_code > worst_status:
                continue
            if status_code < worst_status:
                worst_status = status_code
            # if there are multiple files in the upload with 0 (OK) or
            # multiple files with -1 (WARNING), the worst overall status
            # is 0 or -1 as appropriate with has_autocorrect = True.
            # For -2 and -3, the worst case is has_autocorrect = False
            has_autocorrect_file = file_status['has_autocorrect']
            if (status_code > -2 and has_autocorrect_file and
                    not has_autocorrect_file_for_worst_status):
                has_autocorrect_file_for_worst_status = True
            elif (status_code < -1 and not has_autocorrect_file
                  and has_autocorrect_file_for_worst_status):
                has_autocorrect_file_for_worst_status = False
        if status_code > 0:
            return self.bad_status_txt, None
        return worst_status, has_autocorrect_file_for_worst_status

    def get_check_msg_body(self, check_result, skip=()):
        """
        Build msg body. Moved from emailingPI
        :param check_result: a list of statuses (str)
        :param skip: tuple or list of statuses to skip
        :return:
        """
        status_msgs = check_result['status_msg']
        try:
            msg_components = []
            for c in status_msgs:
                if c in skip:
                    continue
                msg_components.append(''.join(status_msgs[c]['status_body']))
            return ', '.join(msg_components)
        except KeyError:
            return ''

    def get_check_info(self, check_result):
        """
        Parse the check result and return info and formated text results
        Refactored from emailingPIs
        :param check_result: dict, check results
        :return action: str, type of action
        :return msg: str, body of message
        :return check name: str, check name for report (display text)
        :return check_status: str, worst status from the check
        """
        messages = Messages().get_msg_dict()
        check_name = check_result['check_name']
        check_status = self.get_worst_check_status(check_result['status_code'])
        msg_dict = [m for m in messages
                    if m['check_name'] == check_name and
                    m['status'] == check_status][0]
        msg_txt = msg_dict['message']
        action = msg_dict['type']
        if msg_dict['test_name'] != 'file_fixer':
            msg = msg_txt.format(var=self.get_check_msg_body(check_result))
        else:
            # If there is an error or critical issue in the fixer
            # don't report warnings for fixes.
            if check_status == 'WARNING':
                msg = msg_txt.format(var=self.get_check_msg_body(check_result))
            else:
                msg = msg_txt.format(
                    var=self.get_check_msg_body(check_result, ['WARNING']))
        return action, msg, msg_dict['test_name'], check_status

    def get_overall_file_status_code(self, file_report, has_autocorrect_file):
        """

        :param file_report: dict the individual file report
        :param has_autocorrect_file: boolean, True = has autocorrected file
        :return: int, overall status of the original and autocorrected file
        """
        check_file_type = 'qaqc_checks'  # original file
        if has_autocorrect_file:
            check_file_type = 'autorepair_qaqc_checks'  # autocorrected file
        return self.get_status_code(
            check_status=file_report[check_file_type]
                                    ['process_confirmation']['status_code'])

    # ToDo: build test
    def generate_description(self, upload_info, zip_upload):
        """
        Generate the description text for the JIRA issue
        :param upload_info: dictionary of upload info
        :param zip_upload: boolean, True = upload was from zip file
        :return: str, description text
        """
        description_list = []
        if zip_upload:
            zip_file = upload_info['zip_file']
            description_list.append(
                f'Files were uploaded from archival file: {zip_file}.\n')
        description_list.append('QAQC completed with the following results:')

        for file_report in self.get_file_reports(upload_info):
            upload_file = file_report['upload_file']
            qaqc_summary = file_report['qaqc_checks']['check_summary']
            description_list.append(f'{upload_file}: {qaqc_summary}')

            if self.has_autocorrect_file(file_report):
                autocorrect_summary = \
                    file_report['autorepair_qaqc_checks']['check_summary']
                description_list.append(
                    f'Automated fix for {upload_file}: {autocorrect_summary}')
        return '\n'.join(description_list)

    def get_file_statuses(self, upload_info):
        """
        Generate a dictionary with the overall status for each uploaded file
        :param upload_info: dict, upload_info
        :return: dict
        """
        file_statuses = {}
        for file_report in self.get_file_reports(upload_info):
            process_id = file_report['process_id']
            has_autocorrect_file = self.has_autocorrect_file(file_report)
            overall_status = self.get_overall_file_status_code(
                file_report, has_autocorrect_file)
            file_statuses.setdefault(process_id, {})['overall_code'] = \
                overall_status
            file_statuses[process_id]['has_autocorrect'] = has_autocorrect_file
            status_txt = action_txt = self.bad_status_txt
            b = '*'
            if overall_status > 0:
                continue
            elif overall_status < -1:
                status_txt = self.overall_status_txt['fail']
                action_txt = self.overall_action_txt['fail']
            else:
                status_txt = self.overall_status_txt['warning']
                if has_autocorrect_file:
                    action_txt = self.overall_action_txt['pass_autocorrect']
                elif overall_status < 0:
                    action_txt = self.overall_action_txt['warn_no_autocorrect']
                elif overall_status == 0:
                    status_txt = self.overall_status_txt['pass']
                    action_txt = self.overall_action_txt['ok_no_autocorrect']
            file_statuses[process_id]['overall_txt'] = (
                f'{b}{status_txt}{b} | {b}{action_txt}{b}')
        return file_statuses

    def get_overall_upload_status(self, file_statuses):
        """
        Get the overall status of the upload for the email subject
            (JIRA summary)
        :param file_statuses: dict, statuses of each file
        :return: str, text describing upload status
        """
        worst_status, has_autocorrect_file = \
            self.get_worst_file_status(file_statuses)
        if worst_status < -1:
            return self.email_subject_action['action']
        elif worst_status < 0:
            return self.email_subject_action['review']
        elif worst_status == 0:
            if has_autocorrect_file:
                return self.email_subject_action['review']
            return self.email_subject_action['good']
        else:
            return self.bad_status_txt

    def get_overall_upload_jira_state(self, file_statuses):
        """
        Get overall upload JIRA state
        :return: the JIRA transition or None
        """
        worst_status, _ = \
            self.get_worst_file_status(file_statuses)
        if worst_status in (-1, 0):
            return JIRANames.format_QAQC_ready_for_data
        return JIRANames.format_QAQC_sent_qaqc_review

    def generate_auto_email_components(self, upload_info, file_statuses):
        """
        Generate the text for the list of files, their statuses, report links
        :param upload_info: dict, the upload info
        :param file_statuses: dict, statuses for each process_id (file)
        :return: str, the msg component with the list of files
                      with their statuses and report links
        """
        msg_pieces = []
        site_id = upload_info['SITE_ID']
        for file_report in self.get_file_reports(upload_info):
            process_id = file_report['process_id']
            upload_file = file_report['upload_file']
            report_link = self.construct_report_link(site_id, process_id)
            file_status = file_statuses[process_id]['overall_txt']
            msg_pieces.append(
                f'{upload_file}:\n* {file_status}\n'
                f'* Read details in this report: {report_link}')
        return '\n\n'.join(msg_pieces)

    def generate_detailed_components(self, upload_info, file_statuses):
        """
        Generate detailed email message components
        This is legacy code for the detailed email, whose anticipated use is
            limited. Not refactoring.
        :param upload_info: dict, upload info
        :param file_statuses: dict, file statuses
        :return status_result_txt: str, overall upload status
        :return msg_body: str, guts of the detailed email
        :return fix_note: str, note for fixes
        """
        stat_codes = StatusCode()
        fix_text = ''
        archive_count = 0
        ts_warning_flag = False
        for file_report in self.get_file_reports(upload_info):
            r = file_report
            if self.is_file_archival(file_report=file_report):
                archive_count += 1
                continue
            has_autocorrect_file = self.has_autocorrect_file(file_report)
            if has_autocorrect_file:
                # Step thru each autocorrection check to:
                #    1) determine status via autorepair checks CHECK this out!
                #    2) get the messaging for each check
                #    3) Order the message checks
                for c in r['autorepair_qaqc_checks']['checks']:
                    action, line_text, test_name, stat = self.get_check_info(c)
                    # get the first tuple who's line value is line_text if no
                    # such tuple exists return None
                    li = next(iter([t for t in self.auto_repair_detail_parts
                                    if t.line == line_text and
                                    t.action == action]), None)
                    if li is None:
                        # figure out how we want to order check results
                        li = self.LineInfo(line=line_text, action=action,
                                           files=set([]), order=1)
                        self.auto_repair_detail_parts.append(li)
                    li.files.add(r['upload_file'])
            fixed = True
            # determine if autocorrection failed
            process_id = file_report['process_id']
            if file_statuses[process_id]['overall_code'] < stat_codes.WARNING:
                fixed = False
            # If all is good OR autorcorrection was successful
            if fixed:
                # If autocorrection was successful
                if has_autocorrect_file:
                    self.message_detail_parts.extend(
                        [a for a in self.auto_repair_detail_parts
                         if a not in self.message_detail_parts])
                    standard_mode = True
                    for c in r['qaqc_checks']['checks']:
                        action, line_text, test_name, stat = \
                            self.get_check_info(c)
                        if test_name == 'data_headers_NS' \
                                and not any(a['check_name'] ==
                                            ('Are Data Variable names'
                                            ' in correct format?')
                                            for a in
                                            r['autorepair_qaqc_checks']
                                             ['checks']):
                            standard_mode = False
                        if test_name == 'file_fixer':
                            if standard_mode:
                                msg_list = (c['status_msg']['WARNING']
                                             ['status_body'][:-1])
                            else:
                                msg_list = (
                                    [m for m in c['status_msg']['WARNING']
                                                 ['status_body'][:-1]
                                     if not(
                                        m.startswith(
                                            'Tried to fix invalid '
                                            'variable name') or
                                        m.startswith(
                                            'NOTE un-fixable variable '
                                            'names:'))])
                                if len(msg_list) == 0:
                                    break
                            fix_text = ('To proceed with Data QA/QC, we '
                                        'fixed issues if possible as '
                                        'detailed below. ')
                            for s in msg_list:
                                if s.startswith(
                                        'Generated timestamp variables '
                                        'TIMESTAMP_START and TIMESTAMP_END '
                                        'from'):
                                    ts_warning_flag = True
                                li = next(iter(
                                    [t for t in self.message_detail_parts
                                     if t.line == s and
                                     t.action == 'fixed']), None)
                                if li is None:
                                    li = self.LineInfo(
                                        line=s, action='fixed',
                                        files=set([]), order=1)
                                    self.message_detail_parts.append(li)
                                li.files.add(r['upload_file'])
                            break
                # If autocorrection was warning only OR all is good
                else:  # un-fixable or all good
                    for c in r['qaqc_checks']['checks']:
                        action, line_text, test_name, stat = \
                            self.get_check_info(c)
                        if test_name == 'file_fixer':
                            for s in c['status_code']:
                                if s == 'WARNING':
                                    continue
                                for b in c['status_msg'][s]['status_body']:
                                    li = next(iter(
                                        [t for t in self.message_detail_parts
                                         if t.line == b]), None)
                                    if li is None:
                                        li = self.LineInfo(
                                            line=b, action=action,
                                            files=set([]), order=1)
                                        self.message_detail_parts.append(li)
                                    li.files.add(r['upload_file'])
                            continue
                        li = next(
                            iter([t for t in self.message_detail_parts if
                                  t.line == line_text and t.action == action]),
                            None)
                        if li is None:
                            # figure out how we want to order check results
                            li = self.LineInfo(
                                line=line_text, action=action,
                                files=set([]), order=1)
                            self.message_detail_parts.append(li)
                        li.files.add(r['upload_file'])
            # if autorepair failed or not attempted.
            else:  # not fixed
                for c in r['qaqc_checks']['checks']:
                    action, line_text, test_name, stat = \
                        self.get_check_info(c)
                    if test_name not in self.unfixable:
                        if r['autorepair_qaqc_checks'] is not None \
                                and next(iter(
                                [t for t in self.auto_repair_detail_parts
                                 if t.line == line_text and
                                    t.action == action and
                                    r['upload_file'] in t.files]),
                                    None) is None:
                            if test_name == 'file_fixer':
                                continue
                            action = 'Auto_fix'
                    li = next(iter([t for t in self.message_detail_parts
                                    if t.line == line_text and
                                    t.action == action]), None)
                    if li is None:
                        # figure out how we want to order check results
                        li = self.LineInfo(line=line_text, action=action,
                                           files=set([]), order=1)
                        self.message_detail_parts.append(li)
                    li.files.add(r['upload_file'])
        # deal with files that are zips within zips.
        if archive_count == len(upload_info['reports']):
            # all files in upload were multi-file archives
            # don't generate an issue/email
            return None, None, None
        # here we start the messaging logic
        action_items = []
        verification_items = []
        passive_items = []
        for li in self.message_detail_parts:
            if li.line == '':
                continue
            if li.action == 'Active':
                action_items.append(li)
            elif li.action == 'Verification':
                verification_items.append(li)
            elif li.action == 'Passive' or li.action == 'Auto_fix' \
                    or li.action == 'fixed':
                passive_items.append(li)
        action_blocks = self.build_msg_section(action_items)
        verification_blocks = self.build_msg_section(
            verification_items, passive_items)
        # passive_blocks = self.build_msg_section(passive_items)
        fix_note = ('We hope that fixing any identified issues will not take '
                    'too much time from your work, but it is '
                    'necessary to enable timely data processing. ')
        # final email status determined from parsing the message types
        if len(action_blocks) > 0:
            status_result_txt = self.email_subject_action['action']
            action_blocks.insert(0, self.action_opener)
            if len(verification_blocks) > 0:  # or len(passive_blocks) > 0:
                action_blocks.append(self.post_action_opener)
                if len([li for li in passive_items
                        if li.action == 'Auto_fix'
                        or li.action == 'fixed']) > 0:
                    verification_blocks.append(self.verification_closer)
        elif len(verification_blocks) > 0:  # or len(passive_blocks) > 0:
            status_result_txt = self.email_subject_action['review_requested']
            verification_blocks.insert(
                0, self.verification_opener.format(fix=fix_text))
            if ts_warning_flag:
                verification_blocks.insert(1, self.ts_warning)
            if len([li for li in passive_items
                    if li.action == 'Auto_fix' or li.action == 'fixed']) > 0:
                verification_blocks.append(self.verification_closer)
        else:
            status_result_txt = self.email_subject_action['good']
            fix_note = ''
            verification_blocks.insert(0, self.all_passed_text)
        msg_body = '\n'.join(
            ['\n'.join(action_blocks),
             '\n'.join(verification_blocks)])  # , '\n'.join(passive_blocks)])
        return status_result_txt, msg_body, fix_note

    def create_issue_summary(self, status_result, site_id, upload_date):
        """
        Build JIRA issue summary which is included in the email subject
        :param status_result: str, the overall upload status
        :param site_id: str, the site id
        :param upload_date: str, human readable upload date
        :return: str, the issue summary
        """
        return (f'Format Results - {status_result} | {site_id} '
                f'data uploaded on {upload_date}')

    def get_jira_format_issue_inputs(self, upload_info, status_result,
                                     test_issue=False):
        """
        Get format issue inputs.
        :param upload_info: dict, upload information
        :param status_result: str, overall upload status
        :param test_issue: boolean, True = test issue
        :return site_id: str, XX-yyy
        :return process_id: str, the process ids for the upload
                                 separated by a return
        :return start_end_times: str, the start and end times for each file,
                                      separated by a return
        :return uploader: str, name of uploader
        :return summary: str, the JIRA issue summary (also the email subject)
        :return upload_comment: str, comment entered during upload
        """
        site_id = upload_info['SITE_ID']
        upload_reports = upload_info['reports']
        # not using sorted process ids to keep consistent with prior versions
        process_id = '\n'.join([v for v in upload_reports])
        start_end_times = '\n'.join(
            ["-".join([upload_reports[v]['start_time'],
                       upload_reports[v]['end_time']])
             for v in upload_reports])
        uploader = upload_info['uploader_id']
        upload_date = self.get_formated_upload_datetime(upload_info)
        summary = self.create_issue_summary(status_result, site_id,
                                            upload_date)
        upload_comment = upload_info['upload_comment']

        if test_issue:
            site_id = self.test_site
            uploader = self.jira_tester

        return site_id, process_id, start_end_times, uploader, \
            summary, upload_comment

    def create_jira_format_issue(self, jira, upload_info, status_result,
                                 upload_token, description_txt,
                                 detailed_email=False, test_issue=False):
        """
        Create the format issue in JIRA
        :param jira: JIRAInterface instance
        :param upload_info: dict of upload info
        :param status_result: str, the overall status text that is used in
                                   the JIRA issue summary
        :param upload_token: str, the upload token
        :param description_txt: str, the text for the description field
        :param detailed_email: boolean, if True do not set auto reminder
        :param test_issue: boolean, create a test issue in JIRA
        :return: str, the JIRA issue key
        """
        site_id, process_id, start_end_times, uploader, \
            summary, upload_comment = self.get_jira_format_issue_inputs(
                upload_info, status_result, test_issue)

        # if test_mode:
        #     return [site_id, process_id, start_end_times, upload_token,
        #             uploader, summary, description_txt, upload_comment]

        reminder_schedule_id = None
        if not detailed_email:
            reminder_schedule_id = JIRANames.ReminderOptions.auto

        return jira.create_format_issue(
            site_id, process_id, start_end_times, upload_token,
            uploader, summary, description_txt, upload_comment,
            reminder_schedule_id)

    def get_sorted_uploaded_files(self, upload_info):
        """
        Get a list of the uploaded files sorted
        :param upload_info: dict, upload info
        :return: list of uploaded files
        """
        uploaded_files = []
        file_reports = self.get_file_reports(upload_info)
        for file_report in file_reports:
            uploaded_files.append(file_report['upload_file'])
        return uploaded_files

    def get_report_links(self, upload_info):
        """
        Build list of report links
        :param upload_info: dict, upload_info
        :return: list of uploaded files and their links
        """
        site_id = upload_info['SITE_ID']
        process_ids = self.get_process_ids(upload_info)
        uploaded_files = self.get_sorted_uploaded_files(upload_info)
        report_links = [
            f'{upload_file}: {self.construct_report_link(site_id, process_id)}'
            for upload_file, process_id in zip(uploaded_files, process_ids)]
        return report_links

    def construct_report_link(self, site_id, process_id):
        """
        Construct the report link
        :param site_id: str, site id
        :param process_id: str, process id
        :return: str, report link
        """
        return self.report_link_txt.format(s=site_id, p=process_id)

    # ToDo: write test
    def get_formated_upload_datetime(self, upload_info):
        """
        Format the upload date time for the email
        :param upload_info: dictionary of uploaded info
        :return: str, formatted upload date
        """
        # The datetime is in format YYYY-MM-DDTHH:MM:SS.FFFFFFF. By selecting
        #   only the first 19 characters (not the fractions of seconds),
        #   we can more easily reformat
        return dt.strftime(dt.strptime(upload_info['datetime'][:19],
                           "%Y-%m-%dT%H:%M:%S"), "%b %d, %Y")

    def craft_email(self, upload_info, msg_body, key, fix_note, zip_upload,
                    detailed_email=True):
        """
        Craft the email text (what goes into the JIRA comment)
        :param upload_info: dict, upload info
        :param msg_body: str, the check details
        :param key: str, the JIRA issue key
        :param fix_note: str, any text for fixes
        :param zip_upload: boolean, True = uploaded files were from
                                           originally uploaded archival file
        :param detailed_email: boolean, True = create the detailed email txt
                                        False = create the short auto email
        :return: str, the entire email text
        """
        site_id = upload_info['SITE_ID']
        contact = upload_info['uploader']
        upload_date = self.get_formated_upload_datetime(upload_info)
        zip_msg = ''
        if detailed_email:
            report_links = self.get_report_links(upload_info)
            if zip_upload:
                zip_msg = (
                    'The data in these Format QA/QC results were uploaded '
                    'in an archival file format (e.g., zip, 7z). '
                    'See file list at end of email to verify that all '
                    'expected files are included.\n\n')
                zip_file = upload_info['zip_file']
                report_link_zip_msg = ('These files were extracted from the '
                                       f'uploaded archival file: {zip_file}.')
                report_links.insert(0, report_link_zip_msg)
            report_links = '\n'.join(report_links)
            return (
                f'Dear {contact},\n\nThank you for uploading data for '
                f'{site_id} on {upload_date} (see complete file list below).'
                '\n\nFormat QA/QC assesses the compliance of your data '
                'submission with AmeriFlux FP-In format. This step is '
                'critical to ensure that your data will be '
                'processed correctly. Details about the format requirements '
                f'can be found at {self.ui_prefix}'
                'half-hourly-hourly-data-upload-format/. Data that passes '
                'the Format QA/QC checks will be automatically queued for '
                'Data QA/QC, the next step in the AmeriFlux data processing '
                'pipeline.\n\nWe have processed your data through our '
                'Format QA/QC scheme. The results are listed below.\n\n'
                f'{zip_msg}{msg_body}\n\nView the status of your uploaded '
                f'files at {self.ui_prefix}qaqc-reports-data-team/. '
                'Links to view the Format QA/QC report for each file are at '
                'the end of this email.\n\nWe appreciate your help with '
                f'standardizing the data submission format. {fix_note}Please '
                'reply to this email with any questions. You can track '
                f'communications on this Format QA/QC report at {key} using '
                'your AmeriFlux account ID and password to login.\n\n'
                'Sincerely,\nAMP Data Team\n\n*List of uploaded file(s) and '
                f'corresponding Format QA/QC Report link:*\n{report_links}')
        if zip_upload:
            zip_file = upload_info['zip_file']
            zip_msg = ('The files above were extracted from the '
                       f'uploaded archival file: {zip_file}.\n\n')
        return (
            f'Dear {contact},\n\nThank you for uploading data for '
            f'{site_id} on {upload_date}.\n\n'
            f'{self.auto_email_opener}{msg_body}\n\n{zip_msg}'
            f'{self.auto_email_closer.format(key=key)}')

    def build_msg_section(self, items, more_items=None):
        done = []
        blocks = []
        for li in items:
            self.process_item(blocks, done, items, li, more_items)
        if more_items is not None:
            for li in more_items:
                self.process_item(blocks, done, items, li, more_items)
        return blocks

    def process_item(self, blocks, done, items, li, more_items):
        # shared contains li and any other lines that affect the same set of
        # files if shared is empty li has already
        # been processed. final clause of list comprehension is a
        # triadic expression.
        shared = [t for t in items if t.files == li.files
                  and t not in done and
                  (t.action == 'Auto_fix' if li.action == 'Auto_fix'
                   else t.action != 'Auto_fix') and
                  (t.action == 'fixed' if li.action == 'fixed'
                   else t.action != 'fixed')]
        if more_items is not None:
            shared.extend([t for t in more_items if t.files == li.files
                           and t not in done and
                           (t.action == 'Auto_fix' if li.action == 'Auto_fix'
                            else t.action != 'Auto_fix') and
                           (t.action == 'fixed' if li.action == 'fixed'
                            else t.action != 'fixed')])
        if len(shared) == 0:
            return
        # make a txt str for the files that are sharing the same results. Get
        #     the file names from the first element of shared.
        msg_details = ', '.join(shared[0].files)
        if li.action == 'Auto_fix':
            msg = ('\nThese issues were encountered in the following '
                   f'files {msg_details}. If left unaddressed in subsequent '
                   'data submissions, an attempt to automatically '
                   'fix these issues will be made:\n* ')
        elif li.action == 'fixed':
            msg = ('\nThese automatic fixes were attempted to address issues'
                   f' encountered in the following files {msg_details}:\n* ')
        else:
            msg = ('\nThese potential issues were encountered in the '
                   f'following files {msg_details}:\n* ')
        # join the shared check messages (line)
        msg += '\n* '.join([s.line for s in shared])
        blocks.append(msg)
        done.extend(shared)

    def get_upload_data(self, upload_token, code_version=None):
        """
        Get the upload information from the database
        :param upload_token: str, upload token
        :param code_version: str, code version number
        :return: dict, upload info
        """
        if code_version is None:
            code_version = self.code_version
        url = self.upload_reports_ws.format(t=upload_token,
                                            v=code_version.replace('.', '_'))
        try:
            resp = request.urlopen(url)
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            error_details = e.read().decode('utf-8')
            raise Exception(f'{url} returned status code {e.code}\n'
                            f'{error_details}')

    def driver(self, upload_token, detailed_email=False):
        """
        Processes the upload: auto (detailed) :
        1) Get the upload info
        2) Determine if from zip
        3) Generate description msg
        4) Determine status of each file
        5) Determine overall status
        6) Generate email message
        7) Create JIRA issue
        8) Create JIRA comment

        :param upload_token: str, the upload token
        :param detailed_email: boolean, True = create detailed email
                                        False = create auto email
        :return: jira issue key
        """
        if not upload_token:
            return 'No upload_token provided.'
        upload_info = self.get_upload_data(upload_token)

        if self.all_upload_files_archival(upload_info):
            return 'All uploaded files are archival.'

        self.add_original_file_process_id_to_report(upload_info)
        zip_upload = self.is_upload_from_zip(upload_info=upload_info)
        description_txt = self.generate_description(upload_info, zip_upload)
        file_statuses = self.get_file_statuses(upload_info)
        fix_note = ''
        public_comment = False

        if detailed_email:
            status_result_txt, msg_body, fix_note = \
                self.generate_detailed_components(upload_info, file_statuses)
        else:
            # default case -- automated email
            msg_body = self.generate_auto_email_components(upload_info,
                                                           file_statuses)
            status_result_txt = self.get_overall_upload_status(file_statuses)
            if not self.test_issue:
                public_comment = True

        if status_result_txt == self.bad_status_txt:
            return 'Email not generated'

        jira = JIRAInterface()
        if self.test_mode:
            return self.craft_email(
                upload_info, msg_body, 'QAQC-9999', fix_note, zip_upload,
                detailed_email)
        try:
            key, jira_result = self.create_jira_format_issue(
                jira, upload_info, status_result_txt, upload_token,
                description_txt, detailed_email, test_issue=self.test_issue)
        except EmailGenError as email_gen_error:
            err_msg = f'Problem creating JIRA issue: {email_gen_error}'
            _log.warning(err_msg)
            return err_msg
        if jira_result != HTTPStatus.CREATED:
            # ToDo: figure out how to send email alert
            _log.warning(key)
            return key
        # there appears to be race conditions in jira between when jira
        # core creates the issue and when service desk knows about the
        # issue. Also between adding the comment and then label and new state.
        # Sleep briefly to let service desk get caught up.
        time.sleep(2)
        # create add the comment
        email_msg = self.craft_email(upload_info, msg_body, key,
                                     fix_note, zip_upload, detailed_email)
        comment_added = jira.add_comment(issue_key=key, message=email_msg,
                                         public=public_comment)
        if comment_added != HTTPStatus.CREATED:
            # ToDo: figure out email alert
            err_msg = (f'Comment not added to issue {key} for upload '
                       f'{upload_token}')
            return err_msg
        # Sleep briefly to let service desk get caught up.
        time.sleep(2)
        jira.add_label(issue_key=key, labels=[JIRANames.label_results_sent])
        jira_state = self.get_overall_upload_jira_state(file_statuses)
        # Sleep briefly to let service desk get caught up.
        time.sleep(2)
        jira.set_issue_state(issue_key=key, transition=jira_state)
        # Sleep briefly to let service desk get caught up.
        time.sleep(2)

        return key

    def main(self):
        parser = argparse.ArgumentParser(description='Email generation')
        parser.add_argument('upload_token', type=str,
                            help='GUID generated for upload from '
                                 '[FluxdataInterfaceData].[dbo].'
                                 '[FluxDataUploadLog]')
        parser.add_argument('-d', '--detailed_email', action='store_true',
                            help='Generate detailed email')
        parser.add_argument('-t', '--test_mode', action='store_true',
                            help='Intermediary test mode, to be replace '
                                 'by mocking in future')
        args = parser.parse_args()
        upload_token = args.upload_token
        detailed_email = args.detailed_email
        if args.test_mode:
            self.test_mode = args.test_mode
        return self.driver(upload_token, detailed_email)


if __name__ == "__main__":
    # _log = Logger(True).getLogger(__name__)
    print(EmailGen().main())
