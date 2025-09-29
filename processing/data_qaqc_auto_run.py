import ast
import os
import time

from datetime import datetime, timedelta
from mail_handler import Mailer
from typing import Dict, Optional, Union

from configparser import ConfigParser
from db_handler import DBConfig, NewDBHandler
from jira_interface import JIRAInterface
from jira_names import JIRANames
from link_replaced_issues import LinkIssues
from logger import Logger
from main import process_data_qaqc
from process_states import ProcessStates, ProcessStateHandler
from report_status import ReportStatus
from utils import TimestampUtil


_log_name_prefix = 'DataQAQCAuto'
_log = Logger(True, None, None,
              _log_name_prefix).getLogger(_log_name_prefix)


class DataQAQCAutoRunHandler:
    def __init__(self, cfg_file='qaqc.cfg'):
        self.db_conn_pool = {}
        self.jira_reporter, self.jira_host, self.jira_issue_path, \
            self.jira_project, self.jira_pause_seconds, \
            self.data_auto_lookback_time, self.qaqc_processor_email, \
            self.amp_team_email, self.is_test, \
            self.test_jira_project = self._read_cfg(cfg_file)
        self.db_handler = NewDBHandler()
        self.jira_interface = JIRAInterface(configure_organizations=False)
        self.link_issues = LinkIssues(jira=self.jira_interface)
        self.ts_util = TimestampUtil()

    def __del__(self):
        for conn in self.db_conn_pool.values():
            conn.close()

    def configure_connections(self, cfg_file: str = 'qaqc.cfg',
                              use_flux_conn: bool = False):
        """
        Configure connections to the databases.
        Set use_flux_conn to override the connection to the
            dev database in test mode.
        """
        hostname, user, auth, qaqc_db_name, jira_db_name, test_hostname = \
            self._read_db_cfg_section(cfg_file)

        # set up qaqc conn
        db_config = DBConfig(hostname, user, auth, qaqc_db_name)
        if self.is_test and not use_flux_conn:
            db_config = DBConfig(test_hostname, user, auth, qaqc_db_name)
        self.db_conn_pool['qaqc'] = self.db_handler.init_db_conn(db_config)

        # set up jira conn
        db_config = DBConfig(hostname, user, auth, jira_db_name)
        self.db_conn_pool['jira'] = self.db_handler.init_db_conn(db_config)

    @staticmethod
    def _read_db_cfg_section(cfg_file):
        with open(os.path.join(os.getcwd(), cfg_file)) as cfg:
            hostname = user = auth = None
            qaqc_db_name = jira_db_name = None
            test_hostname = None

            config = ConfigParser()
            config.read_file(cfg)
            cfg_section = 'DB'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'flux_hostname'):
                    hostname = config.get(cfg_section, 'flux_hostname')
                if config.has_option(cfg_section, 'flux_user'):
                    user = config.get(cfg_section, 'flux_user')
                if config.has_option(cfg_section, 'flux_auth'):
                    auth = config.get(cfg_section, 'flux_auth')
                if config.has_option(cfg_section, 'jira_db_name'):
                    jira_db_name = config.get(cfg_section, 'jira_db_name')
                if config.has_option(cfg_section, 'flux_db_name'):
                    qaqc_db_name = config.get(cfg_section, 'flux_db_name')

            cfg_section = 'TEST_INFO'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'test_flux_hostname'):
                    test_hostname = config.get(cfg_section,
                                               'test_flux_hostname')

            return (hostname, user, auth, qaqc_db_name,
                    jira_db_name, test_hostname)

    @staticmethod
    def _read_cfg(cfg_file):
        with (open(os.path.join(os.getcwd(), cfg_file)) as cfg):
            reporter = jira_host = jira_issue_path = jira_project = None
            jira_pause_seconds = 2  # seconds
            is_test = test_jira_project = None
            config = ConfigParser()
            config.read_file(cfg)

            cfg_section = 'JIRA'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'amf_data_team_reporter'):
                    reporter = config.get(cfg_section,
                                          'amf_data_team_reporter')
                if config.has_option(cfg_section, 'jira_host'):
                    jira_host = config.get(cfg_section, 'jira_host')
                if config.has_option(cfg_section, 'jira_issue_path'):
                    jira_issue_path = config.get(cfg_section,
                                                 'jira_issue_path')
                if config.has_option(cfg_section, 'project'):
                    jira_project = config.get(cfg_section, 'project')
                if config.has_option(cfg_section, 'jira_pause_seconds'):
                    jira_pause_seconds = config.getint(
                        cfg_section, 'jira_pause_seconds')

            cfg_section = 'DATA_AUTO'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'data_auto_lookback_time_h'):
                    data_auto_lookback_time = config.getint(
                        cfg_section, 'data_auto_lookback_time_h')

            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                qaqc_processor_email = config.get(
                    cfg_section, 'qaqc_processor_email')
                try:
                    amp_team_email = ast.literal_eval(
                        config.get(cfg_section, 'amp_team_email'))
                except Exception:
                    amp_team_email = []

            cfg_section = 'VERSION'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'test'):
                    is_test = config.getboolean(cfg_section, 'test')

            if is_test:
                cfg_section = 'TEST_INFO'
                if config.has_section(cfg_section):
                    if config.has_option(cfg_section, 'test_jira_project'):
                        test_jira_project = config.get(cfg_section,
                                                       'test_jira_project')

            return reporter, jira_host, jira_issue_path, \
                jira_project, jira_pause_seconds, \
                data_auto_lookback_time, \
                qaqc_processor_email, amp_team_email, \
                is_test, test_jira_project

    @staticmethod
    def check_all_jira_statuses_resolved(
            site_issues: list,
            resolved_format_states: list,
            resolved_data_states: list) -> bool:
        """
        Check of all the statuses are resolved
        """
        return all([issue.get('issue_status')
                    in resolved_format_states + resolved_data_states
                   for issue in site_issues])

    def assess_qaqc_jira_issues(
            self, site_issues,
            resolved_format_states, resolved_data_states):
        """
        Look for prior data qaqc run,
            determine if it is the only unresolved issue
        """
        recent_format_issues = []

        for idx, issue in enumerate(site_issues):
            issue_type = issue.get('issue_type')
            if issue_type == JIRANames.format_qaqc_issue_id:
                if issue.get('issue_status') in resolved_format_states:
                    recent_format_issues.append(issue.get('issue_key'))
                    continue
                return False, 'Unresolved Format QAQC', None, None

            if issue_type != JIRANames.data_qaqc_issue_id:
                return False, 'Problem getting issue type', None, None

            issue_key = issue.get('issue_key')

            if self.check_all_jira_statuses_resolved(
                    site_issues[idx + 1:], resolved_format_states,
                    resolved_data_states):
                return True, issue_key, issue, recent_format_issues

            return False, 'Older Format issues unresolved', None, None

        return False, 'Error in assessing QAQC issues', None, None

    @staticmethod
    def adjust_date_year(
            start_date: datetime,
            end_date: datetime) -> (int, int):

        # check start date for DDMM 1231
        start_year = start_date.year
        if start_date.month == 12 and start_date.day > 29:
            start_year += 1

        # Check end date for DDMM 0101
        end_year = end_date.year
        if end_date.month == 1 and end_date.day < 2:
            end_year -= 1

        return start_year, end_year

    def get_years_from_sub_issues(
            self, sub_issues_keys, full_record_dates) -> list:
        """

        """
        def _list_all_years_in_upload():
            issue_start_date, issue_end_date = full_record_dates
            start_year, end_year = self.adjust_date_year(
                issue_start_date, issue_end_date)
            return list(range(start_year, end_year + 1))

        sub_issues = self.get_jira_issues(sub_issues_keys)

        issue_years = []

        for issue_key, issue_fields in sub_issues.items():
            # ignore data issues with these statuses.
            # Note the other statuses to ignore get filtered out
            #     in the jira query b/c we are reusing the query
            #     that links sub issues to data issues.
            issue_status = issue_fields.get('status').get('name')
            if issue_status in (JIRANames.data_sub_issue_not_issue,
                                JIRANames.data_sub_issue_known_issue):
                continue

            years_strs = issue_fields.get(JIRANames.data_sub_issue_years)

            # if not date specified, assume all years
            if not years_strs or years_strs == 'All':
                # if full range, never have to take min / max,
                #    just overwrite
                return _list_all_years_in_upload()

            years = [year_str.strip() for year_str
                     in years_strs.split(',')]

            for yr in years:
                if '-' in yr:
                    try:
                        years_range = yr.split('-')
                        issue_years.extend(
                            list(range(int(years_range[0]),
                                       int(years_range[1]) + 1)))
                        continue
                    except Exception as e:
                        _log.warning('Unable to parse issue '
                                     f'years for {issue_key}: {e}')
                        # if can't assess dates, assume all
                        return _list_all_years_in_upload()
                try:
                    issue_years.append(int(yr))
                except ValueError as e:
                    _log.warning('Unable to parse issue '
                                 f'years for {issue_key}: {e}')
                    # if can't assess dates, assume all
                    return _list_all_years_in_upload()

        return issue_years

    def get_jira_issues(self, issues_keys):
        """"""
        jira_issues: Dict[str: Union[str, dict]] = {}
        for issue_key in issues_keys:
            try:
                issue_info = self.jira_interface.get_jira_issue(issue_key)
                issue_fields = issue_info.get('fields')
                jira_issues[issue_key] = issue_fields
            except Exception as e:
                error_msg = f'Error getting jira issue {issue_key}: {e}'
                jira_issues[issue_key] = error_msg

        return jira_issues

    def get_years_from_uploads(self, recent_format_issues_keys) -> list:
        """

        """
        upload_years = []

        # get the format issues
        format_issues = self.get_jira_issues(recent_format_issues_keys)

        format_file_dates = []
        for issue_key, issue_fields in format_issues.items():
            # if any issue did not return properly,
            #     exit to trigger needs review.
            if not isinstance(issue_fields, dict):
                return upload_years

            # Look through both
            #    Replaced with Upload and Attempt Data QAQC statuses
            # Replaced with Upload jira tickets can have valid files;
            #    the files that need replacing should be in a
            #    subsequent ticket that is Attempt Data QAQC
            # Ignore any canceled tickets
            issue_status = issue_fields.get('status').get('name')
            if issue_status == JIRANames.format_status_canceled:
                continue

            start_end_dates_str = issue_fields.get(
                JIRANames.start_end_dates)

            # If the start end dates were not successfully parsed,
            #    then the file did not pass format qaqc, ignore.
            if not start_end_dates_str:
                continue

            try:
                file_date_list = start_end_dates_str.split('\n')
                for file_date_str in file_date_list:
                    # If the start end dates were not successfully parsed,
                    #    then the file did not pass format qaqc, ignore.
                    if file_date_str.startswith('None'):
                        continue
                    start_end_dates = [
                        datetime.strptime(
                            d, self.ts_util.DATE_ONLY_TS_FORMAT)
                        for d in file_date_str.split('-')]
                    format_file_dates.append(start_end_dates)
            except Exception as e:
                _log.warning('Error extracting fire range dates '
                             f'from {issue_key}: {e}')
                return upload_years

        if not format_file_dates:
            _log.warning(f'No upload file range dates for {issue_key}')
            return upload_years

        for file_date in format_file_dates:
            start_year, end_year = self.adjust_date_year(
                file_date[0], file_date[1])
            upload_years.extend(list(range(start_year, end_year + 1)))

        upload_years = list(set(upload_years))

        return upload_years

    def assess_sub_issues(self, sub_issues, recent_format_issues_keys,
                          full_record_dates):
        """
        Assess the sub issues to see if the recently uploaded format issues
            cover the time range required for corrections.
        """
        # get date range of sub-issues
        issue_years = self.get_years_from_sub_issues(
            sub_issues, full_record_dates)
        if not issue_years:
            return False

        # Add the jira project name to the key number
        recent_format_issues_full_keys = [
            f'{self.jira_project}-{issue_key}'
            for issue_key in recent_format_issues_keys]

        # get date range of uploads
        upload_years = self.get_years_from_uploads(
            recent_format_issues_full_keys)
        if not upload_years:
            return False

        # check if date range of uploads covers range of sub-issues
        if all([yr in upload_years for yr in issue_years]):
            return True

        return False

    def resolve_data_issue(self, issue_key, process_id) -> \
            (bool, Optional[str]):
        """
        Update the jira issue ticket status and process state for the
        prior data QAQC run that is resolved by the new uploads.
        """
        # change the status of the prior data run jira issue
        _log.info(f'Attempting to update the jira status for {issue_key}')
        self.jira_interface.set_issue_state(
            issue_key=issue_key,
            transition=JIRANames.data_QAQC_replace_with_upload)

        time.sleep(self.jira_pause_seconds)  # give jira a pause
        data_issue = self.jira_interface.get_jira_issue(issue_key)
        if data_issue and data_issue.get('fields'):
            issue_fields = data_issue.get('fields')
            issue_status = issue_fields.get('status').get('name')
            if issue_status == JIRANames.data_QAQC_replace_with_upload:
                _log.info(f'Jira status updated for {issue_key}')
            else:
                warn_msg = f'Jira status DID NOT update for {issue_key}'
                _log.warning(warn_msg)
                return False, warn_msg

        # Fail the prior data qaqc run
        data_fail_state = ProcessStateHandler().get_process_state(
            ProcessStates.FailedCurator)
        rs = ReportStatus()
        is_update_success = rs.report_status(process_id=process_id,
                                             state_id=data_fail_state)
        if is_update_success:
            _log.info('Successfully updated the process state for '
                      f'{process_id}')
            return True, None

        warn_msg = f'Failed to update the process state for {process_id}'
        _log.warning(warn_msg)
        return False, warn_msg

    def extract_full_record_dates(self, issue_info):
        """
        extract the full record dates from the prior data qaqc run summary
        e.g., "Data Results | US-XYZ HH 20080101 - 20230430 |
               Using uploads through Sep 14, 2025"
        """
        summary = issue_info.get('summary')
        summary_pieces = summary.split(' ')
        start_date = datetime.strptime(summary_pieces[5],
                                       self.ts_util.DATE_ONLY_TS_FORMAT)
        end_date = datetime.strptime(summary_pieces[7],
                                     self.ts_util.DATE_ONLY_TS_FORMAT)
        return start_date, end_date

    def get_data_qaqc_candidates(self, review_sites_issues):
        # 'Attempt Data QAQC', 'Canceled', 'Replaced with Upload'
        resolved_format_states = [
            JIRANames.format_status_attempt_data_qaqc,
            JIRANames.format_status_canceled,
            JIRANames.format_status_replace_upload]
        # 'Publishable', 'Canceled', 'Replace with Upload'
        resolved_data_states = [
            JIRANames.data_status_publishable,
            JIRANames.data_status_canceled,
            JIRANames.data_status_replace_upload]
        auto_run_candidates = []
        need_review_candidates = {}

        for site_id, site_issues in review_sites_issues.items():

            # check if all issues have status resolved:
            if self.check_all_jira_statuses_resolved(
                    site_issues, resolved_format_states, resolved_data_states):
                # run the site
                auto_run_candidates.append(site_id)
                continue

            is_prior_data_only_unresolved, msg, issue_info, \
                recent_format_issues_keys = self.assess_qaqc_jira_issues(
                    site_issues, resolved_format_states, resolved_data_states)

            if not is_prior_data_only_unresolved:
                need_review_candidates[site_id] = msg
                continue

            data_issue_labels = issue_info.get('labels')
            if data_issue_labels is None:
                data_issue_labels = []
            if isinstance(data_issue_labels, str):
                data_issue_labels = data_issue_labels.split(',')

            # check if prior data issue is special case - AMP generated
            special_case = False
            for special_label in ('BASE', 'FLX-CA'):
                if special_label in data_issue_labels:
                    need_review_candidates[site_id] = (
                        f'Prior data issue has label {special_label}')
                    special_case = True
                    break

            if special_case:
                continue

            # Add jira project name to issue key number returned as
            #    msg in the assess_qaqc_jira_issues method above
            issue_key = f'{self.jira_project}-{msg}'
            process_id = issue_info.get('process_id')

            is_self_review = \
                JIRANames.label_self_review in data_issue_labels

            if is_self_review:
                # if not test resolve the data issue
                if (not self.is_test or
                        (self.jira_project == self.test_jira_project)):
                    is_resolve_successful, resolve_msg = \
                        self.resolve_data_issue(
                            issue_key, process_id)
                    # jira can get overloaded so wait
                    time.sleep(self.jira_pause_seconds)
                    if is_resolve_successful:
                        auto_run_candidates.append(site_id)
                        continue
                    need_review_candidates[site_id] = resolve_msg
                    continue

            # get_sub_issues_to_link -- note this method is the same as
            #    used in data qaqc report generation and
            #    filters out Fixed and Canceled sub-issues
            # Thus, the status "Known Issues" and "Not an issue"
            #    still need to be filtered
            #    (in the assess_sub_issues method).
            try:
                sub_issues_keys = self.jira_interface.get_sub_issues_to_link(
                    issue_key)
            except Exception as e:
                error_msg = f'Error getting sub issues to link for {msg}: {e}'
                need_review_candidates[site_id] = error_msg
                continue

            # It there are no sub-issues, assume ready
            is_ready_for_data_qaqc = True

            if sub_issues_keys:
                data_full_record = self.extract_full_record_dates(issue_info)
                is_ready_for_data_qaqc = self.assess_sub_issues(
                    sub_issues_keys, recent_format_issues_keys,
                    data_full_record)

            if is_ready_for_data_qaqc:
                if not self.is_test or (
                        (self.jira_project == self.test_jira_project)):
                    is_resolve_successful, resolve_msg = \
                        self.resolve_data_issue(
                            issue_key, process_id)
                    time.sleep(self.jira_pause_seconds)  # pause for jira
                    if is_resolve_successful:
                        auto_run_candidates.append(site_id)
                        continue

            need_review_candidates[site_id] = msg
            continue

        return auto_run_candidates, need_review_candidates

    @staticmethod
    def initiate_data_qaqc(site_id, potential_res_lookup):
        """
        screen_name = 'data_qaqc_run_' + str(site_id)
        exe = sys.executable
        exe_cmd = f'{exe} main.py {site_id} {res}\n'
        Popen(['screen', '-dmS', screen_name])
        Popen(['screen', '-rS', screen_name, f'{self.venv_path}/bin/activate'])
        Popen(['screen', '-rS', screen_name, '-X', 'stuff', exe_cmd])
        """
        res = potential_res_lookup.get(site_id)

        if res is None:
            return 'Invalid resolution from last upload'

        try:
            issue_key = process_data_qaqc(site_id, res)
            return issue_key
        except Exception as e:
            return e

    @staticmethod
    def get_potential_res(last_upload_lookup):

        last_upload_res_lookup = {}

        for site_id, filenames in last_upload_lookup.items():
            res = None
            for filename in filenames:
                if '_HH_' in filename:
                    res = 'HH'
                elif '_HR_' in filename:
                    res = 'HR'
                else:
                    continue
            last_upload_res_lookup[site_id] = res

        return last_upload_res_lookup

    def build_email_content(
            self, run_outcome, needs_review, notice_msg=None):
        """

        """
        if notice_msg:
            subject_notice = ' -- No sites to assess'
            body_msg = notice_msg
            return subject_notice, body_msg

        # split run_outcomes into success and fails
        successful_runs = []
        failed_runs = []
        for site_id, msg in run_outcome.items():
            full_msg = f'{site_id}: {msg}'
            if msg.startswith(self.jira_project):
                successful_runs.append(full_msg)
            else:
                failed_runs.append(full_msg)

        subject_notice = ''
        needs_review_list = []
        if needs_review:
            needs_review_list = [f'{site_id}: {msg}'
                                 for site_id, msg
                                 in needs_review.items()]
            subject_notice = ' -- REVIEW NEEDED'

        if len(failed_runs) > 0:
            subject_notice = ' -- REVIEW NEEDED'

        successful_runs_str = '\n'.join(successful_runs)
        failed_runs_str = '\n'.join(failed_runs)
        needs_review_str = '\n'.join(needs_review_list)

        body_content = (
            'Successfully processed sites:\n'
            f'{successful_runs_str}\n\n'
            'Failed runs:\n'
            f'{failed_runs_str}\n\n'
            f'The following sites need AMP review:\n'
            f'{needs_review_str}\n\n'
        )

        return subject_notice, body_content

    def send_amp_mail(self, run_outcome, needs_review, notice_msg=None):
        sender = self.qaqc_processor_email
        recipient = self.amp_team_email
        timestamp = datetime.now().strftime('%Y-%m-%d')

        subject_msg, body_msg = self.build_email_content(
            run_outcome, needs_review, notice_msg)

        if recipient:
            email_amp = Mailer(log=_log)
            subject = (f'AMP Auto Data QAQC: {timestamp}'
                       f'{subject_msg}')
            body_content = (
                f'BASE Auto Data QAQC processing on {timestamp}\n\n'
                f'{body_msg}')
            content = email_amp.build_multipart_text_msg(
                sender, recipient, subject, body_content)
            email_amp.send_mail(sender, recipient, content)
        else:
            _log.warning('[EMAIL] AMP recipient is not configured')

    @staticmethod
    def translate_date_str_to_datetime(date_str, date_format):
        """
        The jira date time in the database is format
           YYYY-MM-DD HH:mm:ss.[fraction of seconds]-[utc in 2 digits]
        """
        full_datetime_str = date_str

        datetime_dt = datetime.strptime(
            full_datetime_str, date_format)

        return datetime_dt

    @staticmethod
    def find_process_run_jira_issue_mismatches(
            lookback_dt: datetime, site_ids_with_last_format: list,
            qaqc_issues: Dict, sites_with_last_format: Dict) -> Dict:
        """"""
        uploads_with_missing_issues = {}

        uploads_with_missing_issues_list = [
            site_id for site_id in site_ids_with_last_format
            if site_id not in qaqc_issues.keys()]

        for site_id in uploads_with_missing_issues_list:
            site_upload_info = sites_with_last_format[site_id]
            if site_upload_info.get('processing_timestamp') < lookback_dt:
                site_pid = site_upload_info.get('process_id')
                _log.info(
                    f'{site_id} has last format processing run but '
                    f'no jira issues. Its process_id {site_pid} occurred '
                    f'before the lookback date {lookback_dt}.')
                continue
            uploads_with_missing_issues[site_id] = 'No jira issues found.'

        return uploads_with_missing_issues

    @staticmethod
    def find_issues_in_lookback(
            issue_date_filter: str,
            qaqc_issues: Dict, lookback_dt: datetime,
            issue_type: Optional[str] = None) -> list:
        """
        Return site_id list for jira issue tickets whose issue date
            filter value is more recent than the lookback ts
            and matches the issue_type if specified.
        """
        site_ids = []

        # ToDo: there is probably a more expedient way to do the
        #       below validations
        if issue_date_filter not in (JIRANames.issue_created,
                                     JIRANames.issue_updated):
            _log.warning(
                'Invalid issue_date_filter in '
                f'find_issues_in_lookback: {issue_date_filter}')
            return site_ids

        if issue_type and issue_type not in (
                JIRANames.data_qaqc_issue_id,
                JIRANames.format_qaqc_issue_id):
            _log.warning(
                'Invalid issue_type in '
                f'find_issues_in_lookback: {issue_type}')
            return site_ids

        for site_id, site_issues in qaqc_issues.items():
            for site_issue in site_issues:
                filter_value_dt = site_issue.get(issue_date_filter)
                if filter_value_dt.tzinfo is not None:
                    filter_value_dt = filter_value_dt.replace(tzinfo=None)
                if filter_value_dt < lookback_dt:
                    continue
                if issue_type and site_issue.get('issue_type') != issue_type:
                    continue
                site_ids.append(site_id)

        return list(set(site_ids))

    def driver(self):
        # set 24 lookback time
        start_processing_dt = datetime.now()
        start_processing_str = start_processing_dt.strftime(
            self.ts_util.JIRA_TS_FORMAT)
        _log.info(f'Data Auto Run starting at {start_processing_str}')

        lookback_dt = start_processing_dt - timedelta(
            hours=self.data_auto_lookback_time)

        lookback_str = lookback_dt.strftime(
            self.ts_util.JIRA_TS_FORMAT)
        _log.info(f'lookback_dt: {lookback_str}')

        # set up connection to databases
        self.configure_connections()

        # get sites to consider
        # 1. get sites whose most recent qaqc run is format.
        sites_with_last_format = \
            self.db_handler.get_sites_with_last_process_type(
                self.db_conn_pool['qaqc'])

        if not sites_with_last_format:
            info_msg = 'No sites with last format processing.'
            _log.info(info_msg)
            self.send_amp_mail({}, {}, info_msg)
            return

        site_ids_with_last_format = list(sites_with_last_format.keys())

        # get jira qaqc issues for the sites with last format
        qaqc_issues = self.db_handler.get_qaqc_jira_issue_info(
            self.db_conn_pool['jira'],
            site_list=site_ids_with_last_format,
            jira_project=self.jira_project)

        # catch the rare case in which a site has a process run
        #    and no jira issue tickets
        uploads_with_missing_issues = {}
        if sorted(site_ids_with_last_format) != sorted(
                list(qaqc_issues.keys())):
            uploads_with_missing_issues = \
                self.find_process_run_jira_issue_mismatches(
                    lookback_dt, site_ids_with_last_format,
                    qaqc_issues, sites_with_last_format)

        # find sites with format or data issues status updates within 24 hours
        sites_with_updates = self.find_issues_in_lookback(
            JIRANames.issue_updated, qaqc_issues, lookback_dt)

        # sites with a format qaqc run as the latest processing attempt
        #     and the attempt occurred in last 24 hours.
        recent_format_sites = self.find_issues_in_lookback(
            JIRANames.issue_created, qaqc_issues, lookback_dt,
            JIRANames.format_qaqc_issue_id)

        # Future extension:
        #   Compare sites_with_last_format with recent_format_sites
        #   to detect uploads with failed format process runs. This would
        #   be a double check anything that slips thru the format_qaqc_driver.

        if recent_format_sites:
            pass
            """
            Hook up format issue linking once complete
            self.link_issues.link_format_issues(
                recent_upload_sites, qaqc_issues, lookback_dt)
            """

        review_sites = list(set(sites_with_updates + recent_format_sites))

        if not review_sites:
            info_msg = 'No sites to review.'
            _log.info(info_msg)
            self.send_amp_mail({}, {}, info_msg)
            return

        # need to get the issues again b/c linking issues will
        #    change issue statuses
        review_site_issues = self.db_handler.get_qaqc_jira_issue_info(
            self.db_conn_pool['jira'], site_list=review_sites,
            jira_project=self.jira_project)

        run_candidates, needs_review = self.get_data_qaqc_candidates(
            review_site_issues)

        needs_review.update(uploads_with_missing_issues)

        run_outcome = {}
        if run_candidates:

            # Use the last uploaded format file to extract the resolution
            last_upload_lookup, _ = self.db_handler.get_last_flux_upload(
                self.db_conn_pool['qaqc'], run_candidates)
            potential_res_lookup = self.get_potential_res(last_upload_lookup)

            for site_id in run_candidates:
                run_msg = self.initiate_data_qaqc(
                    site_id, potential_res_lookup)
                run_outcome[site_id] = run_msg
                _log.info(f'{site_id}: {run_msg}')

        if run_outcome or needs_review:
            self.send_amp_mail(run_outcome, needs_review)

        end_processing_dt = datetime.now()
        end_processing_str = end_processing_dt.strftime(
            self.ts_util.JIRA_TS_FORMAT)
        processing_time = end_processing_dt - start_processing_dt
        processing_time_str = str(processing_time.seconds/60)
        _log.info(f'Processing end: {end_processing_str}\n'
                  f'Processing time: {processing_time_str} minutes')


if __name__ == '__main__':
    DataQAQCAutoRunHandler().driver()
