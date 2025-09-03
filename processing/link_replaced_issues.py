#!/usr/bin/env python

import datetime as dt
import os
import time
from typing import Dict, Union, Optional

from jira_interface import JIRAInterface
from jira_names import JIRANames, JIRATimestamp
from logger import Logger
from utils import TimestampUtil

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'

_log = Logger().getLogger(__name__)


class LinkIssuesError(Exception):
    pass


class LinkIssues:
    def __init__(self, jira: Optional[JIRAInterface] = None):
        self.ts_util = TimestampUtil()
        if jira:
            self.jira = jira
        else:
            self.jira = JIRAInterface(configure_organizations=False)
        self.jira_project = self.jira.jira_project

    def get_uploaded_files(self, site_id: str, qaqc_conn) -> list:
        return qaqc_conn.get_upload_file_info_for_site(site_id)

    def _fuzzy_match_filename(
            self, filename: str, candidate_filename: str,
            filename_has_ext: bool = True) -> bool:
        filename_no_ext = filename
        if filename_has_ext:
            filename_no_ext = os.path.splitext(filename)[0]
        if filename_no_ext.lower() in candidate_filename.lower():
            return True
        filename_pieces = filename_no_ext.split('_')
        filename_last_piece = filename_pieces[-1]
        if filename_last_piece.isdigit() and len(filename_last_piece) > 4:
            filename_no_end_date = '_'.join(filename_pieces[:-1])
            if filename_no_end_date.lower() in candidate_filename.lower():
                return True
        return False

    def _fuzzy_match_filename_to_uploaded_files(
            self, filename: str, uploaded_files: list, process_id: str) -> (
            Union[None, str], dict):
        filename_pieces = filename.split('.')
        filename_no_ext = '.'.join(filename_pieces[:-1])
        for uploaded_file_info in uploaded_files:
            if process_id == uploaded_file_info['process_id']:
                continue
            candidate_filename = uploaded_file_info['data_file']
            is_match = self._fuzzy_match_filename(
                filename_no_ext, candidate_filename, filename_has_ext=False)
            if is_match:
                return uploaded_file_info['process_id'], uploaded_file_info
        return None, dict()

    def assess_candidate_uploaded_file(
            self, candidate_upload_token: str, candidate_file_info: dict,
            process_file: str, linked_files: dict,
            process_date_range: tuple) -> (Union[None, str], bool):

        # Note: For uploads without JIRA issues
        #       and uploads with canceled JIRA issues,
        #    Send AMP an email in the link_updated_issues method

        candidate_start_date = candidate_file_info['start_time']
        candidate_date_range = (candidate_start_date,
                                candidate_file_info['end_time'])

        # If candidate file does not have dates but autocorrected
        #    file does, use autocorrected file dates
        if candidate_start_date is None:
            candidate_start_date = candidate_file_info[
                'autocorrected_start_time']
            candidate_date_range = (
                candidate_start_date,
                candidate_file_info['autocorrected_end_time'])

        linked_process_id = None

        # If the candidate file does not have extractable dates,
        #    try matching on the file name
        if candidate_start_date is None:
            if self._fuzzy_match_filename(
                    process_file,  # process_info['upload_file'],
                    candidate_file_info['data_file']):
                linked_process_id = candidate_file_info['process_id']
            if linked_process_id is not None:
                # If a match is found, consider it a full match
                linked_files[linked_process_id] = candidate_file_info
                return candidate_upload_token, True
            # If filename match is not found,
            #    continue with next candidate
            return None, False

        # Check date overlap
        linked_process_id, full_date_range_coverage = \
            self.check_replacement_date_coverage(
                process_date_range, linked_files,
                candidate_date_range, candidate_file_info)

        if linked_process_id:
            linked_files[linked_process_id] = candidate_file_info
            return candidate_upload_token, full_date_range_coverage

        return None, False

    def convert_str_to_datetime(
            self, timestamp_str: Union[tuple, list]) -> Union[tuple, list]:
        ts_fmt = self.ts_util.JIRA_TS_FORMAT
        if len(timestamp_str[0]) < 9:
            ts_fmt = '%Y%m%d'
        date_values = (
            dt.datetime.strptime(timestamp_str[0], ts_fmt).date(),
            dt.datetime.strptime(timestamp_str[1], ts_fmt).date())
        if isinstance(timestamp_str, list):
            return list(date_values)
        return date_values

    def make_dates_series(
            self, date_range: tuple,
            trim_date_range: Union[None, tuple] = None) -> (list, set):
        if not isinstance(date_range, list):
            date_range = list(date_range)
        date_start_end = self.convert_str_to_datetime(date_range)
        if trim_date_range:
            if not isinstance(trim_date_range, list):
                trim_date_range = list(trim_date_range)
            trim_start_end = self.convert_str_to_datetime(trim_date_range)
            if trim_start_end[0] > date_start_end[0]:
                date_start_end[0] = trim_start_end[0]
            if trim_start_end[1] < date_start_end[1]:
                date_start_end[1] = trim_start_end[1]
        number_of_days = date_start_end[1] - date_start_end[0]
        date_series = set()
        for add_day in range(number_of_days.days + 1):
            a_date = date_start_end[0] + dt.timedelta(days=add_day)
            date_series.add(a_date)
        return date_start_end, date_series

    def check_replacement_date_coverage(
            self, process_date_range: tuple, files_replaced: dict,
            candidate_date_range: tuple,
            candidate_file_info: dict) -> (Union[str, None], bool):

        process_start_end, process_dates = self.make_dates_series(
            process_date_range)
        candidate_start_end, candidate_dates = self.make_dates_series(
            candidate_date_range, trim_date_range=process_date_range)
        candidate_process_id = candidate_file_info['process_id']

        # If candidate file has same start (or within a day)
        #       and the same (within a day) or greater end date
        #    Consider the full date range covered
        if len(process_dates.difference(candidate_dates)) < 3:
            return candidate_process_id, True

        # Determine if the candidate file overlaps the process file
        #    by at least a month. If not, end.
        if len(process_dates.intersection(candidate_dates)) < 30:
            return None, False

        # If there are not already identified files_replaced, return
        if not files_replaced:
            return candidate_process_id, False

        # If so, compare with other replaced_files
        # Extract all the dates
        linked_files_dates = set()
        for linked_process_id, linked_process_info in files_replaced.items():
            linked_date_range = (linked_process_info['start_time'],
                                 linked_process_info['end_time'])
            if linked_date_range[0] == 'None':
                linked_date_range = (
                    linked_process_info['autocorrected_start_time'],
                    linked_process_info['autocorrected_end_time'])
            linked_start_end, linked_dates = self.make_dates_series(
                linked_date_range, trim_date_range=process_date_range)
            if linked_dates:
                linked_files_dates.update(linked_dates)

        # Detect the dates of gaps
        gap_dates = process_dates.difference(linked_files_dates)

        # If there is no overlap in the gaps and the candidate file, return
        if len(gap_dates.intersection(candidate_dates)) < 30:
            return None, False

        # add the candidate dates to the existing linked file dates
        linked_files_dates.update(candidate_dates)
        if len(process_dates.difference(linked_files_dates)) < 3:
            return candidate_process_id, True

        return candidate_process_id, False

    def find_files_replaced_by_upload(
            self, upload_token: str, upload_info: dict) -> (list, list):
        site_id = upload_info['SITE_ID']
        uploaded_files = self.get_uploaded_files(site_id)

        # If there is only one uploaded file, then return empty lists
        if len(uploaded_files) < 2:
            return [], []

        processing_store = upload_info['reports']
        linked_upload_tokens = set()

        # step thru all the processing runs in the current upload
        for process_id, process_info in processing_store.items():
            # some processing runs may be for a file that replaces
            #    several previous uploads
            linked_files = process_info.setdefault('files_replaced', {})
            date_range = (process_info['start_time'], process_info['end_time'])

            # If the date range was not extractable from the process run,
            #    Try to match to a candidate replacement file by the file name
            if date_range[0] == 'None':
                linked_process_id, linked_upload_info = \
                    self._fuzzy_match_filename_to_uploaded_files(
                        process_info['upload_file'], uploaded_files,
                        process_id=process_id)
                if linked_process_id:
                    # If a match on the filename was found, assume that the
                    #    match is the only replaced file and
                    #    continue to next process run
                    linked_files[linked_process_id] = linked_upload_info
                    linked_upload_tokens.add(
                        linked_upload_info['upload_token'])
                    continue

            # Identify most recent files uploaded that cover the date range of
            #    file in the process run
            for candidate_file_info in uploaded_files:
                candidate_upload_token = candidate_file_info['upload_token']
                # If candidate file is part of current upload, skip it
                if candidate_upload_token == upload_token:
                    continue

                linked_upload_token, full_date_range_coverage = \
                    self.assess_candidate_uploaded_file(
                        candidate_upload_token, candidate_file_info,
                        process_info['upload_file'], linked_files, date_range)

                if linked_upload_token:
                    # linked_files is updated in
                    #   assess_candidate_uploaded_file if linked file found
                    linked_upload_tokens.add(linked_upload_token)

                # If full data coverage is met, stop looking thru candidates
                if full_date_range_coverage:
                    break

        return uploaded_files, list(linked_upload_tokens)

    def get_jira_format_issues_for_site(
            self, site_id: str, new_report_key: str) -> (
            dict, Union[None, dict]):
        # pop off the current report using the new_report_key
        site_format_issues = self.jira.get_format_issues(site_id=site_id)
        if new_report_key not in site_format_issues.keys():
            return dict(), None
        new_report_info = site_format_issues.pop(new_report_key)
        return site_format_issues, new_report_info

    def parse_file_date_range(self, file_date_range):
        dates_list = file_date_range.split(' ')
        dates_tuple_list = []
        for dates_entry in dates_list:
            dates_pieces = dates_entry.split('-')
            dates_pieces = [x if x != 'None' else None for x in dates_pieces]
            dates_tuple_list.append((dates_pieces[0], dates_pieces[1]))
        return dates_tuple_list

    def link_jira_issues_with_replaced_files(
            self, site_id: str, new_report_key: str, upload_tokens: list,
            jira_interface: JIRAInterface) -> Union[dict, None]:
        # get all Format JIRA issues for a site:
        #    key = issue key; value = {file date range, state, upload_token}
        # previous reports are ordered by descending report key
        #    (i.e., most recent first)
        previous_reports, current_report = \
            self.get_jira_format_issues_for_site(site_id, new_report_key)

        if not current_report:
            _log.error(f'Report {new_report_key} not found in site {site_id} '
                       'Format QA/QC reports.')
            return None

        if not previous_reports:
            if not upload_tokens:
                _log.info('No previous Format QA/QC reports.')
                return None
            upload_tokens_str = ', '.join(upload_tokens)
            _log.error('Replacement files found but no previous JIRA '
                       'Format QA/QC reports found. Upload tokens: '
                       f'{upload_tokens_str}')
            return None

        linked_reports = {}
        for upload_token in upload_tokens:
            is_linked = False
            for report_key, report_details in previous_reports.items():
                if report_details['upload_token'] == upload_token:
                    is_linked = self.link_issue(new_report_key, report_key,
                                                jira_interface)
                    linked_reports[report_key] = report_details
                    break
            if not is_linked:
                _log.error('Count not find JIRA issue for '
                           f'upload {upload_token}')

        return linked_reports

    def link_issue(self, new_report_key: str, report_key: str,
                   jira_interface: JIRAInterface) -> bool:
        try:
            jira_interface.add_related_link(parent_issue=new_report_key,
                                            child_issue=report_key,
                                            link_type='Updates')
            return True
        except LinkIssuesError as e:
            _log.error(f'Failed to link issue {report_key} '
                       f'to {new_report_key}: {e}')
            return False

    def update_issue_status(self, report_key: str,
                            jira_interface: JIRAInterface) -> bool:
        try:
            jira_interface.set_issue_status(
                key=report_key,
                transition=JIRANames.format_QAQC_replacement_file_uploaded)
            return True
        except LinkIssuesError as e:
            _log.error(f'Failed to update issue {report_key} '
                       f'to Replacement File Uploaded: {e}')
            return False

    def resolve_linked_issues(self, linked_issues: list,
                              jira_interface: JIRAInterface):
        # NOTE: This is placeholder. It will come in the next PR.
        pass

    def link_updated_issues(self, site_id: str, new_report_key: str,
                            upload_tokens: list,
                            jira_interface: JIRAInterface):
        linked_issue_keys = self.link_jira_issues_with_replaced_files(
            site_id=site_id, new_report_key=new_report_key,
            upload_tokens=upload_tokens, jira_interface=jira_interface)

        if linked_issue_keys:
            for linked_issue_key in linked_issue_keys:
                self.update_issue_status(linked_issue_key, jira_interface)
                time.sleep(2)
            self.resolve_linked_issues(linked_issue_keys)

    def get_format_issues_in_lookback(self, site_issues, lookback_dt):
        """
        Lookback thru the site's issues, keep the format issues that
            were created in the lookback period
        """
        format_issue_keys = []
        for site_issue in site_issues:
            issue_type = site_issue.get('issue_type')
            # at the first data QAQC issue type, exit
            if issue_type == JIRANames.data_qaqc_issue_id:
                return format_issue_keys
            issue_created_datetime = site_issue.get('created')
            if issue_created_datetime < lookback_dt:
                issue_key = site_issue.get('issue_key')
                format_issue_keys.append(issue_key)
                continue
            # once a format issue is found that was not
            #    created since the lookback time, stop looking
            break

        return format_issue_keys

    def parse_filenames_from_description(self, description):
        """
        Parse the format issue description
        """
        filenames = []
        if not description:
            return filenames

        desc_parts = description.split('\n')

        for desc_part in desc_parts:
            if desc_part.startswith('QAQC'):
                continue
            if desc_part.startswith('Automated'):
                continue
            filename = desc_part.split(':')[0]
            filenames.append(filename)

        return filenames

    def extract_dates_from_filename(self, filenames):
        """
        attempt to extract the dates from the filename;
            simple approach
        Note: this is similar to the functionality in
            file_name_verifier but not quite.
        """
        fn_dates = {}

        for filename in filenames:
            fn = filename.split('.')[0]
            fn_pieces = fn.split('_')

            if len(fn_pieces) >= 4:
                return {}
            # assume the valid filename format
            ts_start = fn_pieces[2]
            ts_end = fn_pieces[3]

            not_valid_ts_pieces = any([
                (not ts_start.startswith('20') and not ts_start.startswith('19')),
                (not ts_end.startswith('20') and not ts_end.startswith('19')),
                len(ts_start) != self.ts_util.YYYYMMDDHH_LEN,
                len(ts_end) != self.ts_util.YYYYMMDDHH_LEN])

            if not_valid_ts_pieces:
                return {}

            try:
                ts_start = ts_start[0:self.ts_util.YYYYMMDD_LEN]
                ts_end = ts_end[0:self.ts_util.YYYYMMDD_LEN]
                fn_dates[filename] = f'{ts_start}-{ts_end}'
            except Exception as e:
                _log.error(f'Failed to extract date from filename: {filename}: {e}')
                return {}

        return fn_dates

    def extract_dates_if_missing(self, format_issues):
        """
        If the start_end_dates are missing, try to extract from the
            file names in the description
        """
        missing_date = True
        for format_issue in format_issues:
            format_issue.update({
                'missing_date': missing_date,
                'filenames': []})
            start_end_dates = format_issue.get('start_end_dates')
            if not start_end_dates:
                continue
            if 'None' not in start_end_dates:
                missing_date = False
            start_end_date_list = start_end_dates.split('\n')
            if not missing_date:
                format_issue.update({
                    'start_end_dates': start_end_date_list,
                    'missing_date': missing_date,})
                continue
            # parse the description text
            description = format_issue.get('description')
            if not description:
                format_issue.update({'missing_date': missing_date})
                continue
            filenames = self.parse_filenames_from_description(description)
            format_issue.update({'filenames': filenames})

            # deepcopy
            filename_dates = self.extract_dates_from_filename(filenames)
            filenames_matched = []
            # go thru the date entries
            for se_date in start_end_date_list:
                # if the entry is not None-None
                # match to the filename and pop it from the list of filename dates
                if 'None' not in se_date:
                    for fn_key, fn_date in filename_dates.items():
                        if fn_date == se_date:
                            filenames_matched.append(fn_key)
                            continue

            unmatched_dates = []
            for fn_key, fn_date in filename_dates.items():
                if fn_key in filenames_matched:
                    continue
                unmatched_dates.append(fn_date)

            # filter start end dates that have values
            date_values = [se_date for se_date in start_end_date_list
                           if 'None' not in se_date]

            # double check that
            #    number of None values == the unmatched filename dates
            if len(start_end_date_list) - len(date_values) != len(unmatched_dates):
                format_issue.update({'missing_date': missing_date})

            format_issue.update({
                'start_end_dates': date_values.extend[unmatched_dates],
                'missing_date': False})

    def link_format_issues(self, recent_upload_sites: list,
                           qaqc_issues: Dict, lookback_dt: dt.datetime):
        """
        Detect and link format issues as applicable

        :param recent_upload_sites: list of site_ids
        :param qaqc_issues: dictionary of jira format and data issues
            by site_id (see below)
        :param jira: JIRAInterface instance for queries to the jira project
        :param lookback_dt: date time from which more recent issues
            should be evaluated for links

        qaqc_issue dictionary value fields:
            issue_key
            issue_status
            issue_type
            process_id
            labels
            summary
            created  # timestamp
            updated  # timestamp
            jira_id
        """
        for site_id in recent_upload_sites:
            site_issues = qaqc_issues[site_id]

            # get all format issues created in last 24 hours
            format_keys = self.get_format_issues_in_lookback(
                site_issues, lookback_dt)

            # get all the format issues with:
            #    start + end dates
            #    filenames with dates extracted if possible
            format_issues = self.jira.get_format_issues(
                site_id=site_id,
                additional_fields=[JIRANames.issue_description])

            # try to update any format issue without start_end_dates
            self.extract_dates_if_missing(format_issues)

            # start in ascending order, go thru the recent format issues
            for format_key in sorted(format_keys):
                format_issue = format_issues.get(format_key)
                # RESUME HERE
                # See if new file extend data record (compare to last data run)
                # look for exact matches on start_end_date
                # then check if new file extends
