#!/usr/bin/env python
import csv
import json
import os
import status
import time
from configparser import ConfigParser
from datetime import datetime as dt
from jira_interface import JIRAInterface
from jira_names import JIRANames
from logger import Logger
from report_status import ReportStatus, ReportStatusException
from site_attrs import SiteAttributes

__author__ = 'Norm Beekwilder, Danielle Christianson'
__email__ = 'norm.beekwilder@gmail.com, dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


def gen_description(report_statuses) -> str:
    """
    Helper method to generate the JIRA ticket description
        from the list of status objects

    :param report_statuses: list of dictionary objects
    """
    sc = status.StatusCode()
    status_code = 0
    status_counts = [0, 0, 0, 0]
    # Status codes are -3 to 0. To change these to indices for the
    #   status_counts above, 3 must be added to the status codes below.

    for qaqc_check in report_statuses:
        for rs in report_statuses[qaqc_check]:
            if rs.get_status_code() < status_code:
                status_code = rs.get_status_code()
            status_counts[3 + rs.get_status_code()] += 1
    return 'critical({c}), error({e}), warning({w}), ok({o})' \
        .format(c=status_counts[3 + sc.FATAL], e=status_counts[3 + sc.ERROR],
                w=status_counts[3 + sc.WARNING],
                o=status_counts[3 + sc.OK])


class DataReportGenError(Exception):
    pass


class DataReportGen:
    def __init__(self):
        self.jira = JIRAInterface()
        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            cfg_section = 'VERSION'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.test = config.getboolean(cfg_section, 'test')
            else:
                self.test = True
            cfg_section = 'UI'
            if config.has_section(cfg_section):
                self.ui_prefix = config.get(cfg_section, 'ui_prefix')
            else:
                raise DataReportGenError(
                    'Unable to find UI section in the config file.')
            self.report_link_template = (f'{self.ui_prefix}'
                                         'qaqc-report/?site_id={s}&'
                                         'report_id={p}')
            cfg_section = 'REPORT_EMAIL'
            if config.has_section(cfg_section):
                self.report_link_template = config.get(
                    cfg_section, 'report_link')
                email_text_file = config.get(cfg_section, 'messages_json')
                self.instruction_link = config.get(
                    cfg_section, 'self_review_instruction_link')
                # self_review_sites_file is a csv, one site per line
                #   format chosen to facilitate generation/tracking via export
                #   of a google spreadsheet, etc
                self_review_sites_file = config.get(
                    cfg_section, 'self_review_sites_file')
            else:
                raise DataReportGenError(
                    'REPORT_EMAIL section missing in config file.')
            cfg_section = 'TEST_INFO'
            if config.has_section(cfg_section):
                self.test_site = config.get(cfg_section, 'test_site')
                self.test_user = config.get(cfg_section, 'tester_jira_user')
                self.test_participant = config.get(
                    cfg_section, 'tester_jira_user2')
            elif self.test:
                raise DataReportGenError(
                    'TEST_INFO section missing in config file.')

        with open(os.path.join(cwd, email_text_file)) as f:
            general_messages_text = json.load(f)
            self.email_text_lookup = general_messages_text[
                'data_qaqc_report_email']

        self.site_attrs = SiteAttributes().get_site_dict()
        self.self_review_sites = self._read_sites_file(
            cwd, self_review_sites_file)

    def _read_sites_file(self, cwd: str, sites_file: str) -> list:
        self_review_sites = []
        with open(os.path.join(cwd, sites_file)) as f:
            csv_file = csv.reader(f)
            for site_id_row in csv_file:
                site_id = site_id_row[0]
                if site_id not in self.site_attrs.keys():
                    _log.warning(f'{site_id} is not a valid site')
                    continue
                self_review_sites.append(site_id)
        return self_review_sites

    def _get_site_team_members(self, site_id: str) -> dict:
        """
        Get dictionary of site team members from the webservices

        :param site_id: site identifier
        """
        try:
            return ReportStatus().get_site_users(site_id)

        except ReportStatusException as e:
            _log.warning(f'Invalid return for site team users endnpoint: {e}.')
            return {}

    def validate_reporter_info(self, data_qaqc_info, site_id):
        """
        Check for reporter_id value. If none exists, use reporter_name to
        find corresponding email address. Set reporter_id to that email
        address so that Jira can use it to populate the ticket reporter.

        This happens when the site's Site General Info team member email
        address does not match the user's AmeriFlux account email address.
        """

        if data_qaqc_info.get('reporter_id'):
            return

        site_team_members = self._get_site_team_members(site_id)
        pi_name = data_qaqc_info['reporter_name']

        members_without_amf_accounts = site_team_members.get('other_users', {})
        pi_email = members_without_amf_accounts.get(pi_name, '')

        data_qaqc_info['reporter_id'] = pi_email

        if not pi_email:
            _log.warning(f'Could not find email address for {pi_name}. '
                         f'Data QA/QC ticket will have anonymous reporter')

        return

    def gen_report(self, site_id, time_res, process_id, input_file_order,
                   status_list, ftp_link, force_amp_review=False):
        previous_key = self.jira.get_prior_data_qaqc_key(site_id)
        format_process_ids = []
        format_filenames = []
        for f in input_file_order:
            if f.proc_id is not None and f.proc_id not in format_process_ids:
                format_process_ids.append(f.proc_id)
                format_filenames.append(f.name)
        data_qaqc_info = ReportStatus().get_base_info(
            site_id,
            {'process_ids': format_process_ids})

        if not self.test:
            self.validate_reporter_info(data_qaqc_info, site_id)

        try:
            # truncate the last_upload_timestamp
            #   and deal with one ts format
            last_upload_ts = dt.strftime(dt.strptime(
                data_qaqc_info['last_upload_timestamp'][:21],
                "%Y-%m-%dT%H:%M:%S.%f"), "%b %d, %Y")
        except Exception:
            try:
                # truncate the last_upload_timestamp
                #   and deal with the other ts format
                last_upload_ts = dt.strftime(dt.strptime(
                    data_qaqc_info['last_upload_timestamp'][:21],
                    "%Y-%m-%dT%H:%M:%S"), "%b %d, %Y")
            except Exception:
                last_upload_ts = 'No Date Available'
        start_time = input_file_order[0].start[:8]
        end_time = input_file_order[-1].end[:8]
        summary = (f'Data Results | {site_id} {time_res} '
                   f'{start_time} - {end_time} | '
                   f'Using uploads through {last_upload_ts}')
        report_link = self.report_link_template.format(s=site_id, p=process_id)
        if self.test:
            issue_key = self.jira.create_data_issue(
                self.test_site, process_id, time_res, self.test_user, summary,
                'QAQC completed with the following results '
                f'{gen_description(status_list)}',
                [self.test_participant], ftp_link, report_link)
        else:
            issue_key = self.jira.create_data_issue(
                site_id, process_id, time_res, data_qaqc_info['reporter_id'],
                summary, 'QAQC completed with the following results '
                f'{gen_description(status_list)}',
                data_qaqc_info['participant_ids'], ftp_link, report_link)

        # pause to give JIRA time to sync up and set the request type
        time.sleep(10)

        if previous_key is not None:
            self.jira.add_related_link(
                issue_key, previous_key, link_type='Updater')
            sub_issues = self.jira.get_sub_issues_to_link(previous_key)
            for s in reversed(sub_issues):
                self.jira.add_related_link(issue_key, s, link_type='Issue')
                # pause to give JIRA time to sync up
                time.sleep(2)
        format_keys = []
        for p in data_qaqc_info['process_ids']:
            key = self.jira.get_format_qaqc_key(p)
            if key is not None:
                format_keys.append(key)
        for k in format_keys:
            self.jira.add_related_link(issue_key, k, link_type='FormatQA/QC')
            # pause to give JIRA time to sync up; increasing time b/c there
            #     can be many links to update esp for longer data records
            time.sleep(10)

        is_self_review_site = site_id in self.self_review_sites

        if force_amp_review:
            is_self_review_site = False
            _log.info(f'{site_id} is in the self_review_site.csv file and the '
                      'command line force_amp_review flag = '
                      f'{force_amp_review} is overwriting it.')

        msg = self.gen_message(
            site_id, is_self_review_site, issue_key, data_qaqc_info,
            format_filenames, format_process_ids, ftp_link)
        self.jira.add_comment(issue_key, msg, public=is_self_review_site)

        if is_self_review_site:
            # pause to give JIRA time to sync up
            time.sleep(2)
            self.jira.add_label(issue_key,
                                labels=[JIRANames.label_self_review,
                                        JIRANames.label_results_sent])

        return issue_key

    def gen_message(self, site_id: str, is_self_review_site: bool,
                    issue_key: str, site_dict: dict,
                    format_filenames: list,
                    format_process_ids: list, ftp_link: str) -> str:

        # set message type for specific text sections
        msg_type = 'amp_review'
        if is_self_review_site:
            msg_type = 'self_review'

        participants = ', '.join(site_dict.get('participant_names', []))
        if len(participants) > 0:
            participants = ', ' + participants

        # text sections
        salutation = self.email_text_lookup.get('salutation').format(
            r=site_dict['reporter_name'], p=participants, s=site_id,
            n=self.site_attrs[site_id])

        intro = self.email_text_lookup.get(msg_type).get('intro').format(
            ui=self.ui_prefix)
        qaqc_desc = self.email_text_lookup.get('data_qaqc_desc').format(
            ui=self.ui_prefix)

        additional_info = ''
        if msg_type == 'amp_review':
            additional_info = self.email_text_lookup.get(
                msg_type).get('additional_info').format(ui=self.ui_prefix)

        results = self.email_text_lookup.get(
            msg_type).get('qaqc_results').format(
                site_id=site_id, ftp=ftp_link,
                instructions_link=self.instruction_link)

        instructions = self.email_text_lookup.get(msg_type).get(
            'instructions').format(i=issue_key, ui=self.ui_prefix,
                                   ftp=ftp_link)

        closer = self.email_text_lookup.get(
            msg_type).get('closer').format(ftp=ftp_link)

        reference_info = ''
        if msg_type == 'self_review':
            reference_info = self.email_text_lookup.get(msg_type).get(
                'reference_info').format(ftp=ftp_link, qaqc_desc=qaqc_desc)
            qaqc_desc = ''

        format_qaqc_resources = self.email_text_lookup.get(
            'format_qaqc_resources').format(
                f='\n'.join([
                    f'{fn} ([online report {p}|'
                    f'{self.report_link_template.format(s=site_id, p=p)}])'
                    for fn, p in zip(format_filenames,
                                     format_process_ids)]))

        # put all the pieces together
        msg = (f'{salutation}{intro}{qaqc_desc}{additional_info}{results}'
               f'{instructions}{closer}{reference_info}'
               f'{format_qaqc_resources}')

        return msg

    def driver(self, site_id, time_res, process_id, input_file_order,
               status_list, ftp_link, force_amp_review=False):
        if input_file_order is None:
            # test run don't gen report; using input_file_order for local
            #    test runs that use a file arg for main (i.e., don't change
            #    this to self.test unless the testing strategy is reworked.
            return 'QAQC-####'
        return self.gen_report(
            site_id, time_res, process_id, input_file_order, status_list,
            ftp_link, force_amp_review)
