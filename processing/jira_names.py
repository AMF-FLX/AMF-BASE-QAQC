#!/usr/bin/env python

from logger import Logger

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class JIRANamesException(Exception):
    pass


class JIRANames:
    format_QAQC_issue_name = 'Format QAQC Results'
    format_qaqc_issue_id = '10100'
    data_QAQC_issue_name = 'Data QAQC Results'
    data_qaqc_issue_id = '10101'
    site_id = 'customfield_10206'
    site_id_field_name = 'Site ID'
    process_ids = 'customfield_10203'
    process_id_field_name = 'Process ID(s)'
    upload_token = 'customfield_10205'
    upload_comment = 'customfield_10400'
    organizations = 'customfield_10002'
    report_link = 'customfield_10207'
    ftp_link = 'customfield_10208'
    time_res = 'customfield_10204'
    start_end_dates = 'customfield_10600'
    participants = 'customfield_10000'
    sandbox = 'customfield_10300'
    format_QAQC_email_added = '91'
    format_QAQC_canceled = '131'
    format_QAQC_sent_qaqc_review = '101'
    format_QAQC_ready_for_data = '171'
    format_QAQC_reopen_issue = '181'
    format_QAQC_replacement_file_uploaded = 'tbd'
    format_waiting_for_support = '211'
    data_QAQC_replace_with_upload = '41'
    sub_issue_name = 'Issue'
    data_sub_issue_years = 'customfield_10800'
    data_sub_issue_fixed = 'Fixed'
    data_sub_issue_canceled = 'Canceled'
    data_sub_issue_known_issue = 'Known Issue'
    data_sub_issue_not_issue = 'Not an issue'
    upload_comment = 'customfield_10400'
    base_update_resolution = 'BASE Updated (see linked issue)'
    label_results_sent = 'Results_Sent'
    label_self_review = 'Self-Review'
    reminder_schedule = 'customfield_10700'
    issue_created = 'created'
    issue_updated = 'updated'
    issue_reporter = 'reporter'
    issue_participants = 'Request participants'
    issue_status = 'status'
    issue_description = 'description'
    format_status_attempt_data_qaqc = 'Attempt Data QAQC'
    format_status_canceled = 'Canceled'
    format_status_replace_upload = 'Replaced with Upload'
    data_status_replace_upload = 'Replace with Upload'
    data_status_canceled = 'Canceled'
    data_status_publishable = 'Publishable'

    class Status:
        __field_name__ = 'status'
        waiting_for_customer_id = '10002'
        waiting_for_customer_name = 'Waiting for Customer'
        format_qaqc_complete_id = '10200'
        format_qaqc_complete_name = 'Format QAQC Complete'
        format_attempt_data_qaqc_name = 'Attempt Data QAQC'
        format_attempt_data_qaqc_id = '10213'

    class RequestType:
        __field_name__ = 'Customer Request Type'
        format_qaqc = 'Format QAQC'

    class ReminderOptions:
        auto = '10200'
        one_week = '10202'
        one_month = '10203'
        no_reminder = '10201'

    def strip_customfield(self, customfield):
        try:
            return self.__getattribute__(customfield).split('_')[-1]
        except JIRANamesException:
            _log.error(f'{customfield} is not an JIRANames attribute')
            raise JIRANamesException


class JIRATimestamp:
    jira_dt_api = '%Y-%m-%dT%H:%M:%S.%f%z'
    start_end_date = '%Y%m%d'