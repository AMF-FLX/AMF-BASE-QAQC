#!/usr/bin/env python

from logger import Logger

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class JIRANamesException(Exception):
    pass


class JIRANames:
    format_QAQC_issue_name = 'Format QAQC Results'
    data_QAQC_issue_name = 'Data QAQC Results'
    site_id = 'customfield_10206'
    process_ids = 'customfield_10203'
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
    sub_issue_name = 'Issue'
    upload_comment = 'customfield_10400'
    base_update_resolution = 'BASE Updated (see linked issue)'
    label_results_sent = 'Results_Sent'
    label_self_review = 'Self-Review'

    class Status:
        __field_name__ = 'status'
        waiting_for_customer_id = '10002'
        waiting_for_customer_name = 'Waiting for Customer'
        format_qaqc_complete_id = '10200'
        format_qaqc_complete_name = 'Format QAQC Complete'

    class RequestType:
        __field_name__ = 'Customer Request Type'
        format_qaqc = 'Format QAQC'

    def strip_customfield(self, customfield):
        try:
            return self.__getattribute__(customfield).split('_')[-1]
        except JIRANamesException:
            _log.error(f'{customfield} is not an JIRANames attribute')
            raise JIRANamesException
