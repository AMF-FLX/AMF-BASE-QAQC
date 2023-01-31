#!/usr/bin/env python

from logger import Logger
from status import StatusCode
import getpass
import json
import os
from status import StatusEncoder

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class ProcessStatus(object):

    def __init__(self, process_type, filename, process_datetime,
                 process_log_file, headers, status_start_msg, status_end_msg,
                 statuses, report_statuses, process_id=None, process_code=None,
                 process_resolution=None, files_combined=None,
                 report_title=None, upload_filename=None, check_summary=None):
        """
        Object that wraps up information from the process run

        :param statuses: list of status objects converted to report objects

        internally built pieces:
        process_type: str, indicator of what type of process:
            "file_format" or "combo_qaqc"
        process_log_file: str, filename of process qaqc log
            (this the logger from which the status object reads)
        processor: str, name of userid of person processing
        process_datetime: process start time
        process_confirmation: dict returning information about the process run
        files: dict returning information about files
        """
        if headers is not None:
            header_str = ', '.join(headers)
        else:
            header_str = 'Unable to extract headers from file'
        self._process_status = {
            'process_type': process_type,
            'process_log_file': process_log_file,
            'processor': getpass.getuser(),
            'process_datetime': process_datetime,
            'process_confirmation': {
                'status_start_msg': status_start_msg,
                'status_end_msg': status_end_msg
            },
            'files': {
                'new': [os.path.basename(filename)],
                'headers': header_str
            },
            'check_summary': check_summary,
            'checks': []
        }
        self._report_statuses = report_statuses
        self._statuses = statuses
        if process_type == 'File Format':
            self._process_status['checks'] = []
            self._process_status['report_title'] = report_title
            self._process_status['files']['upload_filename'] = \
                os.path.basename(upload_filename)
            self.process_code = self.calc_status_code(statuses)
        elif process_type == 'BASE Generation':
            self._process_status['process_resolution'] = process_resolution
            self._process_status['process_id'] = process_id
            self._process_status['files']['combined'] = files_combined
            self._process_status['checks'] = {}
            self.process_code = process_code
        self._process_status['process_confirmation']['status_code'] = \
            self.report_status_code_str(self.process_code)

    def calc_status_code(self, status_list):
        """
        :return: int overall status code for the list of statuses
        """
        status_codes = []

        for i in range(0, len(status_list)):
            status_codes.append(status_list[i].get_status_code())

        return min(status_codes)

    def report_status_code_str(self, status_code):
        """
        return the str status code
        :param status_code:
        :return:
        """
        status_map = StatusCode()
        return status_map.get_str_repr(status_code=status_code)

    def write_report_json(self):
        self._process_status['checks'] = self._report_statuses
        return json.dumps(self._process_status)

    def write_status_json(self):
        return json.dumps(self._statuses, cls=StatusEncoder)
