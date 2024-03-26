#!/usr/bin/env python

import getpass
import json
import os
import urllib.request
from configparser import ConfigParser
from http import HTTPStatus
from logger import Logger
from urllib.error import HTTPError

__author__ = 'Norm Beekwilder, Sy-Toan Ngo'
__email__ = 'norm.beekwilder@gmail.com, sytoanngo@lbl.gov'

_log = Logger().getLogger(__name__)


class ReportStatus:
    """upload status report to web server"""

    def __init__(self):
        ''' Initialize variables on loading of class here '''
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            cfg_section = 'WEBSERVICES'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.report_status_ws = config.get(
                        cfg_section, 'report_status')
                self.register_base_ws = config.get(
                        cfg_section, 'register_base_qaqc')
                self.get_base_input = config.get(
                        cfg_section, 'get_base_input')
                self.publish_base_ws = config.get(
                        cfg_section, 'publish_base')
                self.file_qaqc_url_prefix = config.get(
                        cfg_section, 'file_qaqc_url_prefix')
                self.siteres_qaqc_url_prefix = config.get(
                        cfg_section, 'siteres_qaqc_url_prefix')
                self.get_base_report_info = config.get(
                        cfg_section, 'get_base_report_info')
                self.get_site_users_ws = config.get(
                        cfg_section, 'get_site_users')
            else:
                self.report_status_ws = None
                self.register_base_ws = None
                self.file_qaqc_url_prefix = None
                self.siteres_qaqc_url_prefix = None
                _log.error('Cannot find web service from config.')
            cfg_section = 'VERSION'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.code_version = config.get(cfg_section, 'code_version')
            else:
                self.code_version = 0
                _log.error('Cannot find code version from config.')

    def _basic_post_request_core(self, msg, url):
        try:
            req = urllib.request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            return urllib.request.urlopen(req, msg_bytes)
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=url, s=e.code, r=e.read().decode('utf-8')))

    def _basic_post_request(self, msg, url):
        response = self._basic_post_request_core(msg, url)
        return response.read().decode('utf-8')

    def report_status(self, process_id,
                      state_id=None, log_file_path=None,
                      report_json=None, status_json=None,
                      start_timestamp=None, end_timestamp=None,
                      file_name=None):

        if not state_id and not log_file_path:
            return False

        msg = {'process_id': process_id,
               'state_id': state_id,
               'process_log_path': log_file_path,
               'report': report_json,
               'status': status_json,
               'data_start_timestamp': start_timestamp,
               'data_end_timestamp': end_timestamp,
               'aggregated_file_path': file_name}

        response = self._basic_post_request_core(msg, self.report_status_ws)
        return response.getcode() == HTTPStatus.OK

    def enter_new_state(self, process_id, state_id):
        msg = {'process_id': process_id,
               'state_id': state_id}
        response = self._basic_post_request_core(msg, self.report_status_ws)
        return response.getcode() == HTTPStatus.OK

    def register_siteRes_process(self, site_id, resolution):
        req_data = {'SITE_ID': site_id,
                    'resolution': resolution,
                    'code_version': self.code_version,
                    'processor': getpass.getuser()}
        return self._basic_post_request(
                req_data, self.register_base_ws).strip('"')

    def get_available_base_input(self, site_id):
        try:
            resp = urllib.request.urlopen(self.get_base_input + site_id)
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=self.get_base_input + site_id,
                s=e.code, r=e.read().decode('utf-8')))

    def register_base_files(self, proc_id, file, input_info):
        req_data = {'base_file': file}
        parts = []
        for part in input_info:
            parts.append({'file': part.name,
                          'start': part.start,
                          'end': part.end})
        req_data['upload_files'] = parts
        url = '{u}/{i}'.format(u=self.register_base_ws, i=proc_id)
        return self._basic_post_request(req_data, url).strip('"')

    def report_publish_base(self, process_id, version):
        msg = {'process_id': process_id,
               'version': version,
               'code_version': self.code_version}
        resp = self._basic_post_request_core(msg, self.publish_base_ws)
        return resp.getcode() == HTTPStatus.OK

    def get_base_info(self, site_id, process_ids):
        url = self.get_base_report_info.format(s=site_id)
        return json.loads(self._basic_post_request(process_ids, url))

    def make_file_qaqc_url(self, path):
        return os.path.join(self.file_qaqc_url_prefix, os.path.basename(path))

    def make_site_res_qaqc_url(self, path, sub_dir=None,
                               site_id=None, process_id=None):
        # DON"T like this method b/c of hardcord of directory structure
        # --- should do this better.
        temp_sub_dir = '/'.join([site_id, process_id, sub_dir])
        base_dir = ''.join([self.siteres_qaqc_url_prefix, temp_sub_dir])
        return os.path.join(base_dir, os.path.basename(path))

    def make_plot_dir_url(self, path, plot_dir,
                          process_type='BASE Generation'):
        if process_type == 'BASE Generation':
            url_path = self.siteres_qaqc_url_prefix
        elif process_type == 'File Format':
            return path
        return path.replace(plot_dir, url_path)

    def get_site_users(self, site_id):
        url = self.get_site_users_ws
        url = url.format(s=site_id)
        try:
            resp = urllib.request.urlopen(url)
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=url, s=e.code, r=e.read().decode('utf-8')))


if __name__ == "__main__":
    print('Not configured to run directly.')
