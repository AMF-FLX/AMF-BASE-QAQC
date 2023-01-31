#!/usr/bin/env python

import argparse
import collections
from configparser import ConfigParser
from db_handler import DBHandler
from future.standard_library import install_aliases
from jira_interface import JIRAInterface
from jira_names import JIRANames
import json
from logger import Logger
import os
from path_util import PathUtil
from process_states import ProcessStates
from process_actions import ProcessActions
from report_status import ReportStatus
import time
import urllib.request
from urllib.error import HTTPError
from utils import FileUploadUtil
import zipfile

install_aliases()

__author__ = 'Norm Beekwilder, Danielle Christianson'
__email__ = 'norm.beekwilder@gmail.com, dschristianson@lbl.gov'

_log = Logger(True, None, None, 'translateEarlyFPBase').getLogger(
    'translateEarlyFPBase')


class TranslateEarlyBase:
    def __init__(self):
        self.SiteInfo = collections.namedtuple(
            'SiteInfo', 'site_id process_id issue_id base_file upload_file '
            'new_process_id new_issue_id comment')
        self.site_list = []
        self.site_dict = {}
        self.var_info_map = {}
        self.var_info_date = None
        temp_dir = None
        hostname = None
        user = None
        auth = None
        site_status_db_name = None
        var_info_db_name = None
        upload_info_url = None
        upload_file_url = None
        self.rs = ReportStatus()
        self.jira = JIRAInterface()
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            cfg_section = 'VERSION'
            if config.has_section(cfg_section):
                self.code_version = config.get(cfg_section, 'code_version')
            cfg_section = 'WEBSERVICES'
            if config.has_section(cfg_section):
                self.get_filename = config.get(cfg_section, 'get_filename')
                upload_info_url = config.get(cfg_section, 'upload_part1')
                upload_file_url = config.get(cfg_section, 'upload_part2')
            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                amp_upload_email = config.get(
                    cfg_section, 'file_upload_notification_email')
            cfg_section = 'BASE'
            if config.has_section(cfg_section):
                # The temp_dir: everything but the end directory
                #               should already exist.
                temp_dir = config.get(cfg_section, 'temp_dir')
                self.base_dir = config.get(cfg_section, 'base_dir')
                self.base_query = config.get(cfg_section, 'base_query')
                self.done_query = config.get(cfg_section, 'done_query')
            cfg_section = 'DB'
            if config.has_section(cfg_section):
                hostname = config.get(cfg_section, 'flux_hostname')
                user = config.get(cfg_section, 'flux_user')
                auth = config.get(cfg_section, 'flux_auth')
                site_status_db_name = config.get(
                    cfg_section, 'flux_site_status_db_name')
                var_info_db_name = config.get(
                    cfg_section, 'flux_var_info_db_name')
        if all([hostname, user, auth, site_status_db_name, var_info_db_name]):
            self.db = DBHandler(hostname=hostname, user=user,
                                password=auth, db_name=site_status_db_name)
            self.var_info_db = DBHandler(hostname=hostname, user=user,
                                         password=auth,
                                         db_name=var_info_db_name)
        else:
            _log.error('Could not establish database connection. Failed.')
        self.temp_trans_base = PathUtil().create_valid_path(
            path=os.path.split(temp_dir)[0],
            sub_dir=os.path.split(temp_dir)[1])
        self.file_upload = FileUploadUtil(
            upload_info_url=upload_info_url, upload_file_url=upload_file_url,
            amp_upload_email=amp_upload_email)

    def get_var_info_map(self, var_info_date):
        self.var_info_map = self.var_info_db.get_var_info_map(var_info_date)

    def translate_base_file(self, in_file, out_file, site_id, resolution):
        vpd_columns = []
        vpd_headers = []
        pi_headers = []
        drop_headers = []
        drop_columns = []
        mapped_headers = []
        header_map = self.var_info_map[site_id]
        zip_archive = zipfile.ZipFile(in_file, 'r')
        zip_contents = zip_archive.namelist()
        base_file = None
        for z in zip_contents:
            if '_BASE_' in z and ('_' + resolution + '_') in z:
                base_file = z
        unzipped_base_file = zip_archive.extract(member=base_file,
                                                 path=self.temp_trans_base)
        with open(unzipped_base_file, 'r', encoding='utf8') as f_in:
            # strip lines before header
            f_in.readline()
            f_in.readline()
            header_line = f_in.readline().rstrip('\n')
            headers = header_line.split(',')
            # classify variable headers
            for i, h in enumerate(headers):
                if h.split('_')[0] == 'VPD':
                    vpd_columns.append(i)
                    vpd_headers.append(h)
                if 'TIMESTAMP' not in h and h not in header_map.keys():
                    drop_headers.append(h)
                    drop_columns.append(i)
            missing_columns = [True]*len(headers)
            missing_columns[0] = False
            missing_columns[1] = False
            data = []
            for line in f_in.readlines():
                line_tokens = line.rstrip('\n').split(',')
                # apply VPD Fix
                for i in vpd_columns:
                    if line_tokens[i] != '-9999':
                        line_tokens[i] = '{f:.2f}'.format(
                            f=float(line_tokens[i]) * 10).rstrip(
                                '0').rstrip('.')
                # track all missing values
                for i, m in enumerate(missing_columns):
                    if m and line_tokens[i] != '-9999':
                        missing_columns[i] = False
                data.append(line_tokens)
            for i, m in enumerate(missing_columns):
                if m and i not in drop_columns:
                    drop_headers.append(headers[i])
                    drop_columns.append(i)
                    _log.warning(f'Variable {headers[i]} was mapped but has '
                                 'no data. Removing variable.')
            for i, h in enumerate(headers):
                if 'TIMESTAMP' in h:
                    mapped_headers.append(h)
                    continue
                if h not in header_map.keys():
                    _log.warning(f'Variable {h} does not have a mapped FP-In '
                                 'variable. ')
                    if h not in drop_headers:
                        _log.error(
                            f'Variable {h} does not have a mapped FP-In '
                            'variable and it was not already removed. '
                            'Check out what is up. ')
                        drop_headers.append(h)
                        drop_columns.append(i)
                elif not header_map[h]:
                    if h not in drop_headers:
                        drop_headers.append(h)
                        drop_columns.append(i)
                if i not in drop_columns:
                    mapped_headers.append(header_map[h])
            drop_columns.sort(reverse=True)
            for d in drop_columns:
                del headers[d]
            for lt in data:
                for d in drop_columns:
                    del lt[d]
            for i, h in enumerate(mapped_headers):
                if '_PI' in h:
                    mapped_headers[i] = (h.split('_')[0])
                    pi_headers.append(h)
            with open(out_file, 'w', encoding='utf8') as f_out:
                f_out.write(','.join(mapped_headers) + '\n')
                for lt in data:
                    f_out.write(','.join(lt) + '\n')
            change_msg = ('BASE file mapped with VarInfoData.dbo.'
                          'TowerVarDisplay_{d}. '.format(
                              d=self.var_info_date))
            if len(vpd_headers) > 0:
                change_msg += 'Converted VPD to hPa from original variable ' \
                              '{c}. '.format(c=', '.join(vpd_headers))
            if len(pi_headers) > 0:
                change_msg += 'Removed _PI flag from {c}. '.format(
                    c=', '.join(pi_headers))
            if len(drop_headers) > 0:
                change_msg += 'Dropped all non-mapped and empty columns ' \
                              '{c}. '.format(c=', '.join(drop_headers))
            change_msg += 'Translated with AMF-QAQC code version {v}.'.format(
                v=self.code_version)
        os.remove(path=unzipped_base_file)
        zip_dir = self.get_zipfile_unzipped_dir_name(unzipped_base_file)
        if os.path.exists(os.path.join(self.temp_trans_base, zip_dir)):
            os.rmdir(path=os.path.join(self.temp_trans_base, zip_dir))
        _log.info(site_id + ': ' + change_msg)
        return change_msg

    def get_zipfile_unzipped_dir_name(self, zipfile_with_path):
        zip_file = os.path.basename(zipfile_with_path)
        zip_dir = zip_file.rstrip('.zip')
        return zip_dir

    def get_site_list(self, args):
        """
        Generate a list of sites for which to translate base;
            populate the site dictionary
        :param args: the arguments passed in the module call
        :return: none
        """
        site_list = []
        if args.sites:
            site_list = args.sites.split(',')
        elif args.defined_query == 'site_status_base':
            site_list = self.db.get_site_status_BASE()
        if args.skip_sites:
            skip_sites = args.skip_sites.split(',')
            for site in skip_sites:
                if site in site_list:
                    site_list.pop(site_list.index(site))
        if not site_list:
            _log.error('No site list. Exiting')
        else:
            _log.info('Site list is as follows: {s}'.format(
                s=', '.join(site_list)))
        return site_list

    def get_existing_site_base_issue_info(self):
        count = 0
        while True:
            results = self.jira.run_query(self.base_query, count * 50)
            for issue in results['issues']:
                if issue['fields'][JIRANames.site_id] in self.site_list:
                    filename = self.get_filename_for_process(
                        issue['fields'][JIRANames.process_ids])
                    self.site_dict[issue['fields'][JIRANames.site_id]] = \
                        self.SiteInfo(
                            site_id=issue['fields'][JIRANames.site_id],
                            process_id=issue['fields'][JIRANames.process_ids],
                            issue_id=issue['key'], base_file=None,
                            upload_file=filename, new_process_id=None,
                            new_issue_id=None, comment=None)
            if len(results['issues']) < results['maxResults']:
                break
            count += 1

    def get_csv_file_info_inside_zip(self, zip_file_name):
        zip_archive = zipfile.ZipFile(zip_file_name, 'r')
        zip_contents = zip_archive.namelist()
        site_res_combos = []
        for z in zip_contents:
            if 'csv' not in z:
                continue
            file_parts = z.split('_')
            map_key = '_'.join([str(file_parts[1]), str(file_parts[3])])
            site_res_combos.append(map_key)
        return site_res_combos

    def run_sites(self):
        file_map = {}
        root, dirs, files = next(os.walk(self.base_dir))
        for f in files:
            # Using the BASE *-1 version for the BASE file to be converted
            if '-1.zip' not in f:
                continue
            map_keys = self.get_csv_file_info_inside_zip(
                zip_file_name=os.path.join(self.base_dir, f))
            for map_key in map_keys:
                file_map[map_key] = os.path.join(root, f)
        for site_id in self.site_dict.keys():
            _log.info(f'Beginning site {site_id}.')
            name, extension = os.path.splitext(
                self.site_dict[site_id].upload_file)
            file_name = str(name.split('\\')[-1])
            file_parts = file_name.split('-')
            file_parts.pop()
            file_name = '-'.join(file_parts) + extension
            file_parts = file_name.split('_')
            file_key = file_parts[0] + '_' + file_parts[1]
            upload_file = os.path.join(self.temp_trans_base, file_name)
            comment = self.translate_base_file(
                in_file=file_map[file_key], out_file=upload_file,
                site_id=site_id, resolution=file_parts[1])
            if not comment:
                self.site_dict[site_id] = None
            else:
                self.site_dict[site_id] = self.SiteInfo(
                    site_id=site_id,
                    process_id=self.site_dict[site_id].process_id,
                    issue_id=self.site_dict[site_id].issue_id,
                    base_file=file_map[file_key], upload_file=file_name,
                    new_process_id=None, new_issue_id=None, comment=comment)
                info_msg = self.file_upload.upload(
                    file_name=file_name, file_path=self.temp_trans_base,
                    site_id=site_id, upload_comment=comment)
                _log.info(info_msg)
            os.remove(path=upload_file)

    def get_new_issues_info(self):
        count = 0
        while True:
            results = self.jira.run_query(
                self.done_query.format(d=self.var_info_date), count * 50)
            if results['total'] < len(self.site_dict):
                # not all QAQC results are ready sleep for a minute
                # and see if its ready.
                time.sleep(60)
                continue
            for issue in results['issues']:
                site_id = issue['fields'][JIRANames.site_id]
                self.site_dict[site_id] = self.SiteInfo(
                    site_id=site_id,
                    process_id=self.site_dict[site_id].process_id,
                    issue_id=self.site_dict[site_id].issue_id,
                    base_file=self.site_dict[site_id].base_file,
                    upload_file=self.site_dict[site_id].upload_file,
                    new_process_id=issue['fields'][JIRANames.process_ids],
                    new_issue_id=issue['key'],
                    comment=self.site_dict[site_id].comment)
            if len(results['issues']) < results['maxResults']:
                break
            count += 1

    def issue_linkage(self):
        for site in self.site_dict.values():
            if site.process_id and site.new_issue_id:
                self.jira.add_related_link(
                    site.new_issue_id, site.issue_id, 'Updater')
                # Cancel the old issue to assign the base update resolution
                # Since we no longer cancel issues if the file passed QAQC and
                #    it is possible for a BASE to be included in published data
                #    see next steps to get back to Attempt Data QAQC
                self.jira.set_issue_state(
                    site.issue_id, JIRANames.format_QAQC_canceled,
                    JIRANames.base_update_resolution)
                # Reopen th issue and apply the Retired label. This means
                #    that we can easily distinguishe the current version of
                #    the BASE files
                self.jira.set_issue_state(
                    site.issue_id, JIRANames.format_QAQC_reopen_issue)
                # Set the issue state back to ready for data qaqc
                self.jira.set_issue_state(
                    site.issue_id, JIRANames.format_QAQC_ready_for_data,
                    labels=['BASE', 'Retired'])
                # Move the new issue to the state ready for qaqc and apply the
                #    BASE label.
                self.jira.set_issue_state(
                    site.new_issue_id,
                    JIRANames.format_QAQC_ready_for_data, labels=['BASE'])
                self.rs.enter_new_state(
                    process_id=site.process_id,
                    status=ProcessStates.RetiredForReprocessing,
                    action=ProcessActions.RetiredForReprocessing)
                msg = '{s}: Old issue (process id = {op} status set ' \
                      'and linked to new issue (process id = {np}).'
                _log.info(msg.format(s=site.site_id, op=site.process_id,
                                     np=site.new_process_id))
            else:
                _log.warning('Problem with setting state and linking for '
                             f'site {site.site_id}')

    def get_filename_for_process(self, process_id):
        try:
            resp = urllib.request.urlopen(self.get_filename + process_id)
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=self.get_filename + process_id, s=e.code,
                r=e.read().decode('utf-8')))

    def rerun_qaqc(self, process_id):
        try:
            resp = urllib.request.urlopen(
                urllib.request.Request(self.get_filename + str(process_id),
                                       method='PATCH'))
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=self.get_filename + process_id,
                s=e.code, r=e.read().decode('utf-8')))

    def test(self):
        # proc_id = self.rerun_qaqc(5260)
        # site_dict = self.get_site_data()
        self.translate_base_file(
            in_file='D:/UVA Sharepoint Code/AmeriFlux-BASE/'
                    'AMF_BR-Sa1_BASE_HR_4-1.csv',
            out_file='foo.csv', site_id='BR-Sa1', resolution='HR')

    def main(self):
        parser = argparse.ArgumentParser(
            description='Translate early FP-In BASE (simple translation of '
                        'L2) to FP-In using VarInfo mapping. VPD units are '
                        'also corrected. NOTE: Until we can fix the '
                        'webservices, Format QAQC code on Wile needs '
                        'to be on current version b/c the webservices use '
                        'the code version on Wile for input into '
                        'qaqcProcessLog for uploaded files.')
        parser.add_argument('var_info_date', type=str,
                            help='Date of the VarInfo archive to use')
        parser.add_argument('-s', '--sites', type=str,
                            help='Text string of sites to translate separated '
                                 'by commas')
        parser.add_argument('-dq', '--defined_query', type=str,
                            help='Defined query. Options include: '
                                 '1) site_status_base: sites that have '
                                 'curateDataSource = BASE in '
                                 'ReportFluxdataBADM.dbo.AMFSiteStatusDisplay')
        parser.add_argument('-skip', '--skip_sites', type=str,
                            help='Text string of sites to skip separated '
                                 'by commas. A query or other list of sites '
                                 'must also be supplied.')
        parser.add_argument('-t', '--test_sites', type=bool, default=False,
                            help='Test the site list; '
                                 'Enter bool True or False')
        args = parser.parse_args()
        self.var_info_date = args.var_info_date
        self.get_var_info_map(var_info_date=self.var_info_date)
        self.site_list = self.get_site_list(args)
        self.get_existing_site_base_issue_info()
        if args.test_sites:
            msg = 'Site list is {sl}'.format(sl=', '.join(self.site_list))
            print(msg)
        if self.site_dict and not args.test_sites:
            self.run_sites()
            self.get_new_issues_info()
            self.issue_linkage()


if __name__ == "__main__":
    TranslateEarlyBase().main()
