#!/usr/bin/env python

import argparse
from configparser import ConfigParser
from db_handler import DBHandler
from logger import Logger
import os
from process_actions import ProcessActions
from process_states import ProcessStates
from report_status import ReportStatus

from future.standard_library import install_aliases
install_aliases()

__author__ = "Danielle Christianson"
__email__ = "dschristianson@lbl.gov"

_log = Logger(True, None, None, "preBASERegen").getLogger(
    "preBASERegen")


class PreBASERegenerator:
    def __init__(self):
        self.mode = None
        self.min_version = 0
        self.max_version = None
        self.process_id_ls = None
        self.sites_queued_for_BASE_update = []
        self.site_update_info = {}
        self.duplication_map = {}
        self.process_info = {}
        self.db_fields = None
        self._get_params_from_config()
        self.report_status = ReportStatus()
        self.process_actions = ProcessActions()
        self.process_states = ProcessStates()
        self.qaqcProcessLog_fields = ['processID', 'processType',
                                      'processDatetime', 'updateID',
                                      'SITE_ID', 'dataRes', 'processor',
                                      'processReportJson',
                                      'processStatusJson', 'processLogFile',
                                      'codeVersion', 'priorProcessID',
                                      'zipProcessID', 'baseName',
                                      'baseVersion', 'startTime', 'endTime',
                                      'retryCount']
        self.incomplete_phase3_states = (self.process_states.GeneratedBASE,
                                         self.process_states.UpdatedBASEBADM,
                                         self.process_states.BASEGenFailed,
                                         self.process_states.BADMUpdateFailed,
                                         self.process_states.BASEBADMPubFailed)

    def _read_config(self, cfg):
        db_hostname = None
        db_user = None
        db_auth = None
        db_name = None
        config = ConfigParser()
        config.read_file(cfg)

        cfg_section = 'DB'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'flux_hostname'):
                db_hostname = config.get(cfg_section, 'flux_hostname')
            if config.has_option(cfg_section, 'flux_user'):
                db_user = config.get(cfg_section, 'flux_user')
            if config.has_option(cfg_section, 'flux_auth'):
                db_auth = config.get(cfg_section, 'flux_auth')
            if config.has_option(cfg_section, 'flux_db_name'):
                db_name = config.get(cfg_section, 'flux_db_name')
        return db_hostname, db_user, db_auth, db_name

    def _get_params_from_config(self):
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            db_hostname, db_user, db_auth, db_name = self._read_config(cfg)
        if not all((db_user, db_auth, db_name)):
            _log.error("DB configurations not assigned")
            return False
        else:
            self.db_handler = DBHandler(db_hostname, db_user, db_auth, db_name)
        return True

    def get_process_info(self, args):
        query_type = 'all'
        if 'latest' in self.mode:
            query_type = 'latest'
        if args.process_id_ls:
            proposed_process_list = args.process_id_ls.split(',')
            self.process_id_ls = \
                self.remove_any_duplicates(proposed_process_list)
        self.process_info, self.db_fields = \
            self.db_handler.get_preBASE_regen_candidates(
                query_type=query_type, process_id_ls=self.process_id_ls)
        if not self.process_info:
            _log.error('No process information returned. '
                       'Ending preBASE regeneration.')
            return
        if not self.process_id_ls:
            self.process_id_ls = list(self.process_info.keys())
        pid_ls = [str(pid) for pid in self.process_id_ls]
        _log.info('Attempting to reprocess the following processIDs: {pids}'
                  .format(pids=', '.join(pid_ls)))

    def remove_any_duplicates(self, a_list):
        clean_list = []
        for a in a_list:
            if a not in clean_list:
                clean_list.append(a)
            else:
                _log.info('ProcessID {p} is in list more than once. Removing.'
                          .format(p=a))
        return clean_list

    def is_version_greater_than_min(self, version):
        ver = version.split('-')[1]
        if int(ver) > self.min_version:
            return True
        else:
            return False

    def is_version_less_than_max(self, version):
        if not self.max_version:
            return True
        ver = version.split('-')[1]
        if int(ver) < self.max_version:
            return True
        else:
            return False

    def prepare_process_info_for_insert(self, process_id, info):
        """
        organize the info for insert
        processID, processType, processDatetime, updateID
        SITE_ID, dataRes, processor, processReportJson,
        processStatusJson, processLogFile, codeVersion,
        priorProcessID, zipProcessID, baseName, baseVersion,
        startTime, endTime, retryCount
        (%d %s %s %d %s %s %s %s %s %s %s %d %d %s %s %s %s %d)
        :param info: single row from qaqcProcess table
        :param process_id: the old process id
        :return:
        """
        fields = ''
        entry = ''
        for i in info.keys():
            if i not in self.qaqcProcessLog_fields:
                continue
            if i in ('baseVersion', 'processID'):
                continue
            elif i == 'priorProcessID':
                info[i] = process_id
            elif not info[i]:
                continue
            elif i in ('processReportJson', 'processStatusJson'):
                info[i] = info[i].replace("b'", "")
                info[i] = info[i].replace("'", "")
            if fields == '':
                join_text = ''
            else:
                join_text = ', '
            fields = fields + join_text + i
            if i in ('updateID', 'priorProcessID', 'zipProcessID',
                     'retryCount'):
                info_text = str(info[i])
            else:
                info_text = "'{ix}'".format(ix=info[i])
            entry = entry + join_text + info_text
        '''
        entry = (info['processType'],  # 'BASE'
                 info['processDatetime'],
                 info['updateID'],  # None
                 info['SITE_ID'],
                 info['dataRes'],
                 info['processor'],  # fluxuser
                 info['processReportJson'],
                 info['processStatusJson'],
                 info['processLogFile'],
                 info['codeVersion'],
                 process_id,  # priorProcessID,
                 info['zipProcessID'],  # None
                 info['baseName'],
                 None,  # baseVersion: written in successful publish
                 info['startTime'],  # currently None but tbdeveloped
                 info['endTime'],  # currently None but tbdeveloped
                 info['retryCount'])  # 0
        '''
        if fields == '' or entry == '':
            return None, None
        return fields, entry

    def insert_preBASE_duplicate_qaqc_process_ids(self):
        for pid in self.process_info.keys():
            info = self.process_info[pid]
            site = info['SITE_ID']
            if site in self.sites_queued_for_BASE_update:
                _log.info('Site {s} is already queued for BASE creation. '
                          'Not duplicating processID {p}.'
                          .format(s=site, p=pid))
                self.process_id_ls.remove(pid)
                continue
            if not self.is_version_greater_than_min(info['baseVersion']):
                _log.info('ProcessID {p} ({s}) is not greater than minimum '
                          'regen version number {v}. Not duplicating it.'
                          .format(p=pid, s=site, v=self.min_version))
                self.process_id_ls.remove(pid)
                continue
            if self.max_version:
                if not self.is_version_less_than_max(info['baseVersion']):
                    _log.info('ProcessID {p} ({s}) is greater than maximum '
                              'regen version number {v}. Not duplicating it.'
                              .format(p=pid, s=site, v=self.max_version))
                    self.process_id_ls.remove(pid)
                    continue
            fields, entry = self.prepare_process_info_for_insert(
                process_id=pid, info=info)
            _log.info('Sending the following info for regen of {p} ({s}): {f}'
                      .format(p=pid, s=site, f=fields))
            self.db_handler.insert_qaqc_process_entry(fields=fields,
                                                      entry=entry)

    def set_regen_statuses(self):
        for pid in self.duplication_map.keys():
            _log.info('Setting regen status for {p}'.format(p=pid))
            self.report_status.enter_new_state(
                process_id=int(self.duplication_map[pid]),
                status=self.process_states.InitiatedPreBASERegen,
                action=self.process_actions.InitiatedPreBASERegen)

    def duplicate_qaqc_data_file_in_base(self):
        new_data_entry_ls = []
        for pid in self.process_id_ls:
            _log.info('Setting filesInBase info for regen ProcessID {n} '
                      '(for last published ProcessID {p}).'
                      .format(n=self.duplication_map[pid], p=pid))
            # get entries in fileInBase
            file_id_list, file_in_base_ls, old_file_process_id_dict = \
                self.db_handler.get_qaqc_file_in_base(
                    process_id=pid, new_process_id=self.duplication_map[pid])
            # write new entries in fileInBase
            self.db_handler.insert_qaqc_file_in_base_entries(
                entry_ls=file_in_base_ls)
            # read new entries back to get new fileIDs
            # get the reprocessed ids
            _, _, new_file_process_id_dict = \
                self.db_handler.get_qaqc_file_in_base(
                    process_id=self.duplication_map[pid])
            file_id_list = [str(fx) for fx in file_id_list]
            file_ids = ', '.join(file_id_list)
            # get the older dataInBase entries
            old_data_in_base_ls = \
                self.db_handler.get_qaqc_data_in_base(file_ids=file_ids)
            # build the tuple for insert
            for fid in old_file_process_id_dict.keys():
                _log.info('----> Setting dataInBase info for fileID {f}.'
                          .format(f=fid))
                if fid not in new_file_process_id_dict.keys():
                    _log.warning('No matching processIDFile = {f} for '
                                 'new regen processID = {n}.'
                                 .format(n=self.duplication_map[pid],
                                         f=fid))
                    continue
                for i, dataid in enumerate(old_file_process_id_dict[fid]):
                    new_data_entry_ls.append((new_file_process_id_dict[fid][i],
                                              old_data_in_base_ls[dataid][1],
                                              old_data_in_base_ls[dataid][2]))
        self.db_handler.insert_qaqc_data_in_base_entries(
            entry_ls=new_data_entry_ls)

    def reset_states_to_preBASE(self, process_id_ls, query, query_key,
                                test_reset=False):
        # get list of processIDs to loop thru
        if query and not process_id_ls:
            process_id_ls = self.db_handler.get_qaqc_process_ids(
                query=query, key=query_key)
        elif not process_id_ls:
            process_id_ls = \
                self.db_handler.get_incomplete_phase3_process_ids()
        if not process_id_ls:
            _log.warning('No process IDs found to reset. Exiting.')
            return
        _log.info('Attempting to reset states for {n} process IDs: {p}'
                  .format(n=len(process_id_ls), p=process_id_ls))
        # loop thru pids
        for pid in process_id_ls:
            _log.info('*** Process ID {p}:'.format(p=pid))
            # get statuses for the pid
            qaqc_status = \
                self.db_handler.get_qaqc_status_history(process_id=pid)
            # find last status before base attempt
            preBASE_state, preBASE_action = \
                self.find_preBASE_status(status_history=qaqc_status)
            # reset status
            if preBASE_state not in (
                    self.process_states.PassedCurator,
                    self.process_states.InitiatedPreBASERegen):
                _log.warning('preBASE_state {s} is not expected. Skipping'
                             .format(s=preBASE_state))
                continue
            if not test_reset:
                self.report_status.enter_new_state(process_id=pid,
                                                   status=preBASE_state,
                                                   action=preBASE_action)
                _log.info('Reset preBase state and action')

    def find_preBASE_status(self, status_history):
        for state_entry in status_history:
            # state_entry = (stateID, state, action, stateDateTime)
            if state_entry[1] in self.incomplete_phase3_states:
                continue
            else:
                _log.info('PreBASE state {s} and action {a} found '
                          'at datetime {dt} (stateID = {sid})'
                          .format(s=state_entry[1], a=state_entry[2],
                                  dt=state_entry[3], sid=state_entry[0]))
                return state_entry[1], state_entry[2]
        _log.warning('No non-incomplete phaseIII state found')
        return None, None

    def driver(self, args):
        self.mode = args.mode
        if 'duplicate' in self.mode:
            # get process info to regen
            self.get_process_info(args)
            if not self.process_info:
                return
            self.site_update_info, self.sites_queued_for_BASE_update = \
                self.db_handler.get_BASE_candidates_for_preBASE_regen()
            if self.site_update_info:
                sites_excluded = []
                for i in self.site_update_info.keys():
                    sites_excluded.append((self.site_update_info[i]['SITE_ID'],
                                           i,
                                           self.site_update_info[i]['status']))
                _log.info('Duplication will exclude the following (site, '
                          'processID, current state) {u}:'
                          .format(u=sites_excluded))
            else:
                _log.info('No sites with updates found.')
            if args.min_version:
                self.min_version = args.min_version
                _log.info('Duplication will include major versions greater '
                          'than {v} as specified in argument'
                          .format(v=self.min_version))
            if args.max_version:
                self.max_version = args.max_version
                _log.info('Duplication will include major versions less '
                          'than {v} as specified in argument'
                          .format(v=self.max_version))
            self.insert_preBASE_duplicate_qaqc_process_ids()
            # get process id map
            self.duplication_map = \
                self.db_handler.get_preBASE_duplicated_process_ids(
                    process_id_ls=self.process_id_ls)
            _log.info('The duplication map is as follows: {m}'
                      .format(m=self.duplication_map))
            # set state in qaqcState
            self.set_regen_statuses()
            # insert new rows into dataInBase and filesInBase
            self.duplicate_qaqc_data_file_in_base()
        elif 'reset' in self.mode:
            pids_ls = None
            if args.process_id_ls:
                proposed_process_list = args.process_id_ls.split(',')
                pids_ls = \
                    self.remove_any_duplicates(proposed_process_list)
            self.reset_states_to_preBASE(process_id_ls=pids_ls,
                                         query=args.reset_query,
                                         query_key=args.query_key,
                                         test_reset=args.test_reset)
        else:
            _log.warning('Mode argument is not valid. Ending regen.')

    def main(self):
        parser = argparse.ArgumentParser(description='Regenerate Phase III: '
                                                     'duplicate or reset')
        parser.add_argument('mode', type=str,
                            help='1) duplicate: duplicate already published '
                                 'Data QAQC runs; '
                                 '2) duplicate-latest: duplicate the latest '
                                 'published Data QAQC runs'
                                 '3) reset: reset unpublished Data QAQC runs')
        parser.add_argument('-vm', '--min_version', type=int,
                            help='Version threshold: all greater versions '
                                 'are duplicated')
        parser.add_argument('-vx', '--max_version', type=int,
                            help='Version threshold: versions earlier'
                                 'are duplicated')
        parser.add_argument('-p', '--process_id_ls', type=str,
                            help='Comma separate process ids to be '
                                 'regenerated')
        parser.add_argument('-rq', '--reset_query',
                            type=str, default=None,
                            help='A special case reset SQL query '
                                 'that returns qaqc process IDs only')
        parser.add_argument('-qk', '--query_key',
                            type=str, default='processID',
                            help='the field name for the special case reset '
                                 'SQL query that contains the processID')
        parser.add_argument('-t', '--test_reset', type=bool, default=False,
                            help='Test the reset; returns the process ids to '
                                 'be reset. Enter bool True or False')
        '''
        # potential functionality to add
        parser.add_argument('-s', '--site_ls', type=str,
                            help='Comma separated sites; '
                                 'version or version threshold is required')
        parser.add_argument('-v', '--version', type=int,
                            help='Single version number to regenerated')
        parser.add_argument('-p', '--process_id_ls', type=str,
                            help='Comma separate process ids to be '
                                 'regenerated')
        '''
        args = parser.parse_args()
        return self.driver(args)


if __name__ == '__main__':
    PreBASERegenerator().main()
