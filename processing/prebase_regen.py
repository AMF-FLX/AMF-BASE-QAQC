#!/usr/bin/env python

import argparse
from configparser import ConfigParser
from db_handler import NewDBHandler, DBConfig
from logger import Logger
from pathlib import Path
from process_states import ProcessStates, ProcessStateHandler
from report_status import ReportStatus
from typing import Optional

from future.standard_library import install_aliases
install_aliases()

__author__ = "Danielle Christianson"
__email__ = "dschristianson@lbl.gov"

_log = Logger(True, None, None,
              'preBASERegen').getLogger('preBASERegen')


class PreBASERegenerator:
    def __init__(self):
        self.report_status = ReportStatus()
        self.process_states = ProcessStateHandler()
        self.incomplete_phase3_states = \
            self.get_incomplete_phase3_process_states()
        self.db_handler = NewDBHandler()
        self.conn = None

    def __del__(self):
        if self.conn:
            self.conn.close()

    @staticmethod
    def _read_config(cfg_filename='qaqc.cfg'):
        with open(Path.cwd() / cfg_filename) as cfg:
            config = ConfigParser()
            config.read_file(cfg)
            cfg_section = 'DB'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'hostname'):
                    hostname = config.get(cfg_section, 'hostname')
                if config.has_option(cfg_section, 'user'):
                    user = config.get(cfg_section, 'user')
                if config.has_option(cfg_section, 'auth'):
                    auth = config.get(cfg_section, 'auth')
                if config.has_option(cfg_section, 'db_name'):
                    db_name = config.get(cfg_section, 'db_name')
            db_config = DBConfig(hostname, user, auth, db_name)
        return db_config

    def get_incomplete_phase3_process_states(self):
        return tuple([
            self.process_states.get_process_state(ps)
            for ps in self.process_states.incomplete_phase3_states])

    @staticmethod
    def remove_any_duplicates(a_list):
        clean_list = []
        for a in a_list:
            if a not in clean_list:
                clean_list.append(a)
            else:
                _log.info(f'processing_log.log_id {a} is in list more than '
                          'once. Removing duplicate.')
        return clean_list

    def reset_states_to_preBASE(self, process_ids: Optional[list],
                                test_reset: bool = False):
        if not process_ids:
            process_ids = \
                self.db_handler.get_incomplete_phase3_process_ids(
                    conn=self.conn,
                    state_ids=self.incomplete_phase3_states)
        if not process_ids:
            _log.warning('No process IDs found to reset. Exiting.')
            return
        _log.info(f'Attempting to reset states for {len(process_ids)} '
                  f'process IDs: {process_ids}')
        # loop thru pids
        for pid in process_ids:
            _log.info(f'*** Process ID {pid}:')
            # get statuses for the pid
            qaqc_states = \
                self.db_handler.get_qaqc_state_history(
                    conn=self.conn, process_id=pid)
            # find last status before base attempt
            pre_base_state = self.find_preBASE_state(state_history=qaqc_states)
            # reset status
            if pre_base_state not in (
                    self.process_states.get_process_state(
                        ProcessStates.PassedCurator),
                    self.process_states.get_process_state(
                        ProcessStates.InitiatedPreBASERegen)):
                _log.warning(f'preBASE_state {pre_base_state} is not expected. '
                             f'Skipping process_id {pid}')
                continue
            if not test_reset:
                self.report_status.enter_new_state(
                    process_id=pid, state_id=pre_base_state)
                _log.info('Reset preBase state')

    def find_preBASE_state(self, state_history):
        for state_entry in state_history:
            # state_entry = (log_id, state_id, log_timestamp)
            if state_entry[1] in self.incomplete_phase3_states:
                continue
            else:
                _log.info(f'PreBASE state {state_entry[1]} found '
                          f'at datetime {state_entry[2]} '
                          f'(state_log.log_id = {state_entry[0]})')
                return state_entry[1]
        _log.warning('No non-incomplete phaseIII state found')
        return None

    def driver(self, input_process_ids: str, is_test: bool):
        db_config = self._read_config()
        self.conn = self.db_handler.init_db_conn(db_config)
        process_ids = None
        if input_process_ids:
            proposed_process_list = input_process_ids.split(',')
            process_ids = \
                self.remove_any_duplicates(proposed_process_list)
        self.reset_states_to_preBASE(process_ids=process_ids,
                                     test_reset=is_test)

    def main(self):
        parser = argparse.ArgumentParser(
            description='Reset state of specified process ids. If no '
                        'process ids specified, query for uncompleted publish.')
        parser.add_argument('-p', '--process_ids', type=str,
                            help='Comma separate process ids to be reset')
        parser.add_argument('-t', '--test_reset', type=bool, default=False,
                            help='Test the reset; returns the process ids to '
                                 'be reset. Enter bool True or False')
        args = parser.parse_args()
        process_ids = args.process_ids
        is_test = args.test_reset
        return self.driver(process_ids, is_test)


if __name__ == '__main__':
    PreBASERegenerator().main()
