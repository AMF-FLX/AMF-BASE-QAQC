#!/usr/bin/env python

import argparse
import ast
import os

from configparser import ConfigParser
from data_reader import DataReader
from logger import Logger
from messages import Messages
from status import StatusGenerator
from typing import Union

__author__ = 'Norm Beekwilder, You-Wei Cheah'
__email__ = 'norm.beekwilder@gmail.com, ycheah@lbl.gov'
_log = Logger().getLogger(__name__)


class GapFilled:
    """ check to see if data file has been gap filled
    """

    def __init__(self):
        """ Initialize variables on loading of class here """
        self.status_msg_parts = []
        self.status_msg_mandatory_parts = []
        self.status_msg_mandatory_nonfill_parts = []
        self.msg = Messages()
        self.mandatory_vars = None

        config = ConfigParser()
        cwd = os.getcwd()
        try:
            with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
                config.read_file(cfg)
                cfg_section = 'MANDATORY_VARIABLES'
                if config.has_section(cfg_section):
                    self.mandatory_vars = ast.literal_eval(config.get(
                        cfg_section, 'mandatory_variables'))
        except Exception:
            raise Exception('mandatory variables not found in config file')

    def gap_fill_detector(
            self, data_reader: DataReader, sub_log1: Logger, sub_log2: Logger,
            sub_log3: Logger, qaqc_mode: str = 'format') -> (
            (Union[None, str],) * 3):
        """
        set qaqc_mode to 'data' for a data qaqc run that shows an error for
            flux variables that are gap filled

        :param data_reader: data reader object containing data to process
        :param sub_log1: Logger for
        :param sub_log2: Logger for
        :param sub_log3: Logger for
        :param qaqc_mode: str, format (default) or data QA/QC mode
        """
        # Possible fix: change this to work with other
        #     forms of missing values...
        file_data = data_reader.get_data()
        for header in file_data.dtype.names:
            if 'timestamp' in header.lower():
                continue
            col_gap = 0
            for val in file_data.mask[header]:
                if val:
                    col_gap += 1
            if col_gap == 0:
                base_header, qualifiers = data_reader.get_base_header(
                    header, return_qualifier_list=True)
                if 'F' not in qualifiers:
                    log_msg = f'No gaps found in column {header}'
                    if qaqc_mode == 'format':
                        self.status_msg_parts.append(header)
                        sub_log1.warning(log_msg)
                    else:
                        if base_header in self.mandatory_vars:
                            # may need to change this to a warning?
                            self.status_msg_mandatory_parts.append(header)
                            sub_log2.error(log_msg)
                        else:
                            self.status_msg_parts.append(header)
                            sub_log1.warning(log_msg)
                elif base_header in self.mandatory_vars:
                    # look for unfilled variable
                    qualifiers.remove('F')
                    qualifiers.insert(0, base_header)
                    potential_non_filled = '_'.join(qualifiers)
                    if potential_non_filled not in file_data.dtype.names:
                        self.status_msg_mandatory_nonfill_parts.append(header)
                        sub_log3.error(f'mandatory variable {header} does not '
                                       'have a non-filled version')
                    else:
                        sub_log3.info('found non-filled version of mandatory '
                                      f'variable {header}')
            else:
                _log.info(f'found {col_gap} missing values in column {header}')
        nonfill_list = None
        if self.status_msg_mandatory_nonfill_parts:
            nonfill_list = ', '.join(self.status_msg_mandatory_nonfill_parts)
        if not self.status_msg_parts and not self.status_msg_mandatory_parts:
            _log.info('Gaps found in all of the variables.')
            return None, None, nonfill_list
        else:
            return ', '.join(self.status_msg_parts), \
                   ', '.join(self.status_msg_mandatory_parts), nonfill_list

    def driver(self, data_reader, qaqc_mode='format'):
        """ This is a driver to test and run QAQC Algorithm
            specific to this class
        """
        _log.info("Beginning to run gap_fill_detection algorithm...")
        _log.resetStats()

        if qaqc_mode == 'format':
            sub_log1 = _log
        else:
            sub_log1 = Logger().getLogger('reg_gap_fill')
        sub_log2 = Logger().getLogger('mand_gap_fill')
        sub_log3 = Logger().getLogger('mand_nonfill')
        sub_statuses = {}
        gap_list, gap_list_mandatory, nonfill_list = self.gap_fill_detector(
            data_reader, sub_log1, sub_log2, sub_log3, qaqc_mode)
        nonfill_status = StatusGenerator().status_generator(
            logger=sub_log3,
            qaqc_check=self.msg.get_display_check(sub_log3.getName()),
            status_msg=nonfill_list, report_type='single_list')
        qaqc_check = self.msg.get_display_check(_log.getName())
        # 'Any Variables suspected gap-fill?'
        status_msg = None
        if gap_list:
            status_msg = ''.join([
                self.msg.get_msg(_log.getName(), 'WARNING'), gap_list,
                self.msg.get_msg(_log.getName(), 'WARNING', 'report_suffix')])
        if qaqc_mode == 'format':
            gap_fill_status = StatusGenerator().status_generator(
                logger=sub_log1, qaqc_check=qaqc_check,
                status_msg=status_msg, report_type='single_msg')

        else:
            sub_statuses['reg_var'] = StatusGenerator().status_generator(
                logger=sub_log1, qaqc_check=qaqc_check,
                status_msg=status_msg, report_type='single_msg')
            status_msg = None
            if gap_list_mandatory:
                status_msg = self.msg.get_msg(_log.getName(), 'ERROR') + \
                             gap_list_mandatory
                # 'Required variables with no missing values: '
            sub_statuses['mand_var'] = StatusGenerator().status_generator(
                logger=sub_log2, qaqc_check=qaqc_check,
                status_msg=status_msg, report_type='single_msg')
            gap_fill_status = StatusGenerator().composite_status_generator(
                logger=_log, qaqc_check=qaqc_check, status_msg=None,
                keep_sub_status_name=True, statuses=sub_statuses,
                report_type='sub_status_row')
        return [gap_fill_status, nonfill_status]

    def test(self, filename=None):
        """Test method to be used for testing module independently
        :param filename: File name of test file
        :type filename: str.
        """
        if not filename:
            parser = argparse.ArgumentParser(description=self.__doc__)
            parser.add_argument('filename', type=str, help="Target filename")
            args = parser.parse_args()
            filename = args.filename

        d = DataReader()
        d.driver(filename)
        stat = self.driver(d)
        return stat


if __name__ == "__main__":
    _log = Logger(True).getLogger(__name__)
    print(GapFilled().test())
