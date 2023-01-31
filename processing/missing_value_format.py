#!/usr/bin/env python

import argparse
from logger import Logger
from status import StatusGenerator
from data_reader import DataReader
from messages import Messages
from utils import DataUtil

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class MissingValueFormat:

    def __init__(self):
        self.status_msg_parts = []
        self.stat_msg_prefix = ''
        self.msg = Messages()
        self.data_util = DataUtil()

    def check_missing_values_col(self, fname, header,
                                 header_index, dr, check_log):
        bad_formats = False
        ln_num = 1
        try:
            dr.read_single_file(fname=fname, check_log=check_log,
                                usemask=False, missing_values=None,
                                datatype='a25', usecols=header_index)
            original_var_names = dr.data.dtype.names
            if len(original_var_names) < 1:
                msg = 'Problem with {h} variable: no variable found'.format(
                    h=header)
                _log.error(msg)
                self.status_msg_parts.append(msg)
                return
            if original_var_names[0] not in header:
                msg = '{h} != {hdr}: if not a duplicate rename, there is ' \
                      'a problem.'.format(h=header, hdr=original_var_names[0])
                _log.error(msg)
                self.status_msg_parts.append(msg)
                return
        except ValueError as e:
            err_msg = ('Unable to read file. Error msg: {e}'.format(
                e=e.message))
            _log.fatal(err_msg)
            self.stat_msg_prefix = 'Value Error. '
        except MemoryError:
            _log.fatal('Unable to read file. Memory Error')
            self.stat_msg_prefix = 'Memory Error. '
        for b in dr.get_data()[header]:
            if len([c for c in b if c > 127]) == 0:
                t = b.decode('ascii')
                invalid_missing_value = \
                    self.data_util.check_invalid_missing_value_format(t)
                if invalid_missing_value:
                    check_log.error('Invalid missing value used on line '
                                    '{li} column {header} [{t}]'.format(
                                        li=ln_num, header=header, t=t))
                    bad_formats = True
            else:
                check_log.error('Invalid value on line {li} '
                                'column {header}'.format(
                                    li=ln_num, header=header))
            ln_num += 1
        if bad_formats:
            self.status_msg_parts.append(
                '{header} ({e_count})'.format(
                    header=header, e_count=check_log.error_count))
            _log.error('Invalid missing value formats found in '
                       '{header}'.format(header=header))

    def driver(self, d, fname):
        """

        :param fname:
        :return:
        """
        report_type = 'single_msg'
        dr = DataReader()
        if d.data is not None:
            headers = d.data.dtype.names
            # still haven't figured out how to return the length of the array
            _log.info('Data recarray has {a} variables of length {li}'
                      .format(a=len(headers), li=d.data.size))
            check_log = Logger().getLogger('missing_value_col')
            check_log.resetStats()
            for h in headers:
                # indexing by column number to deal with any
                #     renamed duplicate variables
                header_indices = [i for i, j in enumerate(headers) if j == h]
                if len(header_indices) < 1:
                    msg = 'Problem with {h} variable: no index found'.format(
                        h=h)
                    _log.error(msg)
                    self.status_msg_parts.append(msg)
                elif len(header_indices) > 1:
                    msg = 'Problem with {h} variable: multiple instances ' \
                          'found'.format(h=h)
                    _log.error(msg)
                    self.status_msg_parts.append(msg)
                else:
                    self.check_missing_values_col(
                        fname=fname, header=h, header_index=header_indices[0],
                        dr=dr, check_log=check_log)
                check_log.resetStats()
            if not self.status_msg_parts:
                status_msg = None
            else:
                status_msg = ', '.join(self.status_msg_parts)
                report_type = 'single_list'
        else:
            status_msg = self.stat_msg_prefix + 'No data. ' + \
                         self.msg.get_msg(_log.getName(), 'CRITICAL', 'Report')
            _log.fatal(status_msg)
        return StatusGenerator().status_generator(
            logger=_log, qaqc_check=self.msg.get_display_check(_log.getName()),
            status_msg=status_msg, report_type=report_type)

    def test(self, filename=None):
        """Test method to be used for testing module independently

        :param filename: File name of test file
        :type filename: str.
        """
        if not filename:
            parser = argparse.ArgumentParser(description=self.__doc__)
            parser.add_argument(
                'filename',
                type=str,
                help='Target filename')
            args = parser.parse_args()
            filename = args.filename
        return self.driver(DataReader(), filename)


if __name__ == '__main__':
    _log = Logger(True).getLogger(__name__)
    print(MissingValueFormat().test().make_report_object())
