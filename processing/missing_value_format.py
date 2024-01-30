#!/usr/bin/env python

import argparse

from logger import Logger
from status import StatusGenerator
from data_reader import DataReader
from messages import Messages
from utils import DataUtil, TextUtil

__author__ = 'Danielle Christianson, Sy-Toan Ngo'
__email__ = 'dschristianson@lbl.gov, sytoanngo@lbl.gov'
_log_missing_value = Logger().getLogger('missing_value_format')
_log_invalid_error_value = Logger().getLogger('invalid_error_value_format')
_log_invalid_warning_value = Logger().getLogger('invalid_warning_value_format')


class MissingValueFormat:

    def __init__(self):
        self.status_msg_parts = []
        self.stat_msg_prefix = ''
        self.msg = Messages()
        self.data_util = DataUtil()
        self.txt_util = TextUtil()

    def check_missing_values_col(self, fname, header,
                                 header_index, dr, check_log):
        bad_formats = False
        ln_num = 1
        try:
            dr.read_single_file(file_path=fname, check_log=check_log,
                                usemask=False, missing_values=None,
                                datatype='a25', usecols=header_index)
            original_var_names = dr.data.dtype.names
            if len(original_var_names) < 1:
                msg = 'Problem with {h} variable: no variable found'.format(
                    h=header)
                _log_missing_value.error(msg)
                self.status_msg_parts.append(msg)
                return
            if original_var_names[0] not in header:
                msg = '{h} != {hdr}: if not a duplicate rename, there is ' \
                      'a problem.'.format(h=header, hdr=original_var_names[0])
                _log_missing_value.error(msg)
                self.status_msg_parts.append(msg)
                return
        except ValueError as e:
            err_msg = ('Unable to read file. Error msg: {e}'.format(
                e=e.message))
            _log_missing_value.fatal(err_msg)
            self.stat_msg_prefix = 'Value Error. '
        except MemoryError:
            _log_missing_value.fatal('Unable to read file. Memory Error')
            self.stat_msg_prefix = 'Memory Error. '
        for b in dr.get_data()[header]:
            if len([c for c in b if c > 127]) == 0:
                t = b.decode('ascii')
                invalid_missing_value, _ = \
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
            _log_missing_value.error('Invalid missing value formats found in '
                                     '{header}'.format(header=header))

    # get the data, not header
    def get_data(self, fname):
        with open(fname, 'r', encoding='utf-8-sig') as f:
            # skip the header
            f.readline()
            data = f.readlines()
        return data

    def has_invalid_values(self, data):
        s = ''.join(data).replace('\n', ',').lower()
        invalid_char = ('i', '!', 'e', '+')
        if set(s).intersection(invalid_char):
            return True
        return False

    def check_invalid_values(self, headers, data, check_log):
        count_error = {}
        count_warning = {}
        for ln_num, line in enumerate(data):
            line = line.lower()
            tokens = self.txt_util.tokenize(line.strip('\n'))
            tokens, _, quotes_removed_idx = \
                self.txt_util.strip_quotes(tokens)
            tokens, _, whitespace_removed_idx = \
                self.txt_util.strip_whitespace(tokens)
            token_check_types = ['imaginary_value',
                                 'factorial_value',
                                 'scientific_value']
            for i, t in enumerate(tokens):
                t = t.strip()
                is_invalid_missing_value_format, msg = \
                    (self
                     .data_util
                     .check_invalid_missing_value_format(t,
                                                         token_check_types))
                if is_invalid_missing_value_format:
                    count_error.setdefault(msg, {}).setdefault(headers[i], 0)
                    count_error[msg][headers[i]] += 1
                    check_log.error('{msg} on line {li} '
                                    'column {header} [{t}]'
                                    .format(msg=msg,
                                            li=ln_num+1,
                                            header=headers[i],
                                            t=t))
                    if count_error[msg][headers[i]] == 1:
                        _log_invalid_error_value.error(
                            'Invalid error value formats found in '
                            '{header}'.format(header=headers[i]))
                if i in whitespace_removed_idx:
                    msg = 'Whitespace'
                    count_warning.setdefault(msg, {}).setdefault(headers[i], 0)
                    count_warning[msg][headers[i]] += 1
                    check_log.warn('{msg} on line {li} '
                                   'column {header} [{t}]'
                                   .format(msg=msg,
                                           li=ln_num+1,
                                           header=headers[i],
                                           t=t))
                    if count_warning[msg][headers[i]] == 1:
                        _log_invalid_warning_value.warn(
                            'Invalid warning value formats found in '
                            '{header}'.format(header=headers[i]))
                if i in quotes_removed_idx:
                    msg = 'Quotes'
                    count_warning.setdefault(msg, {}).setdefault(headers[i], 0)
                    count_warning[msg][headers[i]] += 1
                    check_log.warn('{msg} on line {li} '
                                   'column {header} [{t}]'
                                   .format(msg=msg,
                                           li=ln_num+1,
                                           header=headers[i],
                                           t=t))
                    if count_warning[msg][headers[i]] == 1:
                        _log_invalid_warning_value.warn(
                            'Invalid warning value formats found in '
                            '{header}'.format(header=headers[i]))
        invalid_error_msg = None
        invalid_warning_msg = None
        if count_error:
            invalid_error_parts = []
            msg_parts = []
            for error_name, value in count_error.items():
                msg_parts = [f'{header} ({count})'
                             for header, count in value.items()]
                msg_parts_str = ', '.join(msg_parts)
                invalid_error_parts.append(f'{error_name}: {msg_parts_str}')
            invalid_error_msg = ', '.join(invalid_error_parts)
        if count_warning:
            invalid_warning_parts = []
            msg_parts = []
            for warning_name, value in count_warning.items():
                msg_parts = [f'{header} ({count})'
                             for header, count in value.items()]
                msg_parts_str = ', '.join(msg_parts)
                invalid_warning_parts.append(f'{warning_name}: '
                                             f'{msg_parts_str}')
            invalid_warning_msg = ', '.join(invalid_warning_parts)
        return invalid_error_msg, invalid_warning_msg

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
            _log_missing_value.info('Data recarray has {a} '
                                    'variables of length {li}'
                                    .format(a=len(headers),
                                            li=d.data.size))
            check_log = Logger().getLogger('missing_value_col')
            check_log.resetStats()

            for i, h in enumerate(headers):
                # indexing by column number to deal with any
                #     renamed duplicate variables
                header_indices = [i for i, j in enumerate(headers) if j == h]
                if len(header_indices) < 1:
                    msg = 'Problem with {h} variable: no index found'.format(
                        h=h)
                    _log_missing_value.error(msg)
                    self.status_msg_parts.append(msg)
                elif len(header_indices) > 1:
                    msg = 'Problem with {h} variable: multiple instances ' \
                          'found'.format(h=h)
                    _log_missing_value.error(msg)
                    self.status_msg_parts.append(msg)
                else:
                    self.check_missing_values_col(
                        fname=fname, header=h, header_index=header_indices[0],
                        dr=dr, check_log=check_log)
                check_log.resetStats()

            check_log = Logger().getLogger('missing_value_col')
            check_log.resetStats()
            data = self.get_data(fname=fname)
            has_invalid_values = self.has_invalid_values(data=data)
            # detect nan value
            if has_invalid_values:
                invalid_error_msg, invalid_warning_msg = \
                    self.check_invalid_values(headers=headers,
                                              data=data,
                                              check_log=check_log)
                check_log.resetStats()
            invalid_error_report_type = 'single_msg'
            invalid_warning_report_type = 'single_msg'
            if invalid_error_msg:
                invalid_error_report_type = 'single_list'
            if invalid_warning_msg:
                invalid_warning_report_type = 'single_list'
            if not self.status_msg_parts:
                status_msg = None
            else:
                status_msg = ', '.join(self.status_msg_parts)
                report_type = 'single_list'
        else:
            status_msg = self.stat_msg_prefix + 'No data. ' + \
                         self.msg.get_msg(_log_missing_value.getName(),
                                          'CRITICAL',
                                          'Report')
            _log_missing_value.fatal(status_msg)
        return [StatusGenerator().status_generator(
            logger=_log_missing_value,
            qaqc_check=self.msg.get_display_check(
                _log_missing_value.getName()),
            status_msg=status_msg,
            report_type=report_type),
                StatusGenerator().status_generator(
            logger=_log_invalid_error_value,
            qaqc_check=self.msg.get_display_check(
                _log_invalid_error_value.getName()),
            status_msg=invalid_error_msg,
            report_type=invalid_error_report_type),
                StatusGenerator().status_generator(
            logger=_log_invalid_warning_value,
            qaqc_check=self.msg.get_display_check(
                _log_invalid_warning_value.getName()),
            status_msg=invalid_warning_msg,
            report_type=invalid_warning_report_type)]

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
