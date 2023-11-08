#!/usr/bin/env python

import argparse
import string
import numpy as np

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
            dr.read_single_file(file_path=fname, check_log=check_log,
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

    def check_invalid_values(self, fname, check_log):
        with open(fname, 'r', encoding='utf-8-sig') as f:
            # skip the header
            found_headers = False
            drop_line_count = 0
            header_set = (
                'timestamp_start', 'timestamp', 'time', 'date', 'year',
                'start_timestamp', 'doy', 'dtime', 'hrmin',
                'timestamp_end', 'end_timestamp', 'time_start', 'start_time')
            while not found_headers:
                try:
                    # if readline encounters an invalid UTF8 char it
                    # tosses the remainder of the 8k block of data
                    line = f.readline()
                except UnicodeDecodeError:
                    if first_bad_char:
                        _log.warning(
                            'found invalid UTF8 character when looking for '
                            f'headers on line {drop_line_count}, '
                            'trying cp1252')
                        drop_line_count = 0
                        f.close()
                        f = open(fname,
                                'r', encoding='cp1252')
                        first_bad_char = False
                        continue
                    else:
                        msg = ('File contains invalid UTF8 and cp1252 '
                            'characters. Unknown file encoding. '
                            'Autocorrection FAILED.')
                        self.append_status_msg_parts('fatal', msg)
                        return None, None
                if line == '':
                    # End of File
                    break
                tokens = line.rstrip().split(',')
                # note that strip and strip(string.whitespace) aren't
                # exactly the same but FPin files are supposed to be
                # ascii files so they should yield the same result
                tokens = [h.strip(string.whitespace + '"') for h in tokens]
                head = [t for t in tokens if t.lower() in header_set]
                if len(head) > 0:
                    # found header line
                    found_headers = True
                    break
                drop_line_count += 1

            headers = tokens

            ln_num = 1
            for line in f.readlines():
                tokens = self._tokenize(line.strip('\n'))
                tokens, has_quotes_removed = self._strip_quotes(tokens)
                tokens, has_whitespace_removed = self._strip_whitespace(tokens)
                if has_whitespace_removed:
                    n_lines_whitespace_removed += 1
                if has_quotes_removed:
                    n_lines_quotes_removed += 1
                invalid_data_row = \
                    self.data_util.check_invalid_data_row(data_row=tokens[0])
                if invalid_data_row:
                    # assume units line or some other junk
                    drop_line_count += 1
                    continue
                fixed_tokens = []
                for i, t in enumerate(tokens):
                    t = t.strip()
                    invalid_missing_value_format, msg = \
                        self.data_util.check_invalid_missing_value_format(t,
                                                                        check_types=[
                                                                            'common_value',
                                                                            '69_value',
                                                                            'char_value',
                                                                            'imaginary_value',
                                                                            'non_numeric_value'
                                                                        ])
                    if invalid_missing_value_format:
                        check_log.error('{msg} on line {li} '
                                        'column {header} [{t}]'.format(
                                        msg=msg,
                                        li=ln_num,
                                        header=headers[i],
                                        t=t))
                ln_num += 1

    def _tokenize(self, line):
        return line.split(',')

    def _strip_quotes(self, tokens):
        """

        :param tokens: list of variable names
        :return: list of variable names with quotes removed
                 boolean, True if quotes were removed
        """
        return self._strip_character(tokens, character='"')

    def _strip_whitespace(self, tokens):
        """

        :param tokens: list of variable names
        :return: list of variable names with whitespaces removed
                 boolean, True if whitespaces were removed
        """
        return self._strip_character(tokens, character=string.whitespace)

    def _strip_character(self, tokens, character):
        """ Takes in a line of values and remove whitespaces and quotes
        from the beginning or end of values if the characters exist.

        Returns a list of tokens and a boolean value of True if whitespace
        or quotes are removed
        """
        sum_token_len = sum((len(t) for t in tokens))

        tokens = [t.strip(character) for t in tokens]
        sum_no_character_token_len = sum((len(t) for t in tokens))

        if (sum_token_len - sum_no_character_token_len) > 0:
            return tokens, True
        return tokens, False

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

            has_invalid_data_value = False
            for i, h in enumerate(headers):
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

                # check nan value if any in the data
                if d.data.dtype[i] == '<f8' and has_invalid_data_value == False:
                    if np.any(np.isnan(d.data[h].filled(-1000))):
                        has_invalid_data_value = True
                check_log.resetStats()

            # detect nan value
            if has_invalid_data_value:
                self.check_invalid_values(
                    fname=fname, check_log=check_log)

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
