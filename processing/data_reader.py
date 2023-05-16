#!/usr/bin/env python

import ast
import numpy as np
import string
import sys
import warnings

from collections import Counter
from configparser import ConfigParser
from copy import deepcopy
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from logger import Logger
from messages import Messages
from pathlib import Path
from status import StatusGenerator
from utils import DataUtil, TextUtil, VarUtil

_log = Logger().getLogger(__name__)
warnings.filterwarnings(action='error', category=UserWarning)


class DataReader:
    def __init__(self, test_mode=False):
        self.data = None
        self.header = None
        self.original_header = None
        self.header_as_is = None
        self.filename = None
        self.base_headers = {}
        self.var_dict = FPVariables().get_fp_vars_dict()
        self.msg = Messages()
        self.tex_util = TextUtil()
        self.data_util = DataUtil()
        self.var_util = VarUtil()
        self.test_mode = test_mode
        self.mandatory_headers = None

        config = ConfigParser()
        cwd = Path.cwd()
        try:
            with open(cwd/'qaqc.cfg') as cfg:
                config.read_file(cfg)
                cfg_section = 'MANDATORY_VARIABLES'
                if config.has_section(cfg_section):
                    self.mandatory_headers = ast.literal_eval(config.get(
                        cfg_section, 'mandatory_variables'))
        except Exception:
            raise Exception('mandatory variables not found in config file')

    def _check_timestamp_header(self, header, check_log):
        check_log.info('Checking timestamp headers.')
        err_msg = []
        for actual_header, expected_header in zip(
                header[0:2], ('TIMESTAMP_START', 'TIMESTAMP_END')):
            if actual_header.strip('"' + string.whitespace) != expected_header:
                check_log_err_msg = (
                    f'Variable line contains {actual_header} '
                    f'in expected {expected_header} column.')
                check_log.error(check_log_err_msg)
                err_msg.append(actual_header)
        err_msg = ', '.join(err_msg) if err_msg else None
        if self.test_mode:
            return err_msg
        return StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Are Timestamp variables as expected?',
            status_msg=err_msg, report_type='single_list')

    def _all_headers_have_quotes(self, header_as_is, check_log):
        check_log.info('Checking headers for quotes')
        return self.data_util.are_all_headers_with_quotes(header_as_is)

    def _check_all_headers_quotes(self, header_as_is, check_log):
        all_headers_have_quotes = self._all_headers_have_quotes(header_as_is,
                                                                check_log)
        err_msg = None
        if all_headers_have_quotes:
            err_msg = 'All variable names have quotes.'
            check_log.error(err_msg)
        if self.test_mode:
            return err_msg
        return StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Are Timestamp variables as expected?',
            status_msg=err_msg, report_type='single_msg')

    def _check_data_header(self, header_as_is, check_log):
        check_log.info('Checking data headers.')
        header = []
        bad_headers = []
        bad_columns = []
        bad_base_names = []
        bad_qualifiers = []
        headers_with_whitespace = []
        headers_with_quotes = []
        quotes_in_all_headers = self._all_headers_have_quotes(header_as_is,
                                                              check_log)
        for col_counter, h_as_is in enumerate(header_as_is):
            h = h_as_is.strip('"' + string.whitespace)
            header.append(h)
            add_bad_header = False
            if ' ' in h_as_is:
                add_bad_header = True
                headers_with_whitespace.append(h_as_is)
            if '"' in h_as_is:
                if not quotes_in_all_headers:
                    add_bad_header = True
                headers_with_quotes.append(h_as_is)
            base_header, bad_qualifier = self.get_base_header(
                h, check_log=check_log, return_bad_header=True)
            if base_header not in self.base_headers:
                self.base_headers[base_header] = []
            self.base_headers[base_header].append(h)
            if base_header not in self.var_dict.keys():
                add_bad_header = True
                bad_base_names.append(h_as_is)
            if bad_qualifier:
                add_bad_header = True
                bad_qualifiers.append(h_as_is)
            if add_bad_header:
                bad_headers.append(h_as_is)
                bad_columns.append(col_counter+1)
        for bad_header, bad_column in zip(bad_headers, bad_columns):
            if bad_header in headers_with_whitespace:
                check_log.warning('Whitespace detected in variable '
                                  f'[{bad_header}] in column {bad_column}')
            if bad_header in bad_base_names:
                check_log.warning(f'Unknown variable [{bad_header}] '
                                  f'in column {bad_column}')
            if bad_header in bad_qualifiers:
                check_log.warning('General variable qualifier seen after '
                                  'position or aggregation qualifier '
                                  f'[{bad_header}] in column {bad_column}')
            if bad_header in headers_with_quotes:
                check_log.warning('Quotes detected in variable '
                                  f'[{bad_header}] in column {bad_column}')
        if headers_with_quotes and quotes_in_all_headers:
            headers_with_quotes = ', '.join(headers_with_quotes)
            check_log.info('All headers have quotes as follows. This issue '
                           'is reported in a separate check. Headers as '
                           f'submitted were {headers_with_quotes}')
        bad_headers = ', '.join(bad_headers) if bad_headers else None
        statuses = [StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Are Data Variable names in correct format?',
            status_msg=bad_headers, report_type='single_list')]
        if self.test_mode:
            return bad_headers
        return statuses

    def _check_any_valid_header(self, check_log):
        check_log.info('Checking for any valid data header')
        found_valid_header = False
        for h in self.base_headers.keys():
            vars_with_whitespace = 0
            number_variables_with_h = len(self.base_headers[h])
            for var in self.base_headers[h]:
                if ' ' in var:
                    vars_with_whitespace += 1
            if h in self.var_dict.keys() and h != 'TIMESTAMP_START' \
                    and h != 'TIMESTAMP_END' \
                    and number_variables_with_h != vars_with_whitespace:
                found_valid_header = True
                break
        msg = None
        if not found_valid_header:
            msg = self.msg.get_msg(check_log.getName(), 'ERROR')
            # 'No valid data headers found.'
            check_log.error(msg)
        if self.test_mode:
            return found_valid_header
        return StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Are any Data Variable names FP-in?',
            status_msg=msg, report_type='single_msg')

    def _check_mandatory_data_headers(self, check_log):
        # CHECK THIS: this check needs to be repeated in the combiner
        #     and throw the error there
        status_msg = None
        if not set(self.mandatory_headers) & set(self.base_headers):
            status_msg = self.msg.get_msg(check_log.getName(), 'WARNING')
            # 'No required variable (FC, LE, H) found.'
            check_log.warning(status_msg)
        return StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Are required Data Variables present?',
            status_msg=status_msg, report_type='single_msg')

    def _check_data_header_duplicates(self, header, check_log,
                                      change_header=False):
        """
        NOTE: changing code to change names of duplicate
              headers so that other checks can proceed.
              Last revision prior to this change is 43957.
        :param header:
        :param check_log:
        :return:
        """
        duplicate_headers = ''
        header_counts = Counter(header)
        # if true we have duplicates
        if len(header_counts) != len(header):
            for h, c in header_counts.items():
                if c > 1:
                    dcounter = 1
                    d_ext = []
                    for cx in range(c):
                        if cx == 1:
                            continue
                        new_header = f'{h}_d{dcounter}'
                        header[header.index(h)] = new_header
                        d_ext.append(new_header)
                        dcounter += 1
                    plural = self.tex_util.decide_plurals([len(d_ext)])
                    d_exts = ' '.join(d_ext)
                    check_log.error('Duplicate variable name detected, '
                                    f'{h} appears {c} times; duplicates '
                                    f'renamed to {d_exts}')
                    if duplicate_headers != '':
                        duplicate_headers += '; , '
                    duplicate_headers += (f'{c-1} additional instance{plural} '
                                          f'of {h} temporarily renamed to '
                                          f'{d_exts}')
            if change_header:
                self.header = header
                check_log.info('Variables re-written to indicated duplicate '
                               'variable names.')
        if duplicate_headers == '':
            duplicate_headers = None
        else:
            duplicate_headers += '. , ' + self.msg.get_msg(
                check_log.getName(), 'ERROR', 'report_suffix')
            # These duplicated variables will fail
            #     FP-In variable name format QA/QC.'
        return StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Any duplicate Variable names?',
            status_msg=duplicate_headers, report_type='list_out')

    def check_root_qualifier_headers(self):
        """ Checks that there isn't a root header and _1_1_1 qualifier
            header for the same base variable """
        """ e.g. PA and PA_1_1_1 shouldn't be in the same file """

        log_obj = Logger().getLogger('check_root_qualifier_headers')
        duplicates = []

        for base_header in self.base_headers:
            for header in self.base_headers[base_header]:
                if base_header == header or '_1_1_1' not in header:
                    continue
                if base_header in self.base_headers[base_header]:
                    if (self.get_base_header(header, _log) == base_header):

                        log_obj.warning(
                            f'Found both root variable "{base_header}" '
                            f'and root qualifier variable "{header}"')

                        duplicates.append(f'{base_header}:{header}')

        status_msg = None
        if len(duplicates) > 0:
            status_msg = 'Found duplicate root/qualifier headers: ' + \
                ', '.join(duplicates) + '.'

        return StatusGenerator().status_generator(
            logger=log_obj,
            qaqc_check='Check for duplicate root/qualifier headers',
            status_msg=status_msg)

    def get_base_header(self,
                        header_name,
                        check_log=_log,
                        header_check=False,
                        return_bad_header=False,
                        return_qualifier_list=False):
        base_header = header_name
        header_parts = header_name.split('_')
        counter = len(header_parts)
        bad_header = None
        truly_bad_header = False
        qualifiers = []
        if counter == 1:  # No flags
            if return_bad_header:
                return base_header, bad_header
            elif return_qualifier_list:
                return base_header, qualifiers
            else:
                return base_header
        for part in reversed(header_parts):
            if part not in ('N', 'SD', 'A', 'F') and not part.isdigit():
                break
            counter -= 1
        base_header = '_'.join(header_parts[:counter])
        if base_header == 'FETCH':
            counter += 1
            base_header = '_'.join(header_parts[:counter])

        qualifier_list = header_parts[counter:]

        if not self.var_util.is_valid_qualifier(
                label='_'.join(qualifier_list)):
            base_header = header_name
            truly_bad_header = True
            if header_check:
                check_log.warning(f'Invalid qualifier found: [{header_name}]')

        if return_qualifier_list:
            return base_header, ([] if truly_bad_header else qualifier_list)
        suffix_part_count = len(header_parts) - counter
        if suffix_part_count > 1:
            agg_suffix_seen = False
            for part in qualifier_list:
                if part in ('N', 'SD', 'A') or part.isdigit():
                    agg_suffix_seen = True
                elif agg_suffix_seen:
                    # turning off this warning as default.
                    #     it is reported in _check_data_headers for the
                    #     official data check
                    if header_check:
                        check_log.warning(  # originally was error
                            'General variable qualifier seen '
                            'after position or aggregation '
                            f'qualifier [{header_name}]')
                    bad_header = header_name
                    break

        if return_bad_header:
            return base_header, (None if truly_bad_header else bad_header)
        else:
            return base_header

    def check_header_var_len(self, header, value):
        return len(header) == len(value)

    def get_filename(self):
        return self.filename

    def get_data(self):
        return self.data

    def get_filled_data(self):
        return self.data.filled(fill_value=np.nan)

    def get_base_headers(self):
        return self.base_headers

    def get_dtype(self, variable, datatype='f8'):
        # if variable.upper() in ('TIMESTAMP_START',
        #     'TIMESTAMP_END'):  # original; means some checks can't be done
        # if 'TIMESTAMP' in variable.upper():  # this allows non csv
        #     files to be read with a single column
        if variable.upper() in ('TIMESTAMP_START', 'TIMESTAMP_END'):
            return 'a25'
        else:
            return datatype

    def read_single_file(self, file_path, check_log, usemask=True,
                         missing_values='-9999',
                         datatype=None, usecols=None):
        check_log.info(f'Reading in single file with filename {self.filename}')
        statuses = []
        with open(file_path, 'r') as f:
            header_ln = f.readline()
            header = header_ln.strip('\n')
            # note that strip and strip(string.whitespace) aren't
            # exactly the same but FPin files are supposed to be
            # ascii files so they should yield the same result
            self.header_as_is = header.split(',')
            header = [h.strip('"') for h in self.header_as_is]
            self.header = [h.strip(string.whitespace) for h in header]
            self.original_header = deepcopy(self.header)
            sub_log = Logger().getLogger('duplicate_data_headers')
            sub_log.resetStats()
            statuses.append(self._check_data_header_duplicates(
                self.header, sub_log, change_header=True))
            f.seek(0)  # Reset pointer
        if usecols:
            status_msg = self.gen_data_obj(
                file_path, self.header, check_log, datatype=datatype,
                usemask=usemask, missing_values=missing_values,
                usecols=usecols)
        else:
            status_msg = self.gen_data_obj(
                file_path, self.header, check_log, datatype=datatype,
                usemask=usemask, missing_values=missing_values)
        statuses.append(StatusGenerator().status_generator(
            logger=check_log,
            qaqc_check=self.msg.get_display_check(check_log.getName()),
            # 'Any problems reading file?',
            status_msg=status_msg, report_type='single_msg'))
        return statuses

    def gen_data_obj(self, file_path, header, check_log,
                     usemask=True, missing_values='-9999',
                     datatype=None, usecols=None):
        check_log.info(f'Generating data object from filename {self.filename}')
        fatal_msg = None
        suf_fatal_msg = '{e}{grammar}'
        if not datatype:
            dtype = [(h, self.get_dtype(h)) for h in header]
        else:
            dtype = [(h, self.get_dtype(h, datatype)) for h in header]
        try:
            if usecols:
                self.data = np.genfromtxt(
                    fname=file_path, dtype=dtype, names=header, skip_header=1,
                    delimiter=',', missing_values=missing_values,
                    filling_values=np.nan, usemask=usemask, usecols=usecols)
            else:
                self.data = np.genfromtxt(
                    fname=file_path, dtype=dtype, names=header, skip_header=1,
                    delimiter=',', missing_values=missing_values,
                    filling_values=np.nan, usemask=usemask)
        except UserWarning as e:
            if 'Empty input file' in str(e):
                check_log.fatal('Empty file. Format QA/QC INCOMPLETE.')
                fatal_msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
            else:
                check_log.warning(f'Unexpected warning: {e}')
                fatal_msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
        except ValueError as e:
            check_log.fatal(suf_fatal_msg.format(e=e, grammar=': '))
            fatal_msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
        if fatal_msg:
            fatal_msg += suf_fatal_msg.format(e='', grammar='.')
        return fatal_msg

    def driver(self, file_path, run_type):
        """
        :param file_path: file path
        :param run_type: (o)riginal or (r)epaired file
        :return: list of status objects from each test
        """
        self.filename = Path(file_path).name
        _log.info(f'Data Reader checks initiated for {self.filename} '
                  'as {run_type} file.')

        # set up an empty list to receive the individual status obj
        statuses = []

        # there should be a way to build a for loop for this.
        # for now calling each test individually

        _log_test = Logger().getLogger('read_file')
        _log_test.resetStats()
        check_status = self.read_single_file(file_path, _log_test)
        statuses.extend(check_status)

        _log_test = Logger().getLogger('timestamp_headers')
        _log_test.resetStats()
        check_status = self._check_timestamp_header(self.header_as_is,
                                                    _log_test)
        statuses.append(check_status)

        _log_test = Logger().getLogger('all_headers_quotes')
        _log_test.resetStats()
        check_status = self._check_all_headers_quotes(self.header_as_is,
                                                      _log_test)
        statuses.append(check_status)

        _log_test = Logger().getLogger('data_headers')
        _log_test.resetStats()
        check_status = self._check_data_header(self.header_as_is, _log_test)
        statuses.extend(check_status)

        _log_test = Logger().getLogger('valid_data_headers')
        _log_test.resetStats()
        check_status = self._check_any_valid_header(_log_test)
        statuses.append(check_status)

        _log_test = Logger().getLogger('mandatory_data_headers')
        _log_test.resetStats()
        check_status = self._check_mandatory_data_headers(_log_test)
        statuses.append(check_status)

        status_msg = None
        if StatusGenerator()._get_best_status(logger=_log) < 0:
            status_msg = 'Unexpected warnings / errors / critical ' \
                         'in data_reader. Check log file.'

        statuses.append(StatusGenerator().status_generator(
            logger=_log, qaqc_check='data_reader',
            status_msg=status_msg, report_type='single_msg'))
        # print(len(statuses))
        return statuses


if __name__ == "__main__":
    # Testing only
    _log = Logger(True, process_type='File Format').getLogger(__name__)
    fnv = FileNameVerifier()
    fnv.driver(sys.argv[1])
    DataReader(test_mode=True).driver(sys.argv[1], sys.argv[2])
