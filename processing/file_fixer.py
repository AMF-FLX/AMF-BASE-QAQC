#!/usr/bin/env python

import argparse
import ast
import datetime as dt
import getpass
import json
import math
import os
import re
import shutil
import string
import subprocess
import urllib.request
import zipfile

from configparser import ConfigParser
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from logger import Logger
from shutil import copyfile
from status import StatusGenerator
from urllib.error import HTTPError
from utils import DataUtil, TextUtil, TimestampUtil, VarUtil
from var_fix import VarFixer
from xlrd import open_workbook

__author__ = 'Norm Beekwilder, You-Wei Cheah'
__email__ = 'norm.beekwilder@gmail.com, ycheah@lbl.gov'
_log = Logger().getLogger(__name__)


class FileFixer:
    """
    try to clean up a data file and make it FP-in
    """

    def __init__(self, test_mode=False):
        """
        Initialize variables on loading of class here
        """
        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'FILE_FIXER_CONFIG'
            if config.has_section(cfg_section):
                self.powershell_exe = config.get(cfg_section, 'powershell_exe')
                self.seven_zip_exe = config.get(cfg_section, 'seven_zip_exe')
                self.excel2csv_path = config.get(cfg_section, 'excel2csv_path')
                self.temp_base = config.get(cfg_section, 'temp_dir')
                self.fix_threshold = config.getfloat(cfg_section,
                                                     'fix_threshold')
            elif test_mode:
                self.temp_base = 'testing'
            cfg_section = 'WEBSERVICES'
            if config.has_section(cfg_section):
                self.updates_ws = config.get(cfg_section, 'upload_info')
                self.upload_ws1 = config.get(cfg_section, 'upload_part1')
                self.upload_ws2 = config.get(cfg_section, 'upload_part2')
            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                self.amp_upload_email = config.get(
                    cfg_section, 'file_upload_notification_email')
            elif test_mode:
                self.amp_upload_email = 'foo@foo.foo'
            cfg_section = 'PHASE_III'
            if config.has_section(cfg_section):
                self.PI_vars = ast.literal_eval(
                    config.get(cfg_section, 'PI_vars'))
            elif test_mode:
                config_temp = ConfigParser()
                with open(os.path.join(cwd, 'qaqc_template.cfg')) as cfg_temp:
                    config_temp.read_file(cfg_temp)
                    if config_temp.has_section(cfg_section):
                        self.PI_vars = ast.literal_eval(
                            config_temp.get(cfg_section, 'PI_vars'))
                    else:
                        self.PI_vars = ['VPD']
        self.data_util = DataUtil()
        self.ts_util = TimestampUtil()
        self.txt_util = TextUtil()
        self.var_util = VarUtil()
        self._datetime_to_isodate = self.ts_util.get_ISO_date_from_datetime
        self.status_msg_parts = {'fatal': [], 'error': [],
                                 'warning': [], 'ok': []}
        self.var_dict = FPVariables().get_fp_vars_dict()
        self.var_fixer = VarFixer()
        self.temp_dir = self.temp_base
        self.conversion_log = Logger().getLogger('file_conversion')
        self.conversion_msg = None
        self.resolution = {'half-hourly': 'HH', 'hourly': 'HR', 'bad': 'BAD'}
        self.fix_header_msgs = {'case': 'made uppercase',
                                'timestamp_synonym': 'assumed synonymous',
                                'synonym': 'found synonym',
                                'rm_pi': 'removed _PI qualifier',
                                'reorder': 're-ordered qualifiers',
                                'rm_character': '{c} removed'}
        self.current_datetime = dt.datetime.now()

    def fix_filename(self, dir_name, filename_noext, process_id, timespan,
                     corrected_data):
        """

        :param dir_name:
        :param filename_noext:
        :param process_id:
        :param timespan:
        :param corrected_data:
        :return: str, status message
        """
        filename_verifier = FileNameVerifier()
        filename_verifier_status = filename_verifier.driver(
            os.path.join(dir_name, filename_noext + '.csv'), fixer=True)
        has_opt_param = None
        filename_verifier_msg_parts = []
        filename_verifier_status_msg = \
            filename_verifier_status.get_status_msg()
        if filename_verifier_status_msg:
            if ('optional param' in filename_verifier_status_msg or
                    'underscore' in filename_verifier_status_msg):
                has_opt_param = filename_verifier.fname_attrs.get('optional')
            elif 'incorrect number of components' in \
                    filename_verifier_status_msg:
                msg = ('Filename had incorrect number of components. Fixed '
                       'filename as described below.')
                self.append_status_msg_parts('warning', msg)
        # check the site id and get it from the db if needed,
        # hope that the file wasn't uploaded as other
        if not filename_verifier.is_AMF_site_id(
                filename_verifier.fname_attrs.get('site_id', None)):
            resp = self.get_upload_info(process_id)
            if resp['SITE_ID'].strip().lower() == 'other':
                msg = ('Filename has invalid SITE_ID but was uploaded as'
                       ' other. Autocorrection FAILED.')
                self.append_status_msg_parts('error', msg)
            filename_verifier.fname_attrs['site_id'] = resp['SITE_ID'].strip()
            # self.status_msg_parts.append('file name SITE_ID')
            filename_piece = 'SITE_ID'
            _log.warning(f'Filename component {filename_piece} was fixed.')
            filename_verifier_msg_parts.append(filename_piece)
        # check the resolution and set it if needed
        fixed_res = None
        half_hourly_timedelta = dt.timedelta(minutes=30)
        hourly_timedelta = dt.timedelta(hours=1)
        if not filename_verifier.is_valid_resolution(
                filename_verifier.fname_attrs.get('resolution', None)):
            if timespan == half_hourly_timedelta:
                res = 'half-hourly'
                fixed_res = True
            elif timespan == hourly_timedelta:
                res = 'hourly'
                fixed_res = True
            else:
                _log.error('unrecognized timestamp resolution')
                res = 'bad'
                fixed_res = False
        else:
            filename_res = filename_verifier.fname_attrs['resolution']
            if (filename_res == self.resolution['half-hourly']
                    and timespan != half_hourly_timedelta
                    and timespan == hourly_timedelta):
                _log.warning('file resolution does not match timestamps')
                res = 'hourly'
                fixed_res = True
            elif (filename_res == self.resolution['hourly']
                    and timespan == half_hourly_timedelta
                    and timespan != hourly_timedelta):
                _log.warning('file resolution does not match timestamps')
                res = 'half-hourly'
                fixed_res = True
            elif (timespan != half_hourly_timedelta
                    and timespan != hourly_timedelta):
                _log.error(
                    'unrecognized timestamp resolution in file data.')
                res = 'bad'
                fixed_res = False
        if fixed_res is True:
            filename_verifier.fname_attrs['resolution'] = self.resolution[res]
            filename_piece = 'resolution'
            _log.warning(f'Filename component {filename_piece} was fixed.')
            filename_verifier_msg_parts.append(filename_piece)
        elif fixed_res is False:
            filename_verifier.fname_attrs['resolution'] = self.resolution[res]
            msg = ('Filename resolution could not be corrected. '
                   'Autocorrection FAILED.')
            self.append_status_msg_parts('fatal', msg)
        # Make sure the start time matches the first data row
        if filename_verifier.fname_attrs.get('ts_start', None) != \
                corrected_data[0][0]:
            _log.warning('file name start time does not match first'
                         ' timestamp.')
            filename_piece = 'ts-start (start time)'
            _log.warning(f'Filename component {filename_piece} was fixed.')
            filename_verifier_msg_parts.append(filename_piece)
            filename_verifier.fname_attrs['ts_start'] = corrected_data[0][0]
        # make sure the end time matches the last data row
        if filename_verifier.fname_attrs.get('ts_end', None) != \
                corrected_data[-1][1]:
            _log.warning('file name end time does not match last timestamp.')
            filename_piece = 'ts-end (end time)'
            _log.warning(f'Filename component {filename_piece} was fixed.')
            filename_verifier_msg_parts.append(filename_piece)
            filename_verifier.fname_attrs['ts_end'] = corrected_data[-1][1]
        # strip upload time so repaired file doesn't end up
        # with a double time stamp
        filename_verifier.fname_attrs.pop('ts_upload', None)
        if has_opt_param is not None:
            filename_verifier.fname_attrs.pop('optional', None)
            msg = (f'optional parameter ({has_opt_param}) removed '
                   'from filename')
            if has_opt_param == '':
                msg = 'trailing underscore removed from filename'
            _log.warning(msg)
            filename_verifier_msg_parts.append(msg)
        remade_filename = filename_verifier.make_filename()
        if filename_verifier_msg_parts:
            plural = self.txt_util.decide_plurals(
                [len(filename_verifier_msg_parts)])
            joined_msg_pieces = '; '.join(filename_verifier_msg_parts)
            msg = f'Filename component{plural} fixed: {joined_msg_pieces}'
            _log.warning(msg)
            return remade_filename, \
                filename_verifier.fname_attrs['site_id'], msg
        return remade_filename, filename_verifier.fname_attrs['site_id'], None

    def has_valid_headers(self, data_mapper, header_map):
        """

        :param data_mapper:
        :param header_map:
        :return:
        """
        h_count = 0
        bh_count = 0
        for u, s in zip(data_mapper, header_map):
            if u:
                h_count += 1
                if not s:
                    # if header is used and is not standard
                    bh_count += 1
        # if all headers are bad headers file is NS
        if h_count == bh_count:
            return False
        return True

    def duplicate_variables(self, headers):
        """
        Detect and report duplicate headers
        :param headers: list, fixed variable names
        :return: str, concatenated list of duplicate variables
        """
        duplicate_variables = []
        for col_num, header in enumerate(headers):
            header_pieces = header.split('_')
            if re.match('d[0-9]+', header_pieces[-1]):
                header_without_d_suffix = '_'.join(header_pieces[:-1])
                duplicate_variables.append(f'{header_without_d_suffix} '
                                           f'(column {col_num + 1})')
        if duplicate_variables:
            return '; '.join(duplicate_variables)
        return None

    def fix_file(self, file_path, process_id, local_run=False):
        self.temp_dir = os.path.join(self.temp_base, process_id)
        os.makedirs(self.temp_dir, exist_ok=True)
        dir_name = os.path.dirname(file_path)
        filename = os.path.basename(file_path)
        filename_noext, filename_ext = os.path.splitext(filename)
        # if its not csv see if we can convert to csv
        if filename_ext != '.csv':
            csv_generated, token = self.make_csv(dir_name, filename_noext,
                                                 filename_ext, process_id)
            if csv_generated:
                filename_ext = '.csv'
                dir_name = self.temp_dir
            elif token is not None:
                return None, token
            else:
                # conversion failed
                msg = ('File could not be converted/extracted to csv. '
                       'Autocorrection FAILED.')
                self.append_status_msg_parts('fatal', msg)
                return None, None
        first_bad_char = True
        with open(os.path.join(dir_name, filename_noext + filename_ext), 'r',
                  encoding='utf-8-sig') as f:
            found_headers = False
            drop_line_count = 0
            # try to find header line
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
                        f = open(os.path.join(dir_name,
                                              filename_noext + filename_ext),
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
                    header_tokens = self.txt_util.tokenize(line.strip('\n'))
                    all_headers_with_quotes = \
                        self.data_util.are_all_headers_with_quotes(
                            header_tokens)
                    if all_headers_with_quotes:
                        header_tokens, _ = \
                            self.txt_util.strip_quotes(header_tokens)
                        msg = 'Quotes around variable names removed.'
                        self.append_status_msg_parts('warning', msg)
                    found_headers = True
                    break
                drop_line_count += 1
            if not found_headers:
                file_path = os.path.join(dir_name,
                                         filename_noext + filename_ext)
                _log.fatal(
                    f'Unable to locate variable names in file {file_path}')
                self.status_msg_parts['fatal'].append(
                    'Unable to locate variable names. Autocorrection FAILED.')
                return None, None
            # now that we have headers see if they are valid FP-IN headers
            fixed_headers = []
            header_map = []
            # print(header_tokens)
            unfixable_headers = []
            for h in header_tokens:
                s, fh, _ = self.fix_header(h)
                # basic protection against files that have multiple
                # headers that map to TIMESTAMP_START or TIMESTAMP_END
                if (fh == 'TIMESTAMP_START' and h != fh and
                        'TIMESTAMP_START' in header_tokens):
                    s = False
                if (fh == 'TIMESTAMP_END' and h != fh and
                        'TIMESTAMP_END' in header_tokens):
                    s = False
                if s:
                    fixed_headers.append(fh)
                else:
                    fixed_headers.append(h.upper())
                    _log.warning(f'Unable to fix variable name {h}')
                    unfixable_headers.append(h)
                header_map.append(s)
            # print(unfixable_headers)
            if unfixable_headers:
                plural = self.txt_util.decide_plurals([len(unfixable_headers)])
                unfix_head = '; '.join(unfixable_headers)
                self.status_msg_parts['warning'].append(
                    f'NOTE un-fixable variable name{plural}: {unfix_head}')

            # check for duplicate headers and if so make them unique
            tally = {}
            for i, h in enumerate(fixed_headers):
                if h not in tally.keys():
                    tally[h] = []
                tally[h].append(i)
            for h, locs in tally.items():
                if len(locs) <= 1:
                    continue
                plural = self.txt_util.decide_plurals([len(locs)])
                _log.error(f'found {len(locs)} occurrence{plural} of '
                           f'variable name {h}')
                # Note: retaining renaming of duplicates so rest of fixer can
                #       identify potential fixes.
                dup_headers = []
                for i, loc in enumerate(locs):
                    if i == 0:
                        # don't rename first occurrence of the variable.
                        continue
                    dup_header = f'{h}_d{i}'
                    fixed_headers[loc] = dup_header
                    dup_headers.append(dup_header)
                _count = len(locs) - 1
                _dup_headers = '; '.join(dup_headers)
                plural = self.txt_util.decide_plurals([_count])
                _log.info(
                    f'{_count} additional instance{plural} of {h} '
                    f'temporarily renamed to {_dup_headers} for further QA/QC')
            # read in the data
            data = []
            fixed_missing_count = 0
            missing_values = {}
            n_lines_whitespace_removed = 0
            n_lines_quotes_removed = 0
            for line in f.readlines():
                tokens = self.txt_util.tokenize(line.strip('\n'))
                tokens, has_quotes_removed, _ = self.txt_util.strip_quotes(tokens)
                tokens, has_whitespace_removed, _ = \
                    self.txt_util.strip_whitespace(tokens)
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
                # fix bad missing values
                fixed_tokens = []
                for t in tokens:
                    t = t.strip()
                    invalid_missing_value_format = \
                        self.data_util.check_invalid_missing_value_format(t)
                    if invalid_missing_value_format:
                        fixed_tokens.append('-9999')
                        fixed_missing_count += 1
                        if t not in missing_values:
                            missing_values[t] = 0
                        missing_values[t] += 1
                    else:
                        fixed_tokens.append(t)
                data.append(fixed_tokens)
            if drop_line_count > 0:
                msg = f'Removed {drop_line_count} malformed lines.'
                self.append_status_msg_parts('warning', msg)
            if fixed_missing_count > 0:
                plural = self.txt_util.decide_plurals([fixed_missing_count])
                sub_msg = (
                    f'Changed {fixed_missing_count} missing value{plural} '
                    'to -9999 from')
                _log.warning(sub_msg)
                # self.status_msg_parts['warning'].append(msg)
                local_count = 1
                len_missing_values = len(missing_values)
                print(len_missing_values)
                for t, tcount in missing_values.items():
                    v = '(empty value)' if t == '' else t
                    if (len_missing_values > 1
                            and local_count < len_missing_values - 1):
                        msg_sep = ';'
                    elif (len_missing_values > 1
                            and local_count < len_missing_values):
                        if len_missing_values < 3:
                            msg_sep = ' and'
                        else:
                            msg_sep = '; and'
                    else:
                        msg_sep = '.'
                    plural = self.txt_util.decide_plurals([missing_values[t]])
                    msg = f' {tcount} instance{plural} of {v}{msg_sep}'
                    _log.warning(msg)
                    sub_msg += msg
                    local_count += 1
                self.status_msg_parts['warning'].append(sub_msg)

            if n_lines_whitespace_removed > 0:
                warn_msg = (
                    f'Whitespace in {n_lines_whitespace_removed} data '
                    'line(s) were removed.')
                _log.warning(warn_msg)
                self.status_msg_parts['warning'].append(warn_msg)

            if n_lines_quotes_removed > 0:
                warn_msg = (
                    f'Quotes around data values in {n_lines_quotes_removed} '
                    'line(s) were removed.')
                _log.warning(warn_msg)
                self.status_msg_parts['warning'].append(warn_msg)

        if len(data) == 0:
            _log.error('no data found in file')
            self.status_msg_parts['error'].append('No data found in file. '
                                                  'Autocorrection FAILED.')
            return None, None
        # check TIMESTAMPS we may want to open this up to more formats
        if len(data[0]) != len(fixed_headers):
            _log.error(
                'Number of variables does not match number of data columns. '
                'Autocorrection FAILED.')
            self.status_msg_parts['error'].append(
                'Number of variables does not match number of data columns. '
                'Autocorrection FAILED.')
            return None, None
        data_mapper = [True] * len(data[0])
        timestamp_info = None
        ts_start = -1
        self.ts_width_log = Logger().getLogger(__name__ + '-ts_width')
        if 'TIMESTAMP_START' in fixed_headers:
            ts_start = fixed_headers.index('TIMESTAMP_START')
        elif 'START_TIMESTAMP' in fixed_headers:
            # This should never show up b/c moved fix to variable fixes above
            ts_start = fixed_headers.index('START_TIMESTAMP')
            fixed_headers[ts_start] = 'TIMESTAMP_START'
        ts_end = -1
        if 'TIMESTAMP_END' in fixed_headers:
            ts_end = fixed_headers.index('TIMESTAMP_END')
        elif 'END_TIMESTAMP' in fixed_headers:
            # This should never show up b/c moved fix to variable fixes above
            ts_end = fixed_headers.index('END_TIMESTAMP')
            fixed_headers[ts_end] = 'TIMESTAMP_END'
        ts1 = ts2 = None
        # if the TIMESTAMP headers aren't right out of the box figure out
        # what they did and then get the resolution
        if ts_start < 0 or ts_end < 0:
            # try to fix timestamp issues
            if len(data) < 2:
                _log.error('insufficient data in file to correct timestamps')
                self.status_msg_parts['error'].append(
                    'Insufficient data in file to correct timestamps.'
                    ' Autocorrection FAILED.')
                return None, None
            if ts_start < 0 and ts_end < 0:
                # no standard timestamp headers found
                if 'TIMESTAMP' in fixed_headers:
                    # found a timestamp assume that it is
                    # really TIMESTAMP_START
                    ts_start = fixed_headers.index('TIMESTAMP')
                    fixed_headers[ts_start] = 'TIMESTAMP_START'
                    try:
                        ts1 = self.ts_util.cast_as_datetime(
                            self.ts_width(data[0][ts_start]))
                        ts2 = self.ts_util.cast_as_datetime(
                            self.ts_width(data[1][ts_start]))
                    except Exception:
                        self.check_scientific(data[1][ts_start])
                    if ts1 is not None and ts2 is not None:
                        timespan = ts2 - ts1
                        timestamp_info = (
                            self.ts_width.__name__, "Add", ts_start, timespan)
                        data_mapper[ts_start] = False
                        msg = (
                            'Generated timestamp variables TIMESTAMP_START'
                            ' and TIMESTAMP_END from TIMESTAMP variables'
                            ' assuming data was reporting at the beginning'
                            ' of the half hour. This automated fix is only'
                            ' available temporarily.')
                        self.append_status_msg_parts('warning', msg)
                elif all((h in fixed_headers
                          for h in ('YEAR', 'DOY', 'HRMIN'))):
                    yh, dh, hh = map(
                        fixed_headers.index, ('YEAR', 'DOY', 'HRMIN'))
                    try:
                        ts1 = self.make_timestamp(
                            data[0][yh], data[0][dh], data[0][hh])
                        ts2 = self.make_timestamp(
                            data[1][yh], data[1][dh], data[1][hh])
                    except Exception:
                        pass
                    if ts1 is not None and ts2 is not None:
                        timespan = ts2 - ts1
                        timestamp_info = (
                            self.gen_ts.__name__, yh, dh, hh, timespan)
                        data_mapper[yh] = False
                        data_mapper[dh] = False
                        data_mapper[hh] = False
                        if 'DTIME' in fixed_headers:
                            dth = fixed_headers.index('DTIME')
                            data_mapper[dth] = False
                        msg = ('Generated timestamp variables TIMESTAMP_START'
                               ' and TIMESTAMP_END from YEAR DOY HRMIN'
                               ' variables assuming data was reporting at '
                               'the beginning of the half hour. This '
                               'automated fix is only available temporarily.')
                        self.append_status_msg_parts('warning', msg)
                elif all((h in fixed_headers
                          for h in ('YEAR', 'MONTH', 'DAY', 'HOUR'))):
                    yh, mh, dh, hh = map(
                        fixed_headers.index, ('YEAR', 'MONTH', 'DAY', 'HOUR'))
                    if 'MIN' in fixed_headers:
                        mnh = fixed_headers.index('MIN')
                        try:
                            ts1 = self.make_timestamp2(
                                data[0][yh], data[0][mh], data[0][dh],
                                data[0][hh], data[0][mnh])
                            ts2 = self.make_timestamp2(
                                data[1][yh], data[1][mh], data[1][dh],
                                data[1][hh], data[1][mnh])
                        except Exception:
                            pass
                    else:
                        mnh = None
                        try:
                            ts1 = self.make_timestamp2(
                                data[0][yh], data[0][mh], data[0][dh],
                                data[0][hh], None)
                            ts2 = self.make_timestamp2(
                                data[1][yh], data[1][mh], data[1][dh],
                                data[1][hh], None)
                        except Exception:
                            pass
                    if ts1 is not None and ts2 is not None:
                        timespan = ts2 - ts1
                        timestamp_info = (
                            self.gen_ts2.__name__,
                            yh, mh, dh, hh, mnh, timespan)
                        if mnh is not None:
                            data_mapper[mnh] = False

                        data_mapper[yh] = False
                        data_mapper[mh] = False
                        data_mapper[dh] = False
                        data_mapper[hh] = False
                        if 'DTIME' in fixed_headers:
                            dth = fixed_headers.index('DTIME')
                            data_mapper[dth] = False
                        msg = (
                            'Generated timestamp variables TIMESTAMP_START'
                            ' and TIMESTAMP_END from YEAR MONTH DAY '
                            'HOUR MIN variables assuming data was reporting'
                            ' at the beginning of the half hour. This '
                            'automated fix is only available temporarily.')
                        self.append_status_msg_parts('warning', msg)
            elif ts_end < 0:
                # have timestamp_start just not end
                try:
                    ts1 = self.ts_util.cast_as_datetime(
                        self.ts_width(data[0][ts_start]))
                    ts2 = self.ts_util.cast_as_datetime(
                        self.ts_width(data[1][ts_start]))
                except Exception:
                    self.check_scientific(data[1][ts_start])
                if ts1 is not None and ts2 is not None:
                    timespan = ts2 - ts1
                    timestamp_info = (
                        self.ts_width.__name__, "Add", ts_start, timespan)
                    data_mapper[ts_start] = False
                    msg = ('Generated TIMESTAMP_END from TIMESTAMP_START '
                           'variable.')
                    self.append_status_msg_parts('warning', msg)
            elif ts_start < 0:
                # have timestamp_end just not start
                try:
                    ts1 = self.ts_util.cast_as_datetime(
                        self.ts_width(data[0][ts_end]))
                    ts2 = self.ts_util.cast_as_datetime(
                        self.ts_width(data[1][ts_end]))
                except Exception:
                    self.check_scientific(data[1][ts_end])
                if ts1 is not None and ts2 is not None:
                    timespan = ts2 - ts1
                    timestamp_info = (
                        self.ts_width.__name__, "Sub", ts_end, timespan)
                    data_mapper[ts_end] = False
                    msg = ('Generated TIMESTAMP_START from TIMESTAMP_END'
                           ' variable.')
                    self.append_status_msg_parts('warning', msg)
        else:
            if ts_start != 0 or ts_end != 1:
                msg = ('Moved TIMESTAMP_START and/or TIMESTAMP_END into '
                       'first two columns.')
                self.append_status_msg_parts('warning', msg)
            try:
                ts1 = self.ts_util.cast_as_datetime(
                    self.ts_width(data[0][ts_start]))
                ts2 = self.ts_util.cast_as_datetime(
                    self.ts_width(data[0][ts_end]))
            except Exception:
                self.check_scientific(data[0][ts_start])
            if ts1 is not None and ts2 is not None:
                timespan = ts2 - ts1
                timestamp_info = (
                    self.ts_width.__name__, None, ts_start, ts_end)
                data_mapper[ts_start] = False
                data_mapper[ts_end] = False
        if timestamp_info is None:
            if ts_start > -1:
                self.check_scientific(data[0][ts_start])
            elif ts_end > -1:
                self.check_scientific(data[0][ts_end])
            _log.fatal('unable to correct timestamps')
            self.status_msg_parts['fatal'].append(
                'Unable to correct timestamps. Autocorrection FAILED.')
            return None, None
        if (timespan != dt.timedelta(minutes=30) and
                timespan != dt.timedelta(hours=1)):
            _log.fatal('unable to repair timestamps, unrecognized resolution')
            self.status_msg_parts['fatal'].append(
                'Unable to correct timestamps, '
                f'unrecognized resolution {timespan}. Autocorrection FAILED.')
            return None, None
        # Check upfront if future filled. If so fail fixer.
        try:
            last_timestamp = self.ts_util.cast_as_datetime(
                self.ts_width(data[-1][ts_start]))
            if last_timestamp > self.current_datetime:
                _log.fatal('timestamps are forward filled')
                self.status_msg_parts['fatal'].append(
                    'Timestamps are filled into future. '
                    'Autocorrection FAILED.')
                return None, None
        except Exception:
            pass
        # iterate over the data and make sure that the TIMESTAMP columns are
        # in the correct position also drop old
        # timekeeping data
        corrected_data = []
        filled_log = Logger().getLogger(__name__ + '-filled_timestamp')
        dups_log = Logger().getLogger(__name__ + '-duplicate_timestamp')
        self.ts_width_log.resetStats()
        bad_timestamp_count = 0
        for vals in data:
            try:
                if timestamp_info[0] == self.gen_ts.__name__:
                    ts = self.gen_ts(vals, *timestamp_info[1:])
                elif timestamp_info[0] == self.gen_ts2.__name__:
                    ts = self.gen_ts2(vals, *timestamp_info[1:])
                elif timestamp_info[0] == self.ts_width.__name__:
                    opt, arg1, arg2 = timestamp_info[1:]

                    if opt is None:
                        ts_start = arg1
                        ts_end = arg2
                        ts = (self.ts_width(vals[ts_start]),
                              self.ts_width(vals[ts_end]))
                    elif opt == "Add":
                        ts_start = arg1
                        timespan = arg2
                        _ts_width = self.ts_width(vals[ts_start])
                        ts = (_ts_width,
                              self._datetime_to_isodate(
                                  self.ts_util.cast_as_datetime(_ts_width)
                                  + timespan))
                    elif opt == "Sub":
                        ts_end = arg1
                        timespan = arg2
                        _ts_width = self.ts_width(vals[ts_end])
                        ts = (self._datetime_to_isodate(
                            self.ts_util.cast_as_datetime(_ts_width)
                            - timespan), _ts_width)
                    else:
                        _log.error("Invalid clause")
                else:
                    _log.error("Invalid clause")
                row_start = self.ts_util.cast_as_datetime(ts[0])
                row_end = self.ts_util.cast_as_datetime(ts[1])
            except Exception:
                bad_timestamp_count += 1
                continue
            if (row_end is None or row_start is None or
                    (row_end - row_start) != timespan):
                bad_timestamp_count += 1
                _log.warning(f'Line and timestamp {ts[0]} has bad internal '
                             'resolution, dropping line.')
                continue
            # check again for forward filled dates
            if row_start > self.current_datetime:
                _log.fatal('timestamps are forward filled')
                self.status_msg_parts['fatal'].append(
                    'Timestamps are filled into future. '
                    'Autocorrection FAILED.')
                return None, None
            fixed_row = []
            fixed_row.extend((ts[0], ts[1]))
            for val, use in zip(vals, data_mapper):
                if use:
                    fixed_row.append(val)
            if (len(corrected_data) > 0 and
                    fixed_row[0] != corrected_data[-1][1]):
                # timestamps don't line up figure out what to do
                try:
                    data_end = self.ts_util.cast_as_datetime(
                        corrected_data[-1][1])
                except ValueError:
                    pass
                if row_start > data_end:
                    # missing rows, plug the gap with -9999
                    missing_count = math.ceil(
                        (row_start - data_end).total_seconds() /
                        timespan.total_seconds())
                    ts = data_end
                    for i in range(int(missing_count)):
                        empty_row = ['-9999'] * len(fixed_row)
                        empty_row[0] = self._datetime_to_isodate(ts)
                        ts += timespan
                        empty_row[1] = self._datetime_to_isodate(ts)
                        corrected_data.append(empty_row)
                    _t1 = self._datetime_to_isodate(data_end)
                    _t2 = empty_row[1]
                    filled_log.warning(
                        f'filled timestamp gap from {_t1} to {_t2}')
                    if ts > row_start:
                        _log.warning(
                            'after gap filling timestamps don\'t '
                            f'line up at {fixed_row[0]}, dropping line.')
                        bad_timestamp_count += 1
                        continue
                else:
                    # duplicate rows
                    dups_log.warning(
                        'duplicate row found at timestamp '
                        f'{fixed_row[0]}, dropping line')
                    bad_timestamp_count += 1
                    continue
            corrected_data.append(fixed_row)
        ts_width_wc = self.ts_width_log.warning_count
        if ts_width_wc > 0:
            plural = self.txt_util.decide_plurals([ts_width_wc])
            msg = (f'Set {ts_width_wc} timestamp{plural} to minute '
                   'resolution (YYYYMMDDHHMM is standard AmeriFlux '
                   'FP-In format).')
            self.append_status_msg_parts('warning', msg)
        wc = dups_log.warning_count
        if wc > 0:
            plural = self.txt_util.decide_plurals([wc])
            msg = f'Removed {wc} timestamp duplicate{plural}.'
            self.append_status_msg_parts('warning', msg)
        wc = filled_log.warning_count
        if wc > 0:
            plural = self.txt_util.decide_plurals([wc])
            msg = (f'Filled {wc} timestamp gap{plural} with missing '
                   'data values (-9999).')
            self.append_status_msg_parts('warning', msg)
        if bad_timestamp_count > 0:

            msg = (
                f'Unable to validate timestamps for {bad_timestamp_count} '
                'lines (including any duplicated timestamps reported above). '
                'These lines were removed. Gap-filling may have occurred to '
                'fill any resulting missing timestamps.')

            if bad_timestamp_count > len(corrected_data) * self.fix_threshold:
                fix_percent = str(int(self.fix_threshold*100))
                _log.error(f'{msg} More than {fix_percent}% '
                           'of file data removed.')
                self.status_msg_parts['error'].append(
                    f'{msg} However more than {fix_percent}% of file data '
                    'was changed and QA/QC will not be continued.')
            else:
                self.append_status_msg_parts('warning', msg)
        # now that we know the resolution and start and end times
        # make sure the file name matches what is in it.
        has_valid_headers = self.has_valid_headers(data_mapper, header_map)
        duplicate_variables = self.duplicate_variables(fixed_headers)
        if has_valid_headers and not duplicate_variables:
            remade_filename, site_id, filename_fixer_msg = self.fix_filename(
                dir_name, filename_noext, process_id, timespan, corrected_data)
            if filename_fixer_msg:
                self.status_msg_parts['warning'].append(filename_fixer_msg)
            # build output header line
            out_headers = ['TIMESTAMP_START', 'TIMESTAMP_END']
            for h, use in zip(fixed_headers, data_mapper):
                if use:
                    out_headers.append(h)
            # write out file data
            with open(os.path.join(self.temp_dir, remade_filename), 'w') as f:
                f.write(','.join(out_headers)+'\n')
                for data_line in corrected_data:
                    f.write(','.join(data_line)+'\n')
            # if there were no errors upload file for second round of
            # QAQC File Checks
            if _log.error_count == 0 and _log.fatal_count == 0:
                _log.info('No errors or fatal issues. Attempting to upload '
                          'autocorrected file.')
                if not local_run:
                    # print(local_run)
                    self.upload(remade_filename, process_id, site_id)
                msg = 'File was Autocorrected and corrected file uploaded.'
                self.append_status_msg_parts('warning', msg)
                return remade_filename, None
        if not has_valid_headers:
            msg = 'File does not have any valid AmeriFlux data variable names.'
            self.append_status_msg_parts('error', msg)
        if duplicate_variables:
            msg = f'File has duplicate variables {duplicate_variables}.'
            self.append_status_msg_parts('error', msg)
        msg = ('File had issues that could not be automatically corrected. '
               'Autocorrection FAILED.')
        self.append_status_msg_parts('error', msg)
        return None, None

    def append_status_msg_parts(self, msg_code, msg):
        """
        General append status_msg_parts list
        :param msg_code: str, msg code: fatal, error, warning, ok
        :param msg: str, the msg to add
        :param log: the logger
        """
        if msg_code == 'fatal':
            _log.fatal(msg)
        elif msg_code == 'error':
            _log.error(msg)
        elif msg_code == 'warning':
            _log.warning(msg)
        elif msg_code == 'ok':
            _log.info(msg)
        else:
            _log.fatal('Problem logging status message. Incorrect msg code.')
            return

        self.status_msg_parts[msg_code].append(msg)

    def make_csv(self, dir_name, filename_noext, filename_ext, process_id):
        file_path = os.path.join(dir_name, filename_noext + filename_ext)
        if filename_ext.lower() in ('.xlsx', '.xls'):
            self.readExcel(dir_name, filename_noext, filename_ext)
            self.status_msg_parts['warning'].append('Converted file to .csv.')
        elif any(filename_ext.lower() == ext
                 for ext in ('.txt', '.dat', '.csv', '')):
            shutil.copy(
                file_path, os.path.join(
                    self.temp_dir, filename_noext + '.csv'))
            ext_description = 'missing' if filename_ext == '' else filename_ext
            msg = f'Changed {ext_description} extension to .csv.'
            self.status_msg_parts['warning'].append(msg)
        elif filename_ext.lower() == '.zip':
            if not zipfile.is_zipfile(file_path):
                self.conversion_msg = ('File with zip extension does not '
                                       'appear to be a zip file.')
                self.conversion_log.fatal(self.conversion_msg)
                # self.status_msg_parts['fatal'].append(self.conversion_msg)
                return False, None
            zf = zipfile.ZipFile(file_path)
            file_infos = zf.infolist()
            tmp = []
            for f in file_infos:
                if f.file_size == 0:
                    _log.info(f'The zipped contents {f.filename} '
                              'was not extracted.')
                    continue
                filename_without_path = os.path.basename(f.filename)
                # the filename in file_info shows the complete path in the
                #     in the zip file. Skipping any ancillary files or
                #     subdirectories added by the operating system.
                #     The code below is likely redundant. Consider refactoring
                #     once there is time to fully test.
                if (f.filename.endswith('.DS_Store') or
                        'MACOSX' in f.filename or
                        filename_without_path.startswith('.') or
                        ('/' in f.filename and not filename_without_path)):
                    _log.info(f'The file {f.filename} was not extracted.')
                    continue
                tmp.append(f)
            file_infos = tmp
            if len(file_infos) == 0:
                self.conversion_msg = ('File with zip extension does not'
                                       ' appear to contain any files.')
                self.conversion_log.fatal(self.conversion_msg)
                # self.status_msg_parts['fatal'].append(self.conversion_msg)
                return False, None
            if len(file_infos) > 1:
                files = []
                for f in file_infos:
                    file_path = zf.extract(f, path=self.temp_dir)
                    if os.path.normpath(file_path) != \
                            os.path.normpath(os.path.join(
                                self.temp_dir, os.path.basename(file_path))):
                        copyfile(file_path, os.path.join(
                            self.temp_dir, os.path.basename(file_path)))
                    files.append(os.path.basename(file_path))
                token = self.make_new_upload(files, process_id)
                msg = ('NOTE: Zip file contains multiple files.'
                       ' Created new upload and retired zip file.')
                _log.warning(msg)
                self.status_msg_parts['warning'].append(msg)
                return False, token
            df = zf.extract(file_infos[0], path=self.temp_dir)
            zdir_name = os.path.dirname(df)
            zfilename = os.path.basename(df)
            zfilename_noext, zfilename_ext = os.path.splitext(zfilename)
            if zfilename_ext != '.csv':
                self.status_msg_parts['warning'].append(
                    f'Extracted {zfilename_ext} file from zip file.')
                return self.make_csv(
                    zdir_name, zfilename_noext, zfilename_ext, process_id)
            if zdir_name != self.temp_dir or zfilename_noext != filename_noext:
                shutil.copy(
                    os.path.join(zdir_name, zfilename_noext + zfilename_ext),
                    os.path.join(self.temp_dir, filename_noext + '.csv'))
                self.status_msg_parts['warning'].append(
                    f'NOTE: Single file {zfilename_noext + zfilename_ext} '
                    'was extracted from the Zip file.')
        elif filename_ext in ('.7z', '.rar', '.tgz', '.tar', '.gz'):
            file_path = os.path.join(dir_name, filename_noext + filename_ext)
            subprocess.call(
                [self.seven_zip_exe, 'e', '-y',
                 '-o'+self.temp_dir, file_path])
            files = []
            for f in os.listdir(self.temp_dir):
                target_file_path = os.path.join(self.temp_dir, f)
                if f.endswith('.DS_Store'):
                    continue
                if f.endswith('.tar'):
                    # .tgz files generate a .tar file + its contents
                    subprocess.call(
                        [self.seven_zip_exe, 'e', '-y', '-o' + self.temp_dir,
                         target_file_path])
                    for f in os.listdir(self.temp_dir):
                        if f.endswith('.DS_Store') or f.endswith('.tar'):
                            continue
                        if os.path.isfile(target_file_path):
                            files.append(f)
                    break
                if os.path.isfile(target_file_path):
                    files.append(f)
            if len(files) == 0:
                self.conversion_msg = (
                    f'File with {filename_ext} extension does not'
                    ' appear to contain any files.')
                self.conversion_log.fatal(self.conversion_msg)
                # self.status_msg_parts['fatal'].append(self.conversion_msg)
                return False, None
            if len(files) > 1:
                token = self.make_new_upload(files, process_id)
                msg = (f'NOTE: {filename_ext} file contains multiple files. '
                       'Created new upload and retired {e} file.')
                self.append_status_msg_parts('warning', msg)
                return False, token
            df = os.path.join(self.temp_dir, files[0])
            zdir_name = os.path.dirname(df)
            zfilename = os.path.basename(df)
            zfilename_noext, zfilename_ext = os.path.splitext(zfilename)
            if zfilename_ext.lower() != '.csv':
                self.status_msg_parts['warning'].append(
                    f'Extracted {zfilename_ext} file from '
                    f'{filename_ext} file.')
                return self.make_csv(
                    zdir_name, zfilename_noext, zfilename_ext, process_id)
            if zdir_name != self.temp_dir or zfilename_noext != filename_noext:
                shutil.copy(
                    os.path.join(zdir_name, zfilename_noext + zfilename_ext),
                    os.path.join(self.temp_dir, filename_noext + '.csv'))
                _zip_filename = zfilename_noext + zfilename_ext
                self.status_msg_parts['warning'].append(
                    f'NOTE: Single file {_zip_filename} was extracted '
                    f'from the {filename_ext} file.')
        else:
            self.conversion_msg = (
                f'Conversion from {filename_ext} to CSV not supported')
            self.conversion_log.fatal(self.conversion_msg)
            # self.status_msg_parts['fatal'].append(self.conversion_msg)
            return False, None
        return True, None

    def readExcel(self, file_path, filename, ext):
        wb = open_workbook(os.path.join(file_path, filename + ext))
        max_rows = 0
        sheet = None
        for i in range(0, wb.nsheets):
            s = wb.sheet_by_index(i)
            if s.nrows > max_rows:
                max_rows = s.nrows
                sheet = s
        with open(os.path.join(self.temp_dir, filename + '.csv'), 'w') as f:
            for r in range(sheet.nrows):
                row = sheet.row_values(r)
                f.write(','.join([str(c).rstrip('0').rstrip('.')
                                  for c in row]))
                f.write('\n')

    def make_new_upload(self, files, process_id):
        resp = self.get_upload_info(process_id)
        base_names = [os.path.basename(f) for f in files]
        return self.archive_upload(base_names, process_id, resp['SITE_ID'])

    def check_scientific(self, timestamp):
        sci_notation = self.ts_util.check_scientific_notation(ts=timestamp)
        if sci_notation:
            msg = 'Timestamps are in scientific notation and cannot be fixed'
            self.append_status_msg_parts('fatal', msg)

    def _get_full_year(self, year):
        """
        If needed, translate 2-digit year or float to 4-digit integer year
        This function is used by make_timestamp and make_timestamp2, which
           are both called from fix_file inside of a try / exception. Thus,
           ultimate exceptions are caught there (i.e., due to the None return)
        :param year: str or int
        :return: None or int
        """
        try:
            full_year = int(float(year))
        except Exception:
            _log.fatal('Year value could not interpreted')
            return None
        if 0 < full_year < 100:
            return int(dt.datetime.strptime(str(full_year), '%y').year)
        if full_year == 0:
            return 2000
        # The range of years that we might expect. Go AmeriFlux for many years!
        if 1990 <= full_year <= 2100:
            return full_year
        return None

    def make_timestamp(self, year, day, start_time):
        full_year = self._get_full_year(year)
        doy = int(float(day))
        st = float(start_time)
        if st <= 24:
            hour = int(st)
            minute = 0 if st == hour else 30
        else:
            hour = int(int(start_time) / 100)
            minute = int(start_time) % 100
        if hour == 24:
            hour = 0
            doy += 1
        return dt.datetime(
            full_year, 1, 1, hour, minute) + dt.timedelta(doy - 1)

    def make_timestamp2(self, year, month, day, hour, minute):
        full_year = self._get_full_year(year)
        month = int(float(month))
        day = int(float(day))
        dt_repr = dt.datetime(full_year, month, day)
        if minute is None:
            hour = float(hour)
            minute = (hour * 60) % 60
            hour = int(hour)
        else:
            hour = int(float(hour))
            minute = float(minute)
        if hour == 24:
            hour = 0
            dt_repr += dt.timedelta(1)
        dt_repr += dt.timedelta(hours=hour, minutes=minute)
        return dt_repr

    def ts_width(self, timestamp):
        if timestamp == '-9999':
            return timestamp
        if len(timestamp) > self.ts_util.YYYYMMDDHHMM_LEN:
            self.ts_width_log.warning(
                f'Timestamp {timestamp} has length longer than expected.')
            timestamp = timestamp[:self.ts_util.YYYYMMDDHHMM_LEN]
        if len(timestamp) < self.ts_util.YYYYMMDDHHMM_LEN:
            timestamp += '0' * (self.ts_util.YYYYMMDDHHMM_LEN - len(timestamp))
            self.ts_width_log.warning(
                f'Timestamp {timestamp} has length shorter than expected.')
        return timestamp

    def gen_ts(self, val, year, day, start_time, timespan):
        year = val[year]
        day = val[day]
        start_time = val[start_time]
        if any((v == '-9999' for v in (year, day, start_time))):
            return '-9999', '-9999'
        ts = self.make_timestamp(year, day, start_time)
        return (self._datetime_to_isodate(ts),
                self._datetime_to_isodate(ts + timespan))

    def gen_ts2(self, val, year, month, day, hour, minute, timespan):
        year = val[year]
        month = val[month]
        day = val[day]
        hour = val[hour]
        minute = val[minute] if minute is not None else None
        if any((v == '-9999' for v in (year, month, day, hour, minute))):
            return '-9999', '-9999'

        ts = self.make_timestamp2(year, month, day, hour, minute)
        return (self._datetime_to_isodate(ts),
                self._datetime_to_isodate(ts + timespan))

    def _remove_unaccepted_characters(self, var_name, orig_var_name,
                                      character):
        fix_msg = None
        fixed_var_name = False
        if character in var_name:
            if character == ' ':
                # split on whitespace by default
                var_name_pieces = var_name.split()
                character = 'whitespace'
            else:
                var_name_pieces = var_name.split(character)
                if character == '"':
                    character = 'quotes'
            var_name = ''.join(var_name_pieces)  # join list back together
            _log.warning(f'{character} removed from header [{orig_var_name}].')
            fix_msg = self.fix_header_msgs['rm_character'].format(c=character)
            fixed_var_name = True
        return var_name, fixed_var_name, fix_msg

    def fix_header(self, header_name):
        fixed_header = False
        specific_fix_msg = None
        specific_fix_msgs = []
        base_header = header_name.upper()
        if base_header != header_name:
            _log.warning(f'header {header_name} not all caps.')
            fixed_header = True
            specific_fix_msgs.append(self.fix_header_msgs['case'])
        unaccepted_characters = [' ', '"']
        for unaccepted_character in unaccepted_characters:
            base_header, header_fix, fix_msg = \
                self._remove_unaccepted_characters(base_header, header_name,
                                                   unaccepted_character)
            if fix_msg:
                specific_fix_msgs.append(fix_msg)
            if header_fix:
                fixed_header = True
        header_parts = base_header.split('_')
        counter = len(header_parts)
        if counter > 1:  # process flags
            for part in reversed(header_parts):
                if (part not in ('N', 'SD', 'A', 'F', 'PI')
                        and not part.isdigit()):
                    break
                counter -= 1
            base_header = '_'.join(header_parts[:counter])
            if base_header == 'FETCH':
                counter += 1
                base_header = '_'.join(header_parts[:counter])
        if base_header not in self.var_dict.keys():
            good, fix_head = self.var_fixer.fix_header(base_header)
            if good:
                if (base_header == 'TIMESTAMP' and
                        fix_head == 'TIMESTAMP_START'):
                    specific_fix_msgs.append(
                        self.fix_header_msgs['timestamp_synonym'])
                else:
                    specific_fix_msgs.append(self.fix_header_msgs['synonym'])
                base_header = fix_head
                _log.warning(f'header {header_name} changed to synonym.')
                fixed_header = True
        suffix_part_count = len(header_parts) - counter
        header = base_header
        if suffix_part_count > 0:
            suffix_parts = header_parts[counter:]
            if 'PI' in suffix_parts:
                if 'F' in suffix_parts or base_header in self.PI_vars:
                    suffix_parts.remove('PI')
                    _log.warning(
                        f'removed _PI qualifier from header {header_name}.')
                    specific_fix_msgs.append(self.fix_header_msgs['rm_pi'])
            ordered_suffixes = []
            for part in suffix_parts:
                if part in ('N', 'SD', 'A') or part.isdigit():
                    # if its not a general qualifier append it to
                    # the end of the qualifier list
                    ordered_suffixes.append(part)
                elif len(ordered_suffixes) < 1:
                    # the only general qualifier that we currently allow is F
                    # if it is the first qualifier, just append to the empty
                    # list.
                    ordered_suffixes.append(part)
                else:
                    # its a general qualifier so insert it at the
                    # beginning of the qualifier list. This approach works
                    # b/c we only currently accept general qualifer "F"
                    # otherwise we would need to specifiy the gen qual order.
                    ordered_suffixes.insert(0, part)
                    _log.warning(
                        f'qualifiers in header {header_name} not in '
                        'correct order.')
                    specific_fix_msgs.append(self.fix_header_msgs['reorder'])
            if len(ordered_suffixes) > 0:
                header += '_' + '_'.join(ordered_suffixes)
            if header_name != header:
                fixed_header = True
        good_header = self.var_util.is_valid_variable(header)
        if fixed_header and good_header:
            specific_fix_msg = '; '.join(specific_fix_msgs)
            self.status_msg_parts['warning'].append(
                f'Fixed invalid variable name {header_name} with '
                f'{header}: {specific_fix_msg}')
        return good_header, header, specific_fix_msg

    def get_upload_info(self, process_id):
        ws = self.updates_ws + process_id
        try:
            response = urllib.request.urlopen(ws)
            return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            response = e.read().decode('utf-8')
            raise Exception(
                f'{ws} returned status code {e.code}\n{response}')

    def upload(self, filename, process_id, site_id):
        intent_req = {'userid': getpass.getuser(),
                      'name': 'Format QAQC Pipeline',
                      'email': self.amp_upload_email,
                      'comment': f'repair candidate for {process_id}',
                      'SITE_ID': site_id,
                      'dataType': 'Half hourly data', 'dataFile': [filename]}
        token = self.upload_intent(intent_req)
        self.upload_file(token, filename)

    def archive_upload(self, files, process_id, site_id):
        intent_req = {'userid': getpass.getuser(),
                      'name': 'Format QAQC Pipeline',
                      'email': self.amp_upload_email,
                      'comment': f'Archive upload for {process_id}',
                      'SITE_ID': site_id,
                      'dataType': 'Half hourly data', 'dataFile': files}
        token = self.upload_intent(intent_req)
        for f in files:
            self.upload_file(token, f)
        return token

    def upload_intent(self, msg):
        ws = self.upload_ws1
        try:
            req = urllib.request.Request(ws)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            response = urllib.request.urlopen(req, msg_bytes)
            return response.read().decode('utf-8').strip('"')
        except HTTPError as e:
            self.status_msg_parts['error'].append(
                'Problem uploading repaired file.')
            response = e.read().decode('utf-8')
            raise Exception(
                f'{ws} returned status code {e.code}\n{response}')

    def upload_file(self, token, filename):
        ws = self.upload_ws2.format(t=token, f=filename)
        try:
            req = urllib.request.Request(ws)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            with open(os.path.join(self.temp_dir, filename), 'r') as f:
                file_bytes = f.read().encode('utf-8')
            req.add_header('Content-Length', len(file_bytes))
            urllib.request.urlopen(req, file_bytes)
            info_msg = (
                f'Replacement candidate {filename} '
                f'uploaded with token {token}')
            self.append_status_msg_parts('ok', info_msg)
        except HTTPError as e:
            self.status_msg_parts['error'].append(
                'Problem uploading repaired file.')
            response = e.read().decode('utf-8')
            raise Exception(
                f'{ws} returned status code {e.code}\n{response}')

    def driver(self, filename, process_id, local_run=False):
        """
        This is a driver to test and run QAQC Algorithm specific to this class
        :param filename: File name of test file
        :type filename: str.
        :param process_id: process id associated with file
        :type process_id: int.
        :param local_run: True the fix attempt is a local test run
        :type local_run: boolean
        """
        _log.resetStats()
        self.status_msg_parts = {'fatal': [], 'error': [],
                                 'warning': [], 'ok': []}
        _log.info("Beginning attempt to fix uplaoded file")
        fn, token = self.fix_file(filename, process_id, local_run)
        # qaqc_check = 'fix_file for {f}'.format(f=filename)
        # leave this 'AutoRepair' text b/c front end uses it to build report
        qaqc_check = 'AutoRepair Fixes and/or Error Messages'
        for s in self.status_msg_parts.keys():
            self.status_msg_parts[s] = ', '.join(self.status_msg_parts[s])
        return ([StatusGenerator().split_status_generator(
                    _log, qaqc_check, status_msgs=self.status_msg_parts,
                    report_type='sub_status_list_out'),
                StatusGenerator().status_generator(
                    logger=self.conversion_log,
                    qaqc_check='File Conversion Successful?',
                    status_msg=self.conversion_msg,
                    report_type='single_msg')],
                fn, token)

    def test(self, filename=None, process_id=None):
        """Test method to be used for testing module independently
        :param filename: File name of test file
        :type filename: str.
        :param process_id: process id associated with file
        :type process_id: int.
        """
        if not filename:
            parser = argparse.ArgumentParser(description=self.__doc__)
            parser.add_argument('filename', type=str, help="Target filename")
            parser.add_argument('proc_id', type=str, help="process id")
            args = parser.parse_args()
            filename = args.filename
            process_id = args.proc_id
        return self.driver(filename, process_id, local_run=True)


if __name__ == "__main__":
    _log = Logger(True, process_type='File Format').getLogger(__name__)
    statuses = FileFixer().test()
    print(statuses)
    print(statuses[0][0])
    print(statuses[0][1])
