import ast
import datetime
import getpass
import hashlib
import json
import matplotlib.mlab as mlab
import matplotlib.dates as dates
import numpy as np
import os
import re
import subprocess
import urllib.request
import zipfile
from collections import Counter
from configparser import ConfigParser
from fp_vars import FPVariables
from http import HTTPStatus
from logger import Logger
from sys import platform as _platform
from urllib.error import HTTPError

__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)

"""Common functions that can be shared between files"""


class TimestampUtilException(Exception):
    def __init__(self, expr, msg=None):
        super(TimestampUtilException, self).__init__(expr, msg)
        self.expr = expr
        self.msg = msg
        _log.fatal(expr)


class TimestampUtil:
    def __init__(self):
        self.PREFERRED_TS_FORMAT = '%Y%m%d%H%M'
        self.DATE_ONLY_TS_FORMAT = '%Y%m%d'
        self.JIRA_TS_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
        self.NUMBER_OF_HOURS_IN_DAY = 24
        self.MAX_NUMBER_OF_DAYS_IN_YEAR = 366
        self.YYYY_LEN = 4
        self.YYYYMM_LEN = 6
        self.YYYYMMDD_LEN = 8
        self.YYYYMMDDHH_LEN = 10
        self.YYYYMMDDHHMM_LEN = 12
        self.YYYYMMDDHHMMSS_LEN = 14
        self.ISO_TS_LEN = self.YYYYMMDDHHMM_LEN
        self.valid_ts_len = (
            self.YYYY_LEN, self.YYYYMM_LEN, self.YYYYMMDD_LEN,
            self.YYYYMMDDHH_LEN, self.YYYYMMDDHHMM_LEN,
            self.YYYYMMDDHHMMSS_LEN)

    def get_ISO_str_timestamp(self, ts):
        """Get ISO equivalent string for a timestamp that
        can consists of different formats. This allows any string
        to be used with cast_as_datetime. If input format is
        invalid or timestamp is invalid, raise Exception.
        :param ts: timestamp string
        :type ts: str

        :rtype: str.
        :return: Return a ISO equivalent timestamp string
        """
        if len(ts) not in self.valid_ts_len:
            fatal_msg = f'Invalid timestamp format {ts}'
            raise TimestampUtilException(fatal_msg)

        if len(ts) == self.YYYY_LEN:
            ts = ts + '01'
        if len(ts) == self.YYYYMM_LEN:
            ts = ts + '01'
        if len(ts) > self.YYYYMM_LEN:
            ts = ts + ('0' * (self.ISO_TS_LEN - len(ts)))
        if len(ts) == self.YYYYMMDDHHMMSS_LEN:
            ts = ts[:-2]

        if not self.cast_as_datetime(ts):
            fatal_msg = f'Invalid input timestamp {ts}'
            raise TimestampUtilException(fatal_msg)
        return ts

    def cast_as_datetime(self, value):
        """Try to convert each timestamp into a datetime
        :param value: raw timestamp value from CSV
        :type value: str.

        :rtype: datetime.
        :return: Return a datetime object
        """
        datetime_value = None
        strptime = datetime.datetime.strptime
        try:
            datetime_value = strptime(value, self.PREFERRED_TS_FORMAT)
        except Exception:
            pass
        if datetime_value:
            return datetime_value

        try:
            datetime_value = strptime(
                value.decode('ascii'), self.PREFERRED_TS_FORMAT)
        except Exception:
            fatal_msg = (f'Fail to cast perceived timestamp {value} '
                         'as datetime with decoding.')
            _log.fatal(fatal_msg)
        return datetime_value

    def get_ISO_date_from_datetime(self, dt, format=None):
        if format is None:
            return dt.strftime(self.PREFERRED_TS_FORMAT)
        elif format == self.DATE_ONLY_TS_FORMAT:
            return dt.strftime(self.DATE_ONLY_TS_FORMAT)
        else:
            _log.error(
                'Unrecognized format passed as arg '
                f'get_ISO_date_from_datetime {format}')

    def timestamp_str_to_num(self, ts):
        dt = self.cast_as_datetime(ts)
        dt_num = dates.date2num(dt)
        return dt_num

    @staticmethod
    def check_scientific_notation(ts):
        """
        Assess whether value is in scientific notation
        :param: value = a string
        :return: logical: True if sci notation, False if not
        """
        # the following regex will match a string
        # starting with a digit followed by a decimal point
        # followed by one or more digits followed by E+ followed
        # by one or more digits followed by the end of the string.
        if re.match(r'^\d\.\d+E\+\d+$', ts) is not None:
            return True
        else:
            return False


class Decode:
    def byte_to_str(self, byte):
        """Converts a bytestring to utf-8 str"""
        return byte.decode('UTF-8')


class FileUtil:
    MD5_BLOCK_SIZE = 2 ** 16

    def get_md5(self, filename, block_size=MD5_BLOCK_SIZE):
        """
        Computes md5sum in chunks to reduce memory use

        :param filename: path to file
        :type filename: str
        :param block_size: block size to use for computing each md5 iteration
        :type block_size: int
        """
        md5sum = hashlib.md5()
        with open(filename, 'rb') as f:
            block = f.read(block_size)
            while block:
                md5sum.update(block)
                block = f.read(block_size)
        return md5sum.hexdigest()


class FilenameUtils:
    def remove_upload_timestamp(self, filename):
        base_list = filename.rsplit('-')
        ext_list = base_list[-1].rsplit('.')
        return '.'.join(['-'.join(base_list[:-1]), ext_list[-1]])


class RemoteSSHUtil:
    def __init__(self, log):
        self._log = log
        self._key = self._user = self._host = None

        config = ConfigParser()
        cwd = os.getcwd()

        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'PHASE_III'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'db_flux_processing_key'):
                    self._key = config.get(
                        cfg_section, 'db_flux_processing_key')
                if config.has_option(cfg_section, 'db_flux_processing_user'):
                    self._user = config.get(
                        cfg_section, 'db_flux_processing_user')
                if config.has_option(cfg_section, 'db_flux_processing_host'):
                    self._host = config.get(
                        cfg_section, 'db_flux_processing_host')

        if not all((self._key, self._user, self._host)):
            err_msg = ('Remote SSH configurations '
                       'is not specified in config file')
            self._log.error(err_msg)

    def update_base_badm(self, opt):
        is_success = None
        update_args = ['ssh', '-i', self._key]
        remote_args = ''.join((
            self._user, '@', self._host))
        update_args.extend((remote_args, opt))
        try:
            self._log.info(f'Running update_base_badm with argument: {opt}')
            run = subprocess.run(
                update_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            no_newline_stdout = run.stdout.strip()
            no_newline_stderr = run.stderr.strip()

            if run.returncode == 0:
                self._log.debug('Success code received')
                self._log.debug(f'Stdout msg: {no_newline_stdout}')
                self._log.debug(f'Stderr msg: {no_newline_stderr}')
                return True

            is_success = False
            self._log.error('update_base_badm failed with non-zero status '
                            f'{run.returncode}')
            self._log.error(f'Stdout has msg: {no_newline_stdout}')
            self._log.error(f'Stderr has msg: {no_newline_stderr}')

        except Exception as e:
            is_success = False
            self._log.error('Error triggering BADM updates.')
            self._log.info(f'{e}')
        return is_success


class TextUtil:
    def decide_plurals(self, counts, e=''):
        plural = ''
        for p in counts:
            if p > 1:
                plural = f'{e}s'
        return plural


class WSUtil:
    def __init__(self, log):
        self._log = log

    def get_content(self, endpoint):
        status, content = None, None
        try:
            with urllib.request.urlopen(endpoint) as c:
                status = c.code
                content = c.read().decode('utf-8')
        except HTTPError as e:
            status = e.code
            fail_reason = e.reason
        finally:
            if status != HTTPStatus.OK:
                status_msg = (
                    f'{endpoint} returned status code: '
                    f'{status}\n{fail_reason}')
                self._log.fatal(status_msg)
                raise Exception(status_msg)
        return content


class ZipUtil:
    ZIP_EXT = '.zip'

    def zip_file(self, filename, zip_filename=None, zip_option='w'):
        """
        Creates zip file compressing file (if name from filename
        is not given, filename is used as zip_filename with extension
        changed to .zip). If zip file exists, can append to existing
        zip file using zip_option 'a'.

        Returns zip file name
        :param filename: path to file to be compressed
        :type filename: str
        :param zip_filename: filename for resulting zip file
        :type zip_filename: str
        :param zip_option: eithe 'w' for (over)write, or 'a' for append
        :type zip_option: str
        :rtype: str
        """

        if not zip_filename:
            name, _ = os.path.splitext(filename)
            zip_filename = name + ZipUtil.ZIP_UTIL

        try:
            with zipfile.ZipFile(zip_filename, zip_option,
                                 zipfile.ZIP_DEFLATED) as z:
                msg = (f'Compressing \'{filename}\' into \'{zip_filename}\' '
                       f'using zip option {zip_option}')
                _log.debug(msg)
                z.write(filename, os.path.basename(filename))
        except Exception as e:
            msg = f'Error in zip_file: {e}'
            _log.error(msg)
        return zip_filename


class VarUtilException(Exception):
    def __init__(self, expr, msg=None):
        super(VarUtilException, self).__init__(expr, msg)
        self.expr = expr
        self.msg = msg
        _log.error(expr)


class VarUtil:
    def __init__(self):
        # N.B.: _PI, _IU, _QC qualifiers are currently considered invalid,
        #       even though listed in variables specs web page
        self.GENERAL_VARIABLE_PATTERNS = [
            '^{VAR}$',
            '^{VAR}_[0-9]+_[0-9]+_[0-9]+$',
            '^{VAR}_[0-9]+_[0-9]+_A$',
            '^{VAR}_[0-9]+_[0-9]+_A_SD$',
            '^{VAR}_[0-9]+_[0-9]+_A_N$',
            '^{VAR}_[0-9]+$',
            '^{VAR}_[0-9]+_SD$',
            '^{VAR}_[0-9]+_N$',

            '^{VAR}_F$',
            '^{VAR}_F_[0-9]+_[0-9]+_[0-9]+$',
            '^{VAR}_F_[0-9]+_[0-9]+_A$',
            '^{VAR}_F_[0-9]+_[0-9]+_A_SD$',
            '^{VAR}_F_[0-9]+_[0-9]+_A_N$',
            '^{VAR}_F_[0-9]+$',
            '^{VAR}_F_[0-9]+_SD$',
            '^{VAR}_F_[0-9]+_N$',
        ]
        self.GENERAL_QUALIFIER_PATTERNS = [
            '^[0-9]+_[0-9]+_[0-9]+$',
            '^[0-9]+_[0-9]+_A$',
            '^[0-9]+_[0-9]+_A_SD$',
            '^[0-9]+_[0-9]+_A_N$',
            '^[0-9]+$',
            '^[0-9]+_SD$',
            '^[0-9]+_N$',

            '^F$',
            '^F_[0-9]+_[0-9]+_[0-9]+$',
            '^F_[0-9]+_[0-9]+_A$',
            '^F_[0-9]+_[0-9]+_A_SD$',
            '^F_[0-9]+_[0-9]+_A_N$',
            '^F_[0-9]+$',
            '^F_[0-9]+_SD$',
            '^F_[0-9]+_N$',

            '^_[0-9]+_[0-9]+_[0-9]+$',
            '^_[0-9]+_[0-9]+_A$',
            '^_[0-9]+_[0-9]+_A_SD$',
            '^_[0-9]+_[0-9]+_A_N$',
            '^_[0-9]+$',
            '^_[0-9]+_SD$',
            '^_[0-9]+_N$',

            '^_F$',
            '^_F_[0-9]+_[0-9]+_[0-9]+$',
            '^_F_[0-9]+_[0-9]+_A$',
            '^_F_[0-9]+_[0-9]+_A_SD$',
            '^_F_[0-9]+_[0-9]+_A_N$',
            '^_F_[0-9]+$',
            '^_F_[0-9]+_SD$',
            '^_F_[0-9]+_N$',
        ]

        self.timestamp_variables = ['TIMESTAMP_START', 'TIMESTAMP_END']
        self.var_dict = FPVariables().get_fp_vars_dict()

        # configure variable patterns to be matched as valid
        self.variable_patterns = []
        for genpattern in self.GENERAL_VARIABLE_PATTERNS:
            for var in self.var_dict.keys():
                if not var.startswith('TIMESTAMP'):
                    self.variable_patterns.append(genpattern.format(VAR=var))
        variable_pattern_text = ')|('.join(self.variable_patterns)
        combined_variable_patterns = f'({variable_pattern_text})'
        self.compiled_variable_patterns = re.compile(
            combined_variable_patterns)

        # configure variable patterns to be matched as valid
        qualifier_patterns_text = ')|('.join(self.GENERAL_QUALIFIER_PATTERNS)
        combined_qualifier_patterns = f'({qualifier_patterns_text})'
        self.compiled_qualifier_patterns = re.compile(
            combined_qualifier_patterns)

        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'PHASE_III'
            if config.has_section(cfg_section):
                self.PI_vars = ast.literal_eval(
                    config.get(cfg_section, 'PI_vars'))
            else:
                self.PI_vars = []
                critical_msg = 'Cannot find data PI variables from config.'
                _log.critical(critical_msg)

    def _var_split(self, var, log=_log):
        var_no_qualifiers = None
        q1 = q2 = q3 = None
        try:
            var_parts = var.split('_')
            q1, q2, q3 = var_parts[-3:]
            var_no_qualifiers = '_'.join(var_parts[:-3])
        except Exception:
            log.error(f'Unable to split var {var}')
            var_no_qualifiers = None
            q1 = q2 = q3 = None
        return var_no_qualifiers, q1, q2, q3

    def is_valid_variable(self, label):
        '''
        Returns True if matches RE patterns for valid variables,
        False otherwise

        :param label: full variable label
        :type label: str
        '''
        if label in self.timestamp_variables:
            return True
        return (True if self.compiled_variable_patterns.match(label)
                else False)

    def is_valid_qualifier(self, label):
        '''
        Returns True if matches RE patterns for valid variables,
        False otherwise

        :param label: full variable label
        :type label: str
        '''
        if label == '':
            return True
        return (True if self.compiled_qualifier_patterns.match(label)
                else False)

    def parse_h_v_A(self, var, log=_log):
        h = v = a = None
        var_no_h_v_a = None
        try:
            var_no_qualifiers, h, v, a = self._var_split(var, log)
            h = int(h)
            v = int(v)
            assert a == 'A'
            var_no_h_v_a = var_no_qualifiers
        except Exception:
            err_msg = (f'Unable to parse one of {h}, {v} as int '
                       f'or {a} as A for variable {var}, skipping')
            log.error(err_msg)
            var_no_h_v_a = None
            h = v = a = None
        return var_no_h_v_a, h, v, a

    def parse_h_v_r(self, var, log=_log):
        h = v = r = None
        var_no_h_v_r = None
        try:
            var_no_qualifiers, h, v, r = self._var_split(var, log)
            h = int(h)
            v = int(v)
            r = int(r)
            var_no_h_v_r = var_no_qualifiers
        except Exception:
            err_msg = (f'Unable to parse one of {h}, {v}, {r} as int for '
                       f'variable {var}, skipping')
            log.warning(err_msg)
            var_no_h_v_r = None
            h = v = r = None
        return var_no_h_v_r, h, v, r

    def get_nearest_lower_level_variables(
            self, var_ls, lo_bound, qualifier='v', include_filled_vars=False,
            keep_horiz_layer_var_if_h=False, var_preference=None, log=_log):
        """
        Get nearest lower level variables
        :param var_ls:
        :param lo_bound:
        :param qualifier:
        :param include_filled_vars:
        :param keep_horiz_layer_var_if_h: add horizontal layer variable to the
               output list if qualifier == "h"
        :param var_preference:
        :param log:
        :return: list of variables
        """
        lowest_v_vars = []
        lowest_current_v = None
        horizontal_layer_vars = []
        for var in var_ls:
            var_is_filled = '_F_' in var or var.endswith('_F')
            if not include_filled_vars and var_is_filled:
                continue
            if self.is_var_with_horiz_layer_aggregation(var):
                if qualifier == 'h':
                    horizontal_layer_vars.append(var)
                    continue
                elif qualifier == 'v':
                    v = var.split('_')[-1]
                    v = int(v)
            elif var.endswith('_A'):
                if qualifier == 'v':
                    _, _, v, _ = self.parse_h_v_A(var)
                elif qualifier == 'h':
                    _, v, _, _ = self.parse_h_v_A(var)
            else:
                if qualifier == 'v':
                    _, _, v, _ = self.parse_h_v_r(var)
                elif qualifier == 'h':
                    _, v, _, _ = self.parse_h_v_r(var)
                else:
                    v = None
            # This is for addressing the case where
            # TA is representative of TA_1_1_1
            base_var = self.remove_specific_qualifier(var=var, qualifier='F')
            if v is None:
                if not self.is_var_with_aggregate_qualifiers(var) \
                   and not self.is_var_with_pos_qualifiers(var) \
                   and not self.is_var_with_horiz_layer_aggregation(var) \
                   and self.var_dict.get(base_var) is not None:
                    v = 1
                else:
                    continue
            if v <= lo_bound:
                continue
            if lowest_current_v is None:
                lowest_current_v = v
                lowest_v_vars.append(var)
            elif v < lowest_current_v:
                lowest_current_v = v
                lowest_v_vars = [var]
            elif v == lowest_current_v:
                lowest_v_vars.append(var)
        if keep_horiz_layer_var_if_h and horizontal_layer_vars:
            lowest_v_vars.extend(horizontal_layer_vars)
        if var_preference:
            if 'gap' in var_preference:
                rm_which = 'non-filled'
            else:
                rm_which = 'gap-filled'
            lowest_v_vars = self.remove_dup_filled_nonfilled_var(
                var_list=lowest_v_vars, rm_which=rm_which)
        return lowest_v_vars

    def get_lowest_horiz_variables(self, var_ls, include_filled_vars=False,
                                   var_preference=None,
                                   keep_horiz_layer_var_if_h=False,
                                   log=_log):
        """Get top level as in v is highest"""
        return self.get_nearest_lower_level_variables(
            var_ls=var_ls, lo_bound=float('-inf'), qualifier='h',
            include_filled_vars=include_filled_vars,
            var_preference=var_preference,
            keep_horiz_layer_var_if_h=keep_horiz_layer_var_if_h,
            log=log)

    def get_top_level_variables(self, var_ls, include_filled_vars=False,
                                var_preference=None, log=_log):
        """Get top level as in v is highest"""
        return self.get_nearest_lower_level_variables(
            var_ls=var_ls, lo_bound=float('-inf'), qualifier='v',
            include_filled_vars=include_filled_vars,
            var_preference=var_preference, log=log)

    def get_lowest_r_variable(self, top_level_var_ls,
                              include_filled_vars=False,
                              var_preference=None, log=_log):
        lowest_r_var = lowest_r = None
        for var in top_level_var_ls:
            var_is_filled = '_F_' in var or var.endswith('_F')
            if not include_filled_vars and var_is_filled:
                continue
            try:
                _, _, _, r = self.parse_h_v_r(var)
                h_layer_agg = self.is_var_with_horiz_layer_aggregation(var)
                base_var = self.remove_specific_qualifier(var=var,
                                                          qualifier='F')
                if r is None:
                    if not self.is_var_with_aggregate_qualifiers(var) \
                            and not self.is_var_with_pos_qualifiers(var) \
                            and not h_layer_agg \
                            and self.var_dict.get(base_var) is not None:
                        r = 1
                    else:
                        continue
                if lowest_r is None:
                    lowest_r_var = var
                    lowest_r = r
                else:
                    if r < lowest_r:
                        lowest_r_var = var
                        lowest_r = r
                    elif r == lowest_r:
                        if not var_preference:
                            err_msg = ('We have seen the same r value twice. '
                                       f'Vars: {var} vs {lowest_r_var}.')
                            raise VarUtilException(err_msg)
                        elif 'non' in var_preference and var == base_var:
                            lowest_r_var = var
                        elif 'gap' in var_preference and var != base_var:
                            lowest_r_var = var
                        else:
                            err_msg = (
                                'We have seen the same r value twice. Vars: '
                                f'{var} vs {lowest_r_var}. var_preference '
                                'argument is not set properly')
                            raise VarUtilException(err_msg)
                    else:
                        continue
            except Exception:
                log.error(f'{var} can not be compared')
                lowest_r_var = None
        return lowest_r_var

    def h_v_r_idx_resolver(self, var, use_h, use_v, use_r, log=_log):
        if not any([use_h, use_v, use_r]):
            log.error('Cannot group var based on profile because of '
                      'insufficient criteria. Values are '
                      f'use_h:{use_h}, use_v:{use_v}, use_r:{use_r}')
            return

        _, h, v, r = self.parse_h_v_r(var)
        idx = []
        if use_h:
            idx.append(str(h))
        if use_v:
            idx.append(str(v))
        if use_r:
            idx.append(str(r))

        if len(idx) < 1:
            return
        elif len(idx) > 1:
            idx = '_'.join(idx)
        else:
            idx = idx.pop()
        return idx

    def fill_base_var_with_idx(self, var_ls, base_var):
        """Appends _1_1_1 to BASE variable in a list of variables.
        Returns the BASE variable name if succeeds, otherwise False"""

        for idx, var in enumerate(var_ls):
            if var == base_var:
                base_var_with_idx = self.gen_base_var_with_idx(base_var)
                var_ls.remove(var)
                var_ls.insert(idx, base_var_with_idx)
                return base_var
        return False

    def strip_base_vars_with_idx(self, var_ls, base_var1, base_var2):
        """ Strips off BASE variables with _1_1_1 and returns the original
        name if it was previously replaced."""

        if base_var1:
            base_var1_mapping = self.gen_base_var_with_idx(base_var1)
        if base_var2:
            base_var2_mapping = self.gen_base_var_with_idx(base_var2)

        for idx, var in enumerate(var_ls):
            if base_var1 and var == base_var1_mapping:
                var_ls.remove(var)
                var_ls.insert(idx, base_var1)
            elif base_var2 and var == base_var2_mapping:
                var_ls.remove(var)
                var_ls.insert(idx, base_var2)

    def gen_base_var_with_idx(self, base_var):
        """ Returns BASE variable with _1_1_1 appended when appropriate"""
        if self.is_var_with_general_qualifiers(base_var) \
           or self.is_var_with_pos_qualifiers(base_var) \
           or self.is_var_with_aggregate_qualifiers(base_var):
            return base_var
        else:
            return f'{base_var}_1_1_1'

    def _group_profile_var(
            self, var_ls, use_h=False, use_v=False, use_r=False, log=_log):
        results = {}
        for var in var_ls:
            idx = self.h_v_r_idx_resolver(var, use_h, use_v, use_r, log)
            if idx is None:
                log.error(f'Error getting idx: {idx}')
                continue
            cache = results.get(idx, None)
            if cache:
                cache.append(var)
            else:
                results[idx] = [var]
        return results

    def group_h_profile_var(self, var_ls, log=_log):
        return self._group_profile_var(var_ls, use_h=True, log=_log)

    def group_h_v_profile_var(self, var_ls, log=_log):
        return self._group_profile_var(
            var_ls, use_h=True, use_v=True, log=_log)

    def is_var_with_pos_qualifiers(self, var):
        if all(v is None for v in self.parse_h_v_r(var)):
            return False
        return True

    def is_var_with_aggregate_qualifiers(self, var):
        if all(v is None for v in self.parse_h_v_A(var)):
            return False
        return True

    def is_var_with_general_qualifiers(self, var):
        """ Filter out variables with _F, _PI """
        v_elements = var.split('_')
        if any(q in v_elements for q in ('F', 'PI')):
            return True
        return False

    def is_var_with_gapfilled_qualifier(self, var):
        """ Filter out variables with _F """
        v_elements = var.split('_')
        if any(q in v_elements for q in 'F'):
            return True
        return False

    def is_var_with_horiz_layer_aggregation(self, var):
        """ Determine if variable is horizontal aggregation"""
        v_elements = var.split('_')
        if 'FETCH' in var:
            base_var = '_'.join(v_elements[0:2])
            v_elements = v_elements[2:]
            v_elements.insert(0, base_var)
        number_digits = 0
        for v in v_elements:
            if v.isdigit():
                number_digits += 1
        has_a = False
        if any(vi == 'A' for vi in v_elements):
            has_a = True
        if 0 < number_digits < 2 and not has_a:
            return True
        else:
            return False

    def remove_specific_qualifier(self, var, qualifier):
        v_elements = var.split('_')
        if any(q in v_elements for q in qualifier):
            v_elements.remove(qualifier)
        updated_var = '_'.join(v_elements)
        return updated_var

    def return_which(self, opt1, opt2, return_opt1):
        """

        :param opt1: variable
        :param opt2: variable
        :param return_opt1: logical
        :return:
        """
        if return_opt1:
            return opt1
        else:
            return opt2

    def remove_dup_filled_nonfilled_var(self, var_list, rm_which='gap-filled'):
        """
        remove the specified type of variables if both are present
        :param var_list: list of variables
        :param rm_which: which variable to remove: non-filled or gap-filled
        :return: list with specified variables removed
        """
        if rm_which == 'gap-filled':
            return_filled = False
        else:
            return_filled = True
        filled_vars = []
        nonfilled_vars = []
        for v in var_list:
            if self.is_var_with_gapfilled_qualifier(v):
                filled_vars.append(v)
            else:
                nonfilled_vars.append(v)
        filled_vars_F_removed = [self.remove_specific_qualifier(v, 'F')
                                 for v in filled_vars]
        add_list = []
        if return_filled:
            for v in nonfilled_vars:
                if v not in filled_vars_F_removed:
                    add_list.append(v)
            filled_vars.extend(add_list)
            return filled_vars
        else:  # return nonfilled
            for i, v in enumerate(filled_vars_F_removed):
                if v not in nonfilled_vars:
                    add_list.append(filled_vars[i])
            nonfilled_vars.extend(add_list)
            return nonfilled_vars

    def keep_horiz_layer_vars(self, var_ls):
        horiz_layer_vars = []
        for var in var_ls:
            if self.is_var_with_horiz_layer_aggregation(var):
                horiz_layer_vars.append(var)
        return horiz_layer_vars

    def keep_replicate_agg_vars(self, var_ls):
        replicate_agg_vars = []
        for var in var_ls:
            if self.is_var_with_aggregate_qualifiers(var):
                replicate_agg_vars.append(var)
        return replicate_agg_vars


class SysUtil:
    def __init__(self):
        self.LINUX = 0
        self.WIN = 1
        self.OS_X = 2

    def get_platform(self):
        if _platform.startswith('linux'):
            return self.LINUX
        elif _platform.startswith('darwin'):
            return self.OS_X
        elif _platform.startswith('win'):
            return self.WIN


class StatsUtil:
    def __init__(self):
        pass

    def is_valid_input_for_corr_cond(self, input_x, input_y):
        n_nan_x_pts = None
        n_nan_y_pts = None

        is_masked_x = np.ma.is_masked(input_x)
        is_masked_y = np.ma.is_masked(input_y)

        if not is_masked_x:
            invalid_dist_x = Counter(np.isnan(input_x))
            n_nan_x_pts = invalid_dist_x.get(True)
        if not is_masked_y:
            invalid_dist_y = Counter(np.isnan(input_y))
            n_nan_y_pts = invalid_dist_y.get(True)

        n_input_x = len(input_x)
        n_input_y = len(input_y)
        is_valid = (not is_masked_x
                    and not is_masked_y
                    and n_nan_x_pts is None
                    and n_nan_y_pts is None
                    and n_input_x > 0
                    and n_input_y > 0
                    and n_input_x == n_input_y)
        return is_valid, n_nan_x_pts, n_nan_y_pts

    def ccorr(self, x, y, max_lags=12, normalized=True, is_HR=False):
        """
        Based off matplotlib.pyplot's xcorr implementation
        without plot functions
        """
        nx = len(x)
        ny = len(y)
        if nx != ny:
            err_msg = ('Length of x and y must be the same: '
                       f'x length: {nx}, y length: {ny}')
            raise Exception(err_msg)

        x = mlab.detrend(x)
        y = mlab.detrend(y)

        corr = np.correlate(x, y, mode='full')
        if normalized:
            corr = np.true_divide(corr, np.sqrt(np.dot(x, x) * np.dot(y, y)))

        if max_lags is None:
            max_lags = nx - 1

        if max_lags >= nx:
            err_msg = 'Max lags cannot exceed length of input array'
            raise Exception(err_msg)
        if max_lags < 1:
            err_msg = 'Max lags must be a positive number'
            raise Exception(err_msg)

        if is_HR:
            max_lags //= 2

        _log.info(f'Max lag used = {max_lags}')
        lags = np.arange(-max_lags, max_lags + 1)
        corr = corr[(nx - max_lags - 1): (nx + max_lags)]
        corr = ['%.3f' % e for e in corr]  # Round up results to 3 decimals
        return lags, corr


class FileUploadUtil:
    def __init__(self, upload_info_url, upload_file_url, amp_upload_email):
        self.upload_info_url = upload_info_url
        self.upload_file_url = upload_file_url
        self.amp_upload_email = amp_upload_email

    def upload(self, file_name, file_path, site_id, upload_comment,
               ok_msg='File {f} uploaded with token {t}.',
               error_msg='Problem uploading file {f}.'):
        intent_req = {'userid': getpass.getuser(),
                      'name': 'AMP Data Team',
                      'email': self.amp_upload_email,
                      'comment': upload_comment,
                      'SITE_ID': site_id,
                      'dataType': 'Half hourly data',
                      'dataFile': [file_name]}
        token, e = self.upload_intent(intent_req)
        if e:
            err_msg = e.read().decode('utf-8')
            raise Exception(f'{self.upload_info_url} returned status code '
                            f'{e.code}\n{err_msg}')
        info_msg, e = self.upload_file(token, file_name, file_path,
                                       ok_msg, error_msg)
        if e:
            err_msg = e.read().decode('utf-8')
            ws = self.upload_file_url.format(t=token, f=file_name)
            raise Exception(f'{ws} returned status code {e.code}\n{err_msg}')
        else:
            return info_msg

    def upload_intent(self, upload_intent):
        try:
            req = urllib.request.Request(self.upload_info_url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(upload_intent)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            response = urllib.request.urlopen(req, msg_bytes)
            return response.read().decode('utf-8').strip('"'), None
        except HTTPError as e:
            return None, e

    def upload_file(self, token, file_name, file_path,
                    ok_msg='File {f} uploaded with token {t}.',
                    error_msg='Problem uploading file {f}.'):
        try:
            req = urllib.request.Request(
                self.upload_file_url.format(t=token, f=file_name))
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            with open(os.path.join(file_path, file_name), 'r') as f:
                file_bytes = f.read().encode('utf-8')
            req.add_header('Content-Length', len(file_bytes))
            urllib.request.urlopen(req, file_bytes)
            info_msg = (ok_msg.format(f=file_name, t=token))
            _log.info(info_msg)
            return info_msg, None
        except HTTPError as e:
            info_msg = error_msg.format(f=file_name)
            return info_msg, e


class DataUtil:
    def __init__(self):
        self.missing_value = '-9999'

    def check_invalid_missing_value_format(self, data_value):
        """
        Assess whether data value has invalid missing value format
        :param data_value: data value to assess
        :return: logical: True if invalid, False if valid
        """
        common_values = ('', ' ', '  ', 'nan', 'na',
                         'inf', '-inf', 'infinity', '-infinity')
        # the regex below will match the pattern as follows:
        # a string starting with - and three or more
        #    characters that are either 6 or 9 an optional decimal
        #    point followed by one or more zeros or one
        #    or more 6 or 9 characters
        # a string that starts with "#" followed by one or more word
        #    characters (a-z, A-Z, or _) followed by either "?" or "!"
        #    both of these strings must end with the last matching
        #    character.
        # In future, might want to catch a warning
        #       for -9999 x multiples of 10: r'^(-(6|9){4,}(0+))?$'
        pattern = r'^(-(6|9){3,}(\.(0+|(6|9)+))?|#\w+(\?|!))$'
        data_value = data_value.lower()
        if (data_value != self.missing_value
            and (re.match(pattern, data_value) is not None
                 or data_value in common_values)):
            return True
        else:
            return False

    @staticmethod
    def check_invalid_data_row(data_row):
        """
        Assess whether valid data row entry. Checks for first characters of
            line to be part of timestamp, or numeric value
            NOTE: method will return invalid data row if a bad missing value
                  like NA or Inf is used at the beginning of the row.
        :param data_row: row of data file as text
        :return: logical: True if invalid, False if valid
        """
        # the following regex will match the pattern as follows:
        # a string starting with zero or more whitespace
        # characters followed by either a date string consisting
        # of one or two digits / one or two digits / two to four
        # digits or an optional sign one or mord digits
        # and an optional decimal point followed by
        # zero or more digits.
        pattern = r'^\s*((\d{1,2}/\d{1,2}/\d{2,4})|((\+|-)?\d+(\.\d*)?))'
        if re.match(pattern, data_row) is None:
            return True
        else:
            return False

    def are_all_headers_with_quotes(self, header_as_is):
        headers_with_quotes = [h_as_is for h_as_is in header_as_is
                               if '"' in h_as_is]
        if len(headers_with_quotes) == len(header_as_is):
            return True
        return False


if __name__ == '__main__':
    var_util = VarUtil()
    print(var_util.is_valid_variable('TIMESTAMP_START_1'))
    print(var_util.remove_dup_filled_nonfilled_var(
        ['PPFD_IN_F_1_2_1', 'PPFD_IN_F_2_2_1'], rm_which='non-filled'))
    print(var_util.get_top_level_variables(['PPFD_IN_1_2_1', 'PPFD_IN_1_3_1',
                                            'PPFD_IN_2_2_A']))
    print(var_util.get_lowest_horiz_variables(['PPFD_IN_1_2_1',
                                               'PPFD_IN_2_2_A']))
    print(var_util.get_lowest_r_variable(['PPFD_IN_1_2_1',
                                          'PPFD_IN_2_2_A']))
    print(var_util.get_top_level_variables(['PPFD_IN_2', 'PPFD_IN_1_3_1',
                                            'PPFD_IN_2_2_A']))
    print(var_util.get_lowest_horiz_variables(['PPFD_IN_2',
                                               'PPFD_IN_2_2_A']))
    print(var_util.get_lowest_r_variable(['PPFD_IN_2_2_A']))
