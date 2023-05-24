#!/usr/bin/env python

import datetime
from utils import TimestampUtil, TextUtil
from logger import Logger
from status import StatusGenerator
from messages import Messages

__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class TimestampChecks():

    def __init__(self):
        self._HH_DELTA = datetime.timedelta(minutes=30)
        self._HR_DELTA = datetime.timedelta(hours=1)
        self.gen = StatusGenerator()
        self.ts_util = TimestampUtil()
        self.status_msg_parts = []
        self.txt_util = TextUtil()
        self.msg = Messages()
        self.current_date = datetime.datetime.now()

    def _check_datetime_length(self, value, check_log):
        try:
            dt_str_len = len(value)
            if dt_str_len != 12:
                err_msg = f"Datetime string length is of length {dt_str_len}."
                check_log.error(err_msg)
            else:
                self.ts_util.cast_as_datetime(value, check_log)
        except Exception:
            fatal_msg = f"Unable to get length of timestamp string {value}."
            check_log.error(fatal_msg)

    def _has_identical_elements(self, ls, check_log):
        """Check if input list contains identical elements

        :param ls: input list
        :type ls: list

        :rtype: int
        :return: Returns difference between unique list and total list
        """
        # this should be changed to write some errors in
        # the log for which timestamps
        # then have the logger be a numbers type to count the errors found
        ts_dup = len(ls) - len(list(set(ls)))
        if ts_dup > 0:
            check_log.error(f'Found {ts_dup} duplicate timestamps')
        return ts_dup

    def _get_delta_from_filename_resolution(self, check_log):
        ts_delta = None
        if self.fname_attrs:
            fname_resolution = self.fname_attrs.get('resolution')
            if fname_resolution == 'HH':
                ts_delta = self._HH_DELTA
            elif fname_resolution == 'HR':
                ts_delta = self._HR_DELTA
            else:
                err_msg = f"Unknown resolution in filename: {fname_resolution}"
                check_log.fatal(err_msg)

        debug_msg = f"Time delta obtained from filename is {ts_delta}."
        check_log.debug(debug_msg)
        return ts_delta

    def check_filename_timestamps_to_data(self, check_log):
        msg_parts = []
        fname_ts_start = self.fname_attrs.get('ts_start')
        fname_ts_end = self.fname_attrs.get('ts_end')
        # in python 3 the timestamps are a byte array and not a string
        ts_start = self.data['TIMESTAMP_START'][0].decode('ascii')
        ts_end = self.data['TIMESTAMP_END'][-1].decode('ascii')
        warning_msg = ("Timestamp {s} differs between filename and "
                       "data: filename timestamp: {f_t}, data "
                       "timestamp: {d_t}")
        if fname_ts_start != ts_start:
            check_log.error(warning_msg.format(
                s="start", f_t=fname_ts_start, d_t=ts_start))
            # '{var} {d_t} does not match filename {f_n} time {f_t}'
            msg_parts.append(self.msg.get_msg(check_log.getName(), 'ERROR')
                             .format(f_t=fname_ts_start, d_t=ts_start,
                                     var='TIMESTAMP_START', f_n='ts-start'))
        if fname_ts_end != ts_end:
            check_log.error(warning_msg.format(
                s="end", f_t=fname_ts_end, d_t=ts_end))
            # 'TIMESTAMP_END {d_t} does not match filename  time {f_t}'
            msg_parts.append(self.msg.get_msg(check_log.getName(), 'ERROR')
                             .format(f_t=fname_ts_end, d_t=ts_end,
                                     var='TIMESTAMP_END', f_n='ts-end'))
        if not msg_parts:
            status_msg = None
        else:
            status_msg = ', '.join(msg_parts)
        return status_msg, ts_start, ts_end

    def check_timestamp_format(self, col_name, check_log):
        # Abbreviated names
        for ts in self.data[col_name]:
            self._check_datetime_length(ts, check_log)
            # moved this to _check_datetime_length
            # self.ts_util.cast_as_datetime(ts)

    def check_timestamp_resolution_by_col(
            self, col_name, check_log, check_log_base_name):
        """Check timestamps by column. This should handle that all
        timestamps are in ascending order.
        """
        # Abbreviated names
        _c_dt = self.ts_util.cast_as_datetime
        _ts = self.data[col_name]
        ts_delta = self._get_delta_from_filename_resolution(check_log)
        if not ts_delta:
            return self.msg.get_msg(check_log_base_name, 'CRITICAL')
            # 'Filename resolution not valid.
            # Timestamp resolution verification INCOMPLETE.'

        dif_ls = []
        for ts1, ts2 in zip(_ts[:-1], _ts[1:]):
            tsdt1 = _c_dt(ts1)
            tsdt2 = _c_dt(ts2)
            if tsdt1 is not None and tsdt2 is not None:
                dif_ls.append((tsdt2 - tsdt1) == ts_delta)
            else:
                dif_ls.append(False)

        is_OK = True
        for d, ts1, ts2 in zip(dif_ls, _ts[:-1], _ts[1:]):
            if not d:
                is_OK = False
                warning_msg = (f"Columnar timestamps between {ts1} and {ts2} "
                               "has different resolution from filename.")
                check_log.error(warning_msg)
        if is_OK:
            info_msg = f"Columnar timestamp checks for {col_name} are valid."
            check_log.info(info_msg)
        return None

    def check_timestamp_resolution_by_row(self, check_log):
        """Check timestamps by row. This should handle all
        timestamps in each row have the same resolution.
        """

        # Abbreviated names
        _c_dt = self.ts_util.cast_as_datetime
        ts_start = self.data['TIMESTAMP_START']
        ts_end = self.data['TIMESTAMP_END']
        ts_delta = self._get_delta_from_filename_resolution(check_log)

        if not ts_delta:
            return self.msg.get_msg(check_log.getName(), 'CRITICAL')
            # 'Filename resolution not valid.
            # Timestamp resolution verification INCOMPLETE.'

        dif_ls = []
        for ts1, ts2 in zip(ts_start, ts_end):
            tsdt1 = _c_dt(ts1)
            tsdt2 = _c_dt(ts2)
            if tsdt1 is not None and tsdt2 is not None:
                dif_ls.append((tsdt2 - tsdt1) == ts_delta)
            else:
                dif_ls.append(False)

        is_OK = True
        for idx, (d, start, end) in enumerate(zip(dif_ls, ts_start, ts_end)):
            ln_num = idx + 1  # Assume that header is always one row
            if not d:
                is_OK = False
                warning_msg = (f"Sample span between {start} and {end} has "
                               "different resolution from filename in "
                               f"line {ln_num}")
                check_log.error(warning_msg)
                return None
        if is_OK:
            info_msg = "Row-wise timestamp checks are valid."
            check_log.info(info_msg)
            return None

    def check_timestamp_duplicates(self, ts, check_log):
        return self._has_identical_elements(ts, check_log)

    def check_forward_filled_timestamps(self, check_log):
        """
        Check if data has been forward filled
        :param check_log: logger
        :return: status object
        """
        qaqc_check = self.msg.get_display_check(check_log.getName())
        msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
        last_ts_start = self.ts_util.cast_as_datetime(
            self.data['TIMESTAMP_START'][-1])
        if last_ts_start and last_ts_start > self.current_date:
            check_log.fatal(msg.format(last_timestamp=last_ts_start.strftime(
                self.ts_util.PREFERRED_TS_FORMAT)))
        return self._gen_status(
            check_log, qaqc_check, report_type='single_msg')

    def _gen_status(self, check_log, qaqc_check, report_type):
        return self.gen.status_generator(
            check_log, qaqc_check,
            status_msg=check_log.fatal_msg, report_type=report_type)

    def driver(self, data_reader=None, fname_attrs=None):
        self.data = data_reader.get_data()
        self.fname_attrs = fname_attrs
        ts_start = None
        ts_end = None

        statuses = []  # list to hold individual status objects

        # QAQC Check 0: headers are present in any column
        check_log = Logger().getLogger("timestamp_headers_present")
        qaqc_check = self.msg.get_display_check(check_log.getName())
        # "Are Timestamp variables present?"
        check_log.resetStats()
        ts_headers = True
        status_msg = []
        if 'TIMESTAMP_START' not in data_reader.header:
            check_log.fatal('TIMESTAMP_START not found in data file')
            ts_headers = False
            status_msg.append('TIMESTAMP_START')
        if 'TIMESTAMP_END' not in data_reader.header:
            check_log.fatal('TIMESTAMP_END not found in data file')
            status_msg.append('TIMESTAMP_END')
            ts_headers = False
        if not status_msg:
            status_msg = None
        else:
            status_msg = ', '.join(status_msg)
        statuses.append(
            self.gen.status_generator(
                logger=check_log, qaqc_check=qaqc_check,
                status_msg=status_msg, report_type='single_list'))

        # Consider adding a fatal option to catch all
        # QAQC Check 1: timestamp format
        report_type = 'sub_status_row'
        check_log = Logger().getLogger('timestamp_format')
        qaqc_check = self.msg.get_display_check(check_log.getName())
        # 'Are Timestamps in correct format?'
        check_log.resetStats()
        sub_statuses = {}
        ts_valid = True  # set format check to "pass"
        # for c_name in self.data.dtype.names:
        for c_name in data_reader.header:
            if c_name not in ('TIMESTAMP_START', 'TIMESTAMP_END'):
                continue
            check_log.info('Beginning ' + qaqc_check + ' for ' + c_name)
            sub_log = Logger().getLogger('timestamp_format_' + c_name)
            sub_log.resetStats()
            sub_report_type = 'numbers'
            self.check_timestamp_format(c_name, sub_log)
            if sub_log.error_count > 0 or sub_log.warning_count > 0:
                plural = self.txt_util.decide_plurals(
                    (sub_log.error_count, sub_log.warning_count))
                status_msg = self.msg.get_msg(
                    check_log.getName(), 'ERROR').format(cn=c_name, p=plural)
                ts_valid = False
            elif sub_log.fatal_count > 0:
                status_msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
                # 'Critical error; timestamp format verification INCOMPLETE.'
                sub_report_type = 'single_msg'
                ts_valid = False
            else:
                status_msg = None
            sub_statuses[c_name] = self.gen.status_generator(
                sub_log, qaqc_check,
                status_msg=status_msg,
                report_type=sub_report_type)
        # need a catch for fatal msgs
        if not sub_statuses:
            report_type = 'single_msg'
            sub_statuses = None  # check if this is necessary
        # may want to add a fail ts_valid from status of check_log??
        # leaving out for now so that we see the use cases
        # of unexpected error messages.
        statuses.append(
            self.gen.composite_status_generator(
                logger=check_log, qaqc_check=qaqc_check,
                # status_msg=status_msg,
                keep_sub_status_name=True,
                statuses=sub_statuses,
                report_type=report_type))
        # timestamp format needs to also pass
        if ts_headers and ts_valid:

            # TODO: generate critical results if this
            # if statement false (so we know these tests didn't run)

            #  QAQC Check 2: finename matches timestamp columns
            check_log = Logger().getLogger('filename_match_file')
            qaqc_check = self.msg.get_display_check(check_log.getName())
            # 'Does filename matches file contents?'
            check_log.resetStats()
            check_log.info("Beginning " + qaqc_check)
            status_msg, ts_start, ts_end = \
                self.check_filename_timestamps_to_data(check_log)
            # make this report_type='list' once list is ready
            statuses.append(self.gen.status_generator(
                logger=check_log, qaqc_check=qaqc_check,
                status_msg=status_msg, report_type='single_rows'))

            #  QAQC Check 3 and 4 in for loop
            check_log1 = Logger().getLogger('timestamp_resolution')
            check_log1.resetStats()
            sub_statuses1 = {}
            sub_log_base_name = 'ts_res_col'
            qaqc_check1 = self.msg.get_display_check(sub_log_base_name)
            # "Is Timestamp resolution BETWEEN rows OK?"
            check_log2 = Logger().getLogger('timestamp_duplicates')
            check_log2.resetStats()
            sub_statuses2 = {}
            qaqc_check2 = self.msg.get_display_check(check_log2.getName())
            # "Any Timestamp duplicates?"
            report_type = 'sub_status_row'
            # sub_statuses1_msg = []
            for c_name in self.data.dtype.names:
                if c_name not in ('TIMESTAMP_START', 'TIMESTAMP_END'):
                    continue
                # check 3: sub-check: timestamp resolution btw rows
                sub_log = Logger().getLogger(sub_log_base_name + c_name)
                sub_log.resetStats()
                sub_log.info("Beginning " + qaqc_check1 + " for " + c_name)
                # check returns a fatal message if there is
                # no valid delta time; otherwise none
                status_msg = self.check_timestamp_resolution_by_col(
                    c_name, sub_log, sub_log_base_name)
                if not status_msg:
                    if sub_log.error_count > 0 or sub_log.warning_count > 0:
                        plural = self.txt_util.decide_plurals(
                            (sub_log.error_count, sub_log.warning_count))
                        # ' timestamp{p}{c} with invalid resolution
                        # {res_type} rows'
                        status_msg = self.msg.get_msg(
                            check_log1.getName(), 'ERROR')\
                            .format(c=' in ' + c_name, p=plural,
                                    res_type='between')
                sub_statuses1[c_name] = self.gen.status_generator(
                    sub_log, qaqc_check1,
                    status_msg=status_msg,
                    report_type='numbers')
                # sub_statuses1_msg.append(sub_statuses1[c_name].get_status_msg())
                # check 4: duplicates
                sub_log = Logger().getLogger('ts_dup_' + c_name)
                sub_log.resetStats()
                sub_log.info("Beginning " + qaqc_check2 + " for " + c_name)
                status_msg = None
                # check returns number of duplicates
                no_dup = self.check_timestamp_duplicates(
                    self.data[c_name].tolist(), sub_log)
                if no_dup > 0:
                    plural = self.txt_util.decide_plurals([no_dup])
                    # ' duplicate timestamp{p} found in {c}'
                    status_msg = self.msg.get_msg(
                        check_log2.getName(),
                        'ERROR').format(c=c_name, p=plural)
                sub_statuses2[c_name] = (
                    self.gen.status_generator(
                        logger=sub_log, qaqc_check=qaqc_check2,
                        status_msg=status_msg, report_type='numbers'))
            if not sub_statuses2:
                status_msg = self.msg.get_msg(check_log2.getName(), 'CRITICAL')
                # No timestamp columns found. Timestamp verification INCOMPLETE
                check_log2.fatal(status_msg)
                report_type = 'single_msg'
                sub_statuses2 = None
            else:
                status_msg = None
            # make parent status for duplicate column checks
            statuses.append(self.gen.composite_status_generator(
                logger=check_log2, qaqc_check=qaqc_check2,
                status_msg=status_msg,
                keep_sub_status_name=True,
                report_type=report_type,
                statuses=sub_statuses2))

            #  QAQC Check 5: sub-check: timestamp resolution within row
            sub_log = Logger().getLogger('ts_res_row')
            sub_log.resetStats()
            qaqc_check = self.msg.get_display_check(sub_log.getName())
            # "Is Timestamp Resolution WITHIN row OK?"
            sub_log.info('Beginning ' + qaqc_check)

            status_msg = None

            if 'TIMESTAMP_START' and 'TIMESTAMP_END' in data_reader.header:
                report_type = 'single_msg'
                sub_msg = self.check_timestamp_resolution_by_row(sub_log)
                if not sub_msg:
                    if sub_log.error_count > 0 or sub_log.warning_count > 0:
                        plural = self.txt_util.decide_plurals(
                            (sub_log.error_count, sub_log.warning_count))
                        sub_msg = self.msg.get_msg(
                            check_log1.getName(), 'ERROR') \
                            .format(c='', p=plural, res_type='within')
                        # ' timestamp{p} with invalid resolution within row'
                        # .format(p=plural)
                        report_type = 'numbers'
                sub_statuses1['ts_row_res'] = self.gen.status_generator(
                    logger=sub_log,
                    qaqc_check=qaqc_check,
                    status_msg=sub_msg,
                    report_type=report_type)
                # sub_statuses1_msg.append(sub_statuses1['ts_row_res'].get_status_msg())
            else:
                # This check should be obsolete b/c we should never get here
                # because time variables have to be correct to do this tests.
                status_msg = ('Unexpected error in timestamp '
                              'resolution WITHIN row check.')
                # 'TIMESTAMP_START and TIMESTAMP_END not present.' \
                # 'Timestamp row resolution verification INCOMPLETE.'
                sub_log.fatal(status_msg)

            # make parent status for resolution checks (3 & 5)
            report_type = 'sub_status_row'
            is_msg_same = False
            if sub_statuses1:
                all_sub_msg = []
                for s in sub_statuses1.keys():
                    all_sub_msg.append(sub_statuses1[s].get_status_msg())
                if len(set(all_sub_msg)) < 2 \
                   and all_sub_msg[0] \
                   and len(all_sub_msg) > 1:
                    is_msg_same = True

            if not sub_statuses1 or is_msg_same:
                if sub_statuses1:
                    print(sub_statuses1.keys())
                    status_msg = sub_statuses1[list(
                        sub_statuses1.keys())[0]].get_status_msg()
                else:
                    status_msg = ('Unexpected error in '
                                  'timestamp resolution check.')
                check_log1.fatal(status_msg)
                sub_statuses1 = None
                report_type = 'single_msg'

            qaqc_check = self.msg.get_display_check(check_log1.getName())
            statuses.append(
                self.gen.composite_status_generator(
                    logger=check_log1,
                    qaqc_check=qaqc_check,
                    # 'Is Timestamp resolution OK?',
                    status_msg=status_msg,
                    keep_sub_status_name=True,
                    report_type=report_type,
                    statuses=sub_statuses1))

            # Check for forward filled timestamps
            ff_log = Logger().getLogger('ts_filled_forward')
            statuses.append(self.check_forward_filled_timestamps(ff_log))

        else:
            check_log = Logger().getLogger('incomplete_timestamp_checks')
            qaqc_check = self.msg.get_display_check(check_log.getName())
            # 'Incomplete Timestamp Checks'
            status_msg = self.msg.get_msg(check_log.getName(), 'CRITICAL')
            # 'Filename Matches File Contents, Timestamp Column Resolution,
            # Timestamp Row Resolution, Timestamp Duplicates'
            check_log.fatal('The following QA/QC INCOMPLETE: '
                            + status_msg
                            + ' b/c timestamps not present '
                            + 'or in invalid format.')
            statuses.append(
                self.gen.status_generator(
                    logger=check_log, qaqc_check=qaqc_check,
                    status_msg=status_msg, report_type='single_msg'))
        return statuses, ts_start, ts_end


if __name__ == "__main__":
    # Testing only
    pass
