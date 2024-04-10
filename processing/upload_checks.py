#!/usr/bin/env python

import argparse
import sys
import traceback

from datetime import datetime as dt
from time import time
from typing import Optional

from data_reader import DataReader
from file_name_verifier import FileNameVerifier
from logger import Logger
from status import StatusCode, StatusGenerator  # , StatusEncoder
from timestamp_checks import TimestampChecks
from gap_fill import GapFilled
from pathlib import Path
from process_status import ProcessStatus
from process_states import ProcessStates, ProcessStateHandler
from report_status import ReportStatus
from file_fixer import FileFixer
from shutil import copyfile
from missing_value_format import MissingValueFormat
from data_missing import DataMissing
from utils import FilenameUtils, TimestampUtil
from data_report_gen import gen_description
from messages import Messages


def convert_ts_str_iso_format(ts_str: Optional[str]) -> Optional[str]:
    if ts_str is None or len(ts_str) != 12:
        return None
    return (f'{ts_str[0:4]}-{ts_str[4:6]}-{ts_str[6:8]} '
            f'{ts_str[8:10]}:{ts_str[10:12]}')


def upload_checks(
        filename: str, upload_id: int, run_type: str, site_id: str,
        prior_process_id: Optional[int], zip_process_id: Optional[int],
        local_run=False) -> (Optional[int], bool, Optional[str]):

    s_time = time()
    statuses = []
    report_statuses = []
    process_type = 'File Format'
    msg = Messages()

    start_time = dt.now()
    timestamp_str = start_time.isoformat()

    process_id = None
    process_log_path = None
    is_upload_successful = None
    multi_zip_uuid = None
    autorepair_uuid = None
    json_report, json_status = None, None
    data_start_timestamp, data_end_timestamp = None, None

    try:

        if not local_run:
            rs = ReportStatus()
            process_id = rs.register_format_qaqc_process(
                upload_id=upload_id, process_timestamp=timestamp_str,
                site_id=site_id, prior_process_id=prior_process_id,
                zip_process_id=zip_process_id)
            if not process_id:
                print(f'Attempt to process upload_id {upload_id} '
                      f'for site_id {site_id} failed.')
                # process_id, has_child_upload, upload_token
                return None, False, None
        else:
            process_id = 999999
            start_time = dt.strptime('202403250900', TimestampUtil().PREFERRED_TS_FORMAT)

        _log = Logger(True, process_id, site_id, process_type,
                      start_time).getLogger('upload_checks')  # Initialize logger

        process_log_path = _log.default_log

    except Exception as ex:
        msg = 'Error in initialization of upload_checks.'
        if not process_id:
            msg += ' Attempt to acquire process_id unsuccessful.'
        if not process_log_path:
            msg += ' Failed to initialize logger.'
        msg += f' Error: {ex}'
        raise Exception(msg)

    try:

        # set up summarized output items
        s = None
        data_start_timestamp = None
        data_end_timestamp = None
        json_status = None
        json_report = None

        process_states = ProcessStateHandler(initialize_lookup=not local_run)

        status_ct = 0
        all_status = True
        fixable = False

        log_start_time = start_time.strftime(
            format='%Y-%b-%d %H:%M %Z').strip(' ')

        original_filename = FilenameUtils().remove_upload_timestamp(
            filename)
        original_filename_path = Path(original_filename)
        fname_ext = original_filename_path.suffix
        d = DataReader()
        fnv = FileNameVerifier()  # keep status updated

        # if file is not an accepted archival format process as normal
        if fname_ext not in ('.zip', '.7z'):

            fnv_status = fnv.driver(filename)
            statuses.append(fnv_status)
            if len(statuses) == status_ct:
                all_status = False
            status_ct = len(statuses)
            status_msg = ('Error in processing. '
                          'Please see process log and debug.')

            # data reader
            try:
                dr_statuses = d.driver(filename, run_type)
                statuses.extend(dr_statuses)
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

            # if header can be read but not data, data reader checks complete.
                if d.data is None:
                    status_msg = ('Problem while loading data file;'
                                  ' QA/QC INCOMPLETE.')
                    _log.fatal(status_msg)
                    fixable = True

            except Exception as ex:
                status_msg = msg.get_msg(_log.getName(), 'CRITICAL')
                _log.fatal(status_msg + ' Exception thrown in data_reader.py')
                _log.info(f'error {ex}')
                _log.info(traceback.format_exc())
                statuses.append(StatusGenerator().status_generator(
                    logger=_log,
                    qaqc_check=msg.get_display_check(_log.getName()),
                    status_msg=status_msg,
                    report_type='single_list'))
                d.header = [('Data Reader Failed. '
                             'No header information available.')]
                fixable = True

            if d.data is not None:
                # timestamp checks
                ts_statuses, ts_start, ts_end = TimestampChecks().driver(
                    d, fnv.fname_attrs)  # keep status updated
                data_start_timestamp = convert_ts_str_iso_format(ts_start)
                data_end_timestamp = convert_ts_str_iso_format(ts_end)
                statuses.extend(ts_statuses)
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

                # Missing Value check
                statuses.append(MissingValueFormat().driver(d, filename))
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

                min_status_code = min([s.get_status_code() for s in statuses])
                if min_status_code < StatusCode.OK:
                    fixable = True

                # the remaining tests detect things that cannot be fixed
                # so if that is all that is wrong don't run the fixer

                # Gap Fill
                statuses.extend(GapFilled().driver(d))
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

                # All missing value Warning
                statuses.extend(DataMissing().driver(d))
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

                if all_status:
                    status_msg = 'All format QA/QC checks attempted.'
                else:
                    status_msg = ('Something unexpected happened. '
                                  'File Format QA/QC INCOMPLETE.')
        # if file is accepted archival format
        else:
            fixable = True
            run_type = 'z'
            d.original_header = [('Header information not available from '
                                  'archival file type (e.g., zip, 7z).')]
            zip_filename = original_filename_path.name
            zip_log = Logger().getLogger('zip_file')
            zip_qaqc_check = msg.get_display_check(zip_log.getName())
            zip_log.warning(
                f'file {zip_filename} is standard archival format')
            statuses.append(StatusGenerator().status_generator(
                logger=zip_log, qaqc_check=zip_qaqc_check,
                status_msg=zip_filename, report_type='single_list'))
            status_msg = ''
            if len(statuses) == status_ct:
                status_msg = ('Unexpected problem with archive file error '
                              'reporting. ')
            status_ct = len(statuses)

        # this code decides whether to go to fixer or not!
        process_status_code = min([s.get_status_code() for s in statuses])

        if process_status_code > -1:
            status_msg += (' No issues were encountered. Data will be'
                           ' queued for further data processing.')

        e_time = time()

        total_running_time = e_time - s_time
        _log.info(f'Total Format QA/QC time: {total_running_time} seconds')

        rs = ReportStatus()
        title_prefix = ''
        if run_type in ('o', 'z'):
            # original file
            if process_status_code < StatusCode.OK:
                if fixable:
                    # QAQC fail try fix
                    if not local_run:
                        rs.report_status(
                            process_id=process_id,
                            state_id=process_states.get_process_state(
                                ProcessStates.IssuesFound))

                    # get
                    fixer_status, autorepair_filename, multi_zip_uuid, \
                        autorepair_uuid, is_upload_successful = \
                        FileFixer().driver(
                            filename, process_id, site_id, local_run)
                    statuses.extend(fixer_status)

                    if len(statuses) == status_ct:
                        all_status = False

                    if all_status:
                        if run_type == 'z':
                            status_msg += ('Automated extraction of '
                                           f'{zip_filename} was attempted')
                            # if not multi_zip_uuid:
                            #     status_msg += '. '
                        else:
                            status_msg += (' Autocorrection of detected '
                                           'issues was attempted')
                    if fixer_status[0].get_status_code() < StatusCode.WARNING:
                        # fixer failed
                        if not local_run:
                            rs.report_status(
                                process_id=process_id,
                                state_id=process_states.get_process_state(
                                    ProcessStates.FailedRepair))
                        s = process_states.get_process_state(
                            ProcessStates.FailedRepairRetire)
                        status_msg += ' and FAILED.'
                    elif multi_zip_uuid is not None:
                        # Archive uploaded as individual files
                        s = process_states.get_process_state(
                            ProcessStates.ArchiveUploaded)
                        status_msg += (' and the included files were '
                                       're-uploaded. See separate Format '
                                       'QA/QC report for each included file.')
                    else:
                        # fixer worked
                        # fixer uploaded new file
                        if not local_run:
                            rs.report_status(
                                process_id=process_id,
                                state_id=process_states.get_process_state(
                                    ProcessStates.AutoRepair))

                        s = process_states.get_process_state(
                            ProcessStates.AutoRepairRetire)
                        if run_type == 'o':
                            status_msg += (' and autocorrected file was '
                                           f'uploaded: {autorepair_filename}. '
                                           'See Format QA/QC report '
                                           'for autocorrected file.')
                    fixer_time = time() - e_time
                    _log.info(
                        f'Total fixer time time: {fixer_time} seconds')
                else:
                    if process_status_code < StatusCode.WARNING:
                        # QAQC failed
                        s = process_states.get_process_state(
                            ProcessStates.FailedQAQC)
                    else:
                        # QAQC passed
                        copy_file(filename, fnv)
                        s = process_states.get_process_state(
                            ProcessStates.PassedQAQC)
                        status_msg += (' Data will be queued for '
                                       'further data processing.')
            else:
                # QAQC passed
                copy_file(filename, fnv)
                s = process_states.get_process_state(ProcessStates.PassedQAQC)
        else:
            # repaired file
            title_prefix = 'Autocorrected file: '
            if process_status_code < StatusCode.WARNING:
                # QAQC failed
                if not local_run:
                    rs.report_status(process_id=process_id,
                                     state_id=process_states.get_process_state(
                                         ProcessStates.FailedQAQC))
                s = process_states.get_process_state(
                    ProcessStates.FailedRepairRetire)
                status_msg += (' Additional input is needed to further process'
                               ' the data.')
            else:
                # QAQC passed
                copy_file(filename, fnv)
                s = process_states.get_process_state(ProcessStates.PassedQAQC)
                if process_status_code < StatusCode.OK:
                    status_msg += (' Data will be queued for '
                                   'further data processing.')
        original_filename_basename = original_filename_path.name
        report_title = f'{title_prefix}{original_filename_basename}'
        for stat in statuses:  # make this a method??
            sc = stat.get_status_code()
            sm = stat.get_status_msg()
            if sc < 0 or sm:
                report_statuses.append(stat.make_report_object())
        if local_run:
            check_summary = {}
        else:
            check_summary = gen_description({'Format QAQC': statuses})
        process_status = ProcessStatus(
                    process_type=process_type,
                    filename=original_filename,
                    upload_filename=filename,
                    report_title=report_title,
                    process_datetime=log_start_time,
                    process_log_file=rs.make_file_qaqc_url(_log.default_log),
                    headers=d.original_header,
                    status_start_msg=None,
                    # change this to reflect FATAL msg
                    status_end_msg=status_msg,
                    statuses=statuses,
                    report_statuses=report_statuses,
                    check_summary=check_summary)
        json_report = process_status.write_report_json()
        json_status = process_status.write_status_json()
    except Exception as ex:
        _log.info(f'unhandled exception {ex}')
        _log.info(traceback.format_exc())

    try:
        if local_run:
            print(process_id, is_upload_successful,
                  multi_zip_uuid or autorepair_uuid)
            print(json_report)
            print(data_start_timestamp)
            print(data_end_timestamp)
        else:
            rs = ReportStatus()
            rs.report_status(
                process_id=process_id,
                state_id=s, log_file_path=process_log_path,
                report_json=json_report, status_json=json_status,
                start_timestamp=data_start_timestamp,
                end_timestamp=data_end_timestamp)
    except Exception as ex:
        msg = f'Write of upload_checks output to webservice failed: {ex}'
        _log.info(msg)
        raise Exception(msg)

    return process_id, is_upload_successful, multi_zip_uuid or autorepair_uuid


def copy_file(filename, fnv):
    site_path = Path.cwd() / fnv.fname_attrs['site_id']
    site_path.mkdir(parents=True, exist_ok=True)
    copyfile(filename, site_path / Path(filename).name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload checks')
    parser.add_argument(
        'filename', type=str, help='Target filename')
    parser.add_argument(
        # 'process_id', type = str, help = 'file processing ID')
        'upload_id', type=int,
        help='log_id in the data_upload_log table for the file to process')
    parser.add_argument(
        'run_type', type=str, help='(o)riginal or (r)epaired file run')
    parser.add_argument(
        'site_id', type=str, help='site_id')
    parser.add_argument(
        '-ppid', '--prior_process_id', type=int,
        help='prior process id for run_type r')
    parser.add_argument(
        '-zid', '--zip_process_id', type=int,
        help='zip file process id if applicable for run_type o')
    parser.add_argument(
        '-t', '--test', action='store_true', default=False,
        help='Sets flag for local run that does not write'
             ' to database')
    args = parser.parse_args()

    format_process_id, child_upload_success, upload_token = \
        upload_checks(args.filename, args.upload_id, args.run_type,
                      args.site_id, args.prior_process_id,
                      args.zip_process_id, args.test)

    sys.exit((format_process_id, child_upload_success, upload_token))
