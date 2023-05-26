#!/usr/bin/env python

import argparse
import traceback
from time import time
from data_reader import DataReader
from file_name_verifier import FileNameVerifier
from logger import Logger
from status import StatusCode, StatusGenerator  # , StatusEncoder
from timestamp_checks import TimestampChecks
from gap_fill import GapFilled
from pathlib import Path
from process_status import ProcessStatus
from process_states import ProcessStates
from process_actions import ProcessActions
from report_status import ReportStatus
from file_fixer import FileFixer
from shutil import copyfile
from missing_value_format import MissingValueFormat
from data_missing import DataMissing
from utils import FilenameUtils
from data_report_gen import DataReportGen
from messages import Messages


def main():
    s_time = time()
    statuses = []
    report_statuses = []
    process_type = 'File Format'
    msg = Messages()

    parser = argparse.ArgumentParser(description='Upload checks')
    parser.add_argument(
        'filename', type=str, help='Target filename')
    parser.add_argument(
        'process_id', type=str, help='file processing ID')
    parser.add_argument(
        'run_type', type=str, help='(o)riginal or (r)epaired file run')
    parser.add_argument(
        'site_id', type=str, help='site_id')
    parser.add_argument(
        '-t', '--test', action='store_true', default=False,
        help='Sets flag for local run that does not write to database')
    args = parser.parse_args()

    _log = Logger(True, args.process_id,
                  args.site_id,
                  process_type).getLogger('upload_checks')  # Initialize logger

    status_ct = 0
    all_status = True
    ts_start = None
    ts_end = None
    local_run = args.test
    fixable = False

    try:
        log_start_time = _log.log_file_timestamp.strftime(
            format='%Y-%b-%d %H:%M %Z')

        original_filename = FilenameUtils().remove_upload_timestamp(
            args.filename)
        original_filename_path = Path(original_filename)
        fname_ext = original_filename_path.suffix
        d = DataReader()
        fnv = FileNameVerifier()  # keep status updated

        # if file is not an accepted archival format process as normal
        if fname_ext not in ('.zip', '.7z'):

            fnv_status = fnv.driver(args.filename)
            statuses.append(fnv_status)
            if len(statuses) == status_ct:
                all_status = False
            status_ct = len(statuses)
            status_msg = ('Error in processing. '
                          'Please see process log and debug.')

            # data reader
            try:
                dr_statuses = d.driver(args.filename, args.run_type)
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
                statuses.extend(ts_statuses)
                if len(statuses) == status_ct:
                    all_status = False
                status_ct = len(statuses)

                # Missing Value check
                statuses.append(MissingValueFormat().driver(d, args.filename))
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
            args.run_type = 'z'
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
        print(process_status_code)

        if process_status_code > -1:
            status_msg += (' No issues were encountered. Data will be'
                           ' queued for further data processing.')

        e_time = time()

        total_running_time = e_time - s_time
        _log.info(f'Total Format QA/QC time: {total_running_time} seconds')

        rs = ReportStatus()
        title_prefix = ''
        if args.run_type in ('o', 'z'):
            # original file
            if process_status_code < StatusCode.OK:
                if fixable:
                    # QAQC fail try fix
                    if not local_run:
                        rs.report_status(
                            ProcessActions.IssuesFound,
                            ProcessStates.IssuesFound, None, None,
                            process_id=args.process_id)
                    else:
                        args.process_id = '178'
                    fixer_status, filename, multi_zip = FileFixer().driver(
                        args.filename, args.process_id, local_run)
                    statuses.extend(fixer_status)
                    if len(statuses) == status_ct:
                        all_status = False
                    if all_status:
                        if args.run_type == 'z':
                            status_msg += ('Automated extraction of '
                                           f'{zip_filename} was attempted')
                            # if not multi_zip:
                            #     status_msg += '. '
                        else:
                            status_msg += (' Autocorrection of detected '
                                           'issues was attempted')
                    if fixer_status[0].get_status_code() < StatusCode.WARNING:
                        # fixer failed
                        if not local_run:
                            rs.report_status(
                                ProcessActions.FailedRepair,
                                ProcessStates.FailedRepair,
                                None, None, process_id=args.process_id)
                        s = ProcessStates.FailedRepairRetire
                        a = ProcessActions.FailedRepairRetire
                        status_msg += ' and FAILED.'
                    elif multi_zip is not None:
                        # Archive uploaded as individual files
                        s = ProcessStates.ArchiveUploaded
                        a = ProcessActions.ArchiveUpload
                        status_msg += (' and the included files were '
                                       're-uploaded. See separate Format '
                                       'QA/QC report for each included file.')
                    else:
                        # fixer worked
                        # fixer uploaded new file
                        if not local_run:
                            rs.report_status(
                                ProcessActions.AutoRepair,
                                ProcessStates.AutoRepair, None, None,
                                process_id=args.process_id)
                        s = ProcessStates.AutoRepairRetire
                        a = ProcessActions.AutoRepairRetire
                        if args.run_type == 'o':
                            status_msg += (' and autocorrected file was '
                                           f'uploaded: {filename}. '
                                           'See Format QA/QC report '
                                           'for autocorrected file.')
                    fixer_time = time() - e_time
                    _log.info(
                        f'Total fixer time time: {fixer_time} seconds')
                else:
                    if process_status_code < StatusCode.WARNING:
                        # QAQC failed
                        s = ProcessStates.FailedQAQC
                        a = ProcessActions.FailedQAQCorig
                    else:
                        # QAQC passed
                        copy_file(args, fnv)
                        s = ProcessStates.PassedQAQC
                        a = ProcessActions.PassedQAQC
                        status_msg += (' Data will be queued for '
                                       'further data processing.')
            else:
                # QAQC passed
                copy_file(args, fnv)
                s = ProcessStates.PassedQAQC
                a = ProcessActions.PassedQAQC
        else:
            # repaired file
            title_prefix = 'Autocorrected file: '
            if process_status_code < StatusCode.WARNING:
                # QAQC failed
                if not local_run:
                    rs.report_status(
                        ProcessActions.FailedQAQCrepair,
                        ProcessStates.FailedQAQC,
                        None, None, process_id=args.process_id)
                s = ProcessStates.FailedRepairRetire
                a = ProcessActions.FailedRepairRetire
                status_msg += (' Additional input is needed to further process'
                               ' the data.')
            else:
                # QAQC passed
                copy_file(args, fnv)
                s = ProcessStates.PassedQAQC
                a = ProcessActions.PassedQAQC
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
            check_summary = DataReportGen().gen_description(
                {'Format QAQC': statuses})
        process_status = ProcessStatus(
                    process_type=process_type,
                    filename=original_filename,
                    upload_filename=args.filename,
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
        if local_run:
            print(json_report)
            print(ts_start)
            print(ts_end)
        else:
            rs.report_status(
                action=a, status=s, report_json=json_report,
                status_json=json_status, log_file=_log.default_log,
                process_id=args.process_id, start_year=ts_start,
                end_year=ts_end)

    except Exception as ex:
        _log.info(f'unhandled exception {ex}')
        _log.info(traceback.format_exc())


def copy_file(args, fnv):
    site_path = Path.cwd() / fnv.fname_attrs['site_id']
    site_path.mkdir(parents=True, exist_ok=True)
    copyfile(args.filename, site_path / Path(args.filename).name)

if __name__ == "__main__":
    main()
