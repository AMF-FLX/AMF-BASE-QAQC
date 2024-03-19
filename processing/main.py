import argparse
import os
import traceback

from time import time

from data_reader import DataReader
from data_report_gen import DataReportGen, gen_description
from diurnal_seasonal_pattern import DiurnalSeasonalPattern
from file_name_verifier import FileNameVerifier
from gap_fill import GapFilled
from join_site_data import JoinSiteData
from logger import Logger
from multivariate_comparison import MultivariateComparison
from plot_config import PlotConfig
from physical_range import PhysicalRange
from process_status import ProcessStatus
from process_states import ProcessStates, ProcessStateHandler
from publish import Publish
from report_status import ReportStatus
# from shadows import Shadows
from site_attrs import SiteAttributes
# from spike_detection import SpikeDetection
# from SSITC_fetch_filter import SSITC_FF_check
from status import StatusCode
from sw_in_pot_gen import SW_IN_POT_Generator
from timestamp_alignment import TimestampAlignment
from timestamp_checks import TimestampChecks
from ustar_filtering import USTARFiltering
from variable_coverage import VariableCoverage


def main():
    s_time = time()
    process_type = 'BASE Generation'

    parser = argparse.ArgumentParser(description='Main QAQC Driver')
    parser.add_argument(
        'site_id', type=str, help='Target site ID')
    parser.add_argument(
        'resolution', type=str, help='Target resolution (HH or HR)')
    parser.add_argument(
        '-t', '--test', action='store_true',
        help='Test mode: requires filename argument')
    parser.add_argument(
        '-fn', '--filename', type=str, help='Path of test file')
    parser.add_argument(
        '-np', '--no_pub', action='store_true',
        help='Test mode: do not publish logs and plots')
    parser.add_argument(
        '-fa', '--force_amp_review', action='store_true',
        help='Force AMP review, i.e., over-ride self-review if site '
             'is a self-review site')

    args = parser.parse_args()
    # parser.parse_known_args

    if args.site_id not in SiteAttributes().get_site_dict():
        print(f'Invalid SITE_ID {args.site_id} exiting.')
        return

    if args.test:
        if not args.filename:
            print('Test argument needs to be used with filename argument')
            return
        else:
            process_id = str('TestProcess_###')
    else:
        rs = ReportStatus()
        process_id = ReportStatus().register_siteRes_process(
            args.site_id, args.resolution)

    # Initialize logger
    _log = Logger(True, process_id, args.site_id, process_type).getLogger(
        'BASE Generation')
    log_dir = _log.get_log_dir()
    base_dir_for_run = os.path.split(log_dir)[0]

    try:
        process_states = ProcessStateHandler()
        log_start_time = _log.log_file_timestamp.strftime(
            format='%Y-%b-%d %H:%M %Z')
        status_list = {}
        report_statuses = {}
        process_status_codes = []

        p = PlotConfig(True)
        plot_dir = p.get_plot_dir_for_run(args.site_id, process_id)

        if not args.no_pub:
            publisher = Publish()
            # get the ftp directories, otherwise set them to the local path
            ftp_site_dir = publisher._ssh_getdirname(args.site_id)
            if not ftp_site_dir:
                publisher._ssh_mkdir(args.site_id)
                ftp_site_dir = publisher._ssh_getdirname(args.site_id)
        else:
            ftp_site_dir = args.site_id
        ftp_plot_dir = p.get_ftp_plot_dir_for_run(
            args.site_id, process_id, ftp_site_dir)
        ftp_dir = os.path.split(ftp_plot_dir)[0]

        input_file_order = None

        if args.test:
            if args.filename:
                fname = args.filename
                # status_list = []
            else:
                _log.error('Test filename not specified.')
                return
        else:
            fname, status, input_file_order = JoinSiteData().driver(
                process_id, args.site_id, args.resolution)
            if not fname:
                _log.fatal('JoinSiteData did not produce a file, exiting.')
                if not args.test:
                    rs.report_status(
                        process_id=process_id,
                        state_id=process_states.get_process_state(
                            ProcessStates.CombinerFailed))
                return
            if not args.test:
                rs.report_status(
                    process_id=process_id,
                    state_id=process_states.get_process_state(
                        ProcessStates.FilesCombined))
            qaqc_check = 'File Combiner'
            status_list[qaqc_check] = status
            report_list, process_status_code = select_report(
                stat_list=status, test_name=qaqc_check, _log=_log)
            report_statuses[qaqc_check] = report_list
            process_status_codes.append(process_status_code)
            input_files = set([(f.proc_id, f.name) for f in input_file_order])
            input_files = [
                {'filename': f[1], 'process_id': f[0]} for f in input_files
            ]

        fnv = FileNameVerifier()
        fnv_status = fnv.driver(fname)
        format_status_list = []
        format_status_list.append(fnv_status)

        site_id = fnv.fname_attrs.get('site_id')
        resolution = fnv.fname_attrs.get('resolution')

        if site_id != args.site_id:
            _log.fatal(
                f'Filename Site_ID {site_id} does not '
                f'match specified run Site_ID {args.site_id}')

        d = DataReader()
        format_status_list.extend(d.driver(fname, run_type='o'))
        # put a logical in to make sure passes data reader OK?

        # Check that there isn't a root header and _1_1_1 qualifier
        # header for the same base variable
        # e.g. PA and PA_1_1_1 shouldn't be in the same file
        _log.info('Checking for duplicate root/qualifier headers')
        format_status_list.append(d.check_root_qualifier_headers())

        ts_check = TimestampChecks()
        ts_check_status, ts_start, ts_end = ts_check.driver(d, fnv.fname_attrs)
        format_status_list.extend(ts_check_status)

        qaqc_check = 'Format QAQC'
        status_list[qaqc_check] = format_status_list
        report_list, process_status_code = select_report(
            stat_list=format_status_list,
            test_name=qaqc_check, _log=_log)
        report_statuses[qaqc_check] = report_list
        process_status_codes.append(process_status_code)
        # put a logical in to make sure it passes the timestamp checks?

        # Check for variable coverage
        qaqc_check = 'Variable Coverage'
        _log.info(f'Running {qaqc_check} check')
        check_status = VariableCoverage().driver(
            d, site_id, process_id, resolution)
        status_list[qaqc_check] = check_status
        test_report, process_status_code = select_report(
            stat_list=check_status, test_name=qaqc_check, _log=_log)
        report_statuses[qaqc_check] = test_report
        process_status_codes.append(process_status_code)

        # Gap Fill
        # To Do: change this to "data" mode so that an error is
        #        thrown when there is not data for the mandatory variables
        _log.info('Running Gap fill check')
        status_list['Gap-fill Analysis'] = GapFilled().driver(d)

        # To Do: split off variables with no values

        # Physical Range test
        qaqc_check = 'Physical Range'
        _log.info('Running ' + qaqc_check)
        check_status, test_plot_dir = PhysicalRange(
            site_id=site_id,
            process_id=process_id,
            plot_dir=plot_dir,
            ftp_plot_dir=ftp_plot_dir).driver(d)
        status_list[qaqc_check] = check_status
        test_report, process_status_code = select_report(
            stat_list=check_status, test_name=qaqc_check,
            test_plot_dir=test_plot_dir.replace(plot_dir, ftp_plot_dir),
            _log=_log)
        report_statuses[qaqc_check] = test_report
        process_status_codes.append(process_status_code)

        # Multivariate Comparison
        qaqc_check = 'Multivariate Comparison'
        _log.info('Running ' + qaqc_check)
        check_status, test_plot_dir = MultivariateComparison(
            site_id, process_id, plot_dir, ftp_plot_dir).driver(d)
        status_list[qaqc_check] = check_status
        test_report, process_status_code = select_report(
            stat_list=check_status, test_name=qaqc_check,
            test_plot_dir=test_plot_dir.replace(plot_dir, ftp_plot_dir),
            _log=_log)
        report_statuses[qaqc_check] = test_report
        process_status_codes.append(process_status_code)

        # Diurnal Seasonal Pattern analysis
        qaqc_check = 'Diurnal Seasonal Pattern'
        _log.info('Running ' + qaqc_check)
        ds = DiurnalSeasonalPattern(
            site_id, process_id, resolution,
            plot_dir=plot_dir, ftp_plot_dir=ftp_plot_dir)
        check_status, test_plot_dir = ds.driver(d)
        status_list[qaqc_check] = check_status
        test_report, process_status_code = select_report(
            stat_list=check_status, test_name=qaqc_check,
            test_plot_dir=test_plot_dir.replace(plot_dir, ftp_plot_dir),
            _log=_log)
        report_statuses[qaqc_check] = test_report
        process_status_codes.append(process_status_code)

        # Spike detection -- uncomment for testing
        # _log.info('Running spike detection')
        # sd = SpikeDetection()
        # status_list['Spike Detection'] = sd.driver(d)

        # Radiation hunter
        # _log.info('Running radiation hunter')
        # status_list['Radiation Shadows'] = \
        #     Shadows().driver(d, resolution.lower())

        # SSITC fetch filter check -- not working yet!
        # _log.info('Running SSITC fetch filter check')
        # status_list.extend(SSITC_FF_check().driver(d))

        # Get rem_sw_in_data for use in Timestamp Alignment checks below
        _log.info('Running SW_IN_POT generator')
        gen = SW_IN_POT_Generator()
        rem_sw_in_pot_data = gen.gen_rem_sw_in_pot_data(
                d, process_id, resolution, site_id, ts_start, ts_end)
        # Merge POT_data with rest of the data
        d.data = gen.merge_data(
            d, site_id, resolution, process_id, ts_start, ts_end)
        # Add SW_IN_POT as a valid data header
        d.base_headers['SW_IN_POT'] = ['SW_IN_POT']

        # USTAR Filtering
        _log.info('Running USTAR Filtering')
        check_status = USTARFiltering(
            site_id, process_id, plot_dir=plot_dir,
            ftp_plot_dir=ftp_plot_dir
        ).driver(d)
        status_list['USTAR Filtering'] = check_status

        # Timestamp Alignment checks
        qaqc_check = 'Timestamp Alignment'
        _log.info('Running ' + qaqc_check)
        check_status, test_plot_dir = TimestampAlignment().driver(
            data_reader=d, rem_sw_in_data=rem_sw_in_pot_data,
            site_id=site_id, resolution=resolution,
            output_dir=plot_dir, ftp_plot_dir=ftp_plot_dir)

        status_list[qaqc_check] = check_status
        test_report, process_status_code = select_report(
            stat_list=check_status, test_name=qaqc_check,
            test_plot_dir=test_plot_dir.replace(plot_dir, ftp_plot_dir),
            _log=_log)
        report_statuses[qaqc_check] = test_report
        process_status_codes.append(process_status_code)

        worst_process_status_code = min(process_status_codes)
        # print(worst_process_status_code)
        status_msg = 'Maybe all checks complete.'
        if worst_process_status_code < -2:
            status_msg = 'Checks INCOMPLETE.'

        if args.test:
            input_files = fname
            process_id = '9999'
            check_summary = 'test_summary'
        else:
            check_summary = gen_description(status_list)

        process_status = ProcessStatus(
            process_type=process_type,
            filename=fname,  # this is the combined file
            # list of dict obj for files combined in combiner
            files_combined=input_files,
            process_resolution=args.resolution,
            process_id=process_id,
            process_code=worst_process_status_code,
            process_datetime=log_start_time,
            # process_log_file=rs.make_site_res_qaqc_url(_log.default_log)
            process_log_file=_log.default_log.replace(
                base_dir_for_run, ftp_dir),
            headers=d.header,
            status_start_msg=None,
            status_end_msg=status_msg,
            statuses=status_list,
            report_statuses=report_statuses,
            check_summary=check_summary)

        # Write to database
        state = ProcessStates.FinishedQAQC
        # write jsons
        json_report = process_status.write_report_json()
        json_status = process_status.write_status_json()

        if args.test:
            print(json_report)
        else:
            rs.report_status(
                state_id=state, report_json=json_report,
                log_file_path=_log.default_log,
                process_id=process_id, status_json=json_status)

        # Publish files to FTP
        if not args.no_pub:
            publisher.transfer(site_id, process_id)

        if not args.test:
            DataReportGen().driver(
                site_id, resolution, process_id, input_file_order,
                status_list, ftp_plot_dir, args.force_amp_review)

        e_time = time()

        total_running_time = e_time - s_time
        _log.info(f'Total running time: {total_running_time} seconds')
    except Exception as e:
        _log.info(f'Unhandled exception {e}')
        _log.info(traceback.format_exc())


def select_report(stat_list, test_name, _log, test_plot_dir=None):
    stat_map = StatusCode()
    test_report = {'info_msg_list': [],
                   'plot_dir_link': test_plot_dir,
                   'table': [],
                   'high_level_status': []}
    all_status_codes = []
    all_plots = []
    # print(len(stat_list))
    for stat in stat_list:
        # print(stat)
        sm = stat.get_status_msg()
        sc = stat.get_status_code()
        sr = stat.get_report_section()
        # print((stat, sc, sm, sr))
        all_status_codes.append(sc)
        if stat.get_plot_paths():
            all_plots.extend(stat.get_plot_paths())
        if sr == 'high_level':
            if not sm:
                sm = 'This is a test message.'
            test_report['high_level_status'].append(
                {'status': stat_map.get_str_repr(sc), 'status_msg': sm})
        elif sr == 'info':
            test_report['info_msg_list'].append(sm)
        else:
            if sc < 0 or sm:
                test_report['table'].append(stat.make_report_object())
    worst_status_code = min(all_status_codes)
    if worst_status_code > -1:
        status_msg = 'No errors or warnings found.'
        _log.info(test_name + status_msg)
        test_report['info_msg_list'].append(status_msg)
    elif -2 < worst_status_code < 0:
        warning_msg = f'{test_name}: Warnings found.'
        _log.warning(warning_msg)
    elif -3 < worst_status_code < -1:
        err_msg = f'{test_name}: Errors and possibly warnings found.'
        _log.error(err_msg)
    elif worst_status_code < -2:
        critical_msg = f'{test_name}: Critical error'
        _log.critical(critical_msg)

    test_report['overall_status'] = StatusCode().get_str_repr(
        worst_status_code)
    test_report['all_plots'] = all_plots
    '''
    test_report['high_level_status'] = [
    {'status': 'OK', 'status_msg': 'This is an example'},
    {'status': 'WARNING', 'status_msg': 'Fake info'},
    {'status': 'ERROR', 'status_msg': 'for UI testing only.'},
    {'status': 'INFO', 'status_msg': 'Happy designing!'}]
    '''
    return test_report, worst_status_code


if __name__ == '__main__':
    main()
