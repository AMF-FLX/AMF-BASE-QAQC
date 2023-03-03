import json
import matplotlib.pyplot as plt
import mmap
import numpy as np
import os
import pytest
import random
import shutil
import sys

from configparser import ConfigParser
from data_reader import DataReader
from diurnal_seasonal_pattern import DiurnalSeasonalPattern
from fp_vars import FPVariables
from logger import Logger
from plot_config import PlotConfig
from status import Status, StatusCode, StatusGenerator

__author__ = 'You-Wei Cheah', 'Josh Geden'
__email__ = 'ycheah@lbl.gov', 'joshgeden10@gmail.com'


# Path to json file with test inputs & expected values for e2e tests
json_file_path = os.path.join(
    'test', 'testdata', 'diurnal_seasonal_pattern',
    'test_diurnal_seasonal_pattern.json')


def do_nothing(*args, **kwargs):
    return


def mock_FPVariables_init(self):
    self.fp_vars = {
        'FC': 'dummy',
        'H': 'dummy',
        'G': 'dummy',
        'SW_IN': 'dummy',
        'TA': 'dummy',
        'TIMESTAMP_END': 'dummy',
        'TIMESTAMP_START': 'dummy',
    }


@pytest.fixture
def rand_site_diurnal_seasonal():
    process_id = random.randint(0, sys.maxsize)
    return DiurnalSeasonalPattern('AA-FLX', process_id, 'HH')


@pytest.fixture
def diurnal_seasonal(monkeypatch):
    ''' Initializes generic diurnal seasonal pattern'''
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    return DiurnalSeasonalPattern(None, None, None)


def set_class_variables(diurnal_seasonal, site_id, resolution):
    process_id = random.randint(0, sys.maxsize)
    diurnal_seasonal.site_id = site_id
    diurnal_seasonal.resolution = resolution
    diurnal_seasonal.process_id = process_id


def test_historical_data_avail(diurnal_seasonal):
    set_class_variables(diurnal_seasonal, 'US-MMS', 'HR')
    assert diurnal_seasonal.check_historical_data_avail() is True

    set_class_variables(diurnal_seasonal, 'US-Ha1', 'HH')
    assert diurnal_seasonal.check_historical_data_avail() is False

    set_class_variables(diurnal_seasonal, 'AA-Flx', 'HH')
    assert diurnal_seasonal.check_historical_data_avail() is False


def test_find_year_indices(diurnal_seasonal):
    timestamps = [
        b'201201020000',
        b'201301010000',
        b'201401010000'
    ]

    expected_indices = [0, 1, 2]
    actual_indices = diurnal_seasonal.find_year_indices(timestamps)
    assert expected_indices == actual_indices


def test_find_vars(diurnal_seasonal):
    hist_vars_with_qualifiers = ['CO2', 'H2O', 'FC', 'X']
    diurnal_seasonal.input_data = np.array(
        [(b'201101010000', b'201101010030', '1', '1', '1')],
        dtype=[('TIMESTAMP_START', 'S25'), ('TIMESTAMP_STOP', 'S25'),
               ('CO2', '<f8'), ('H2O', '<f8'), ('FC', '<f8')]
    )

    expected_vars = ['CO2', 'H2O', 'FC']
    actual_vars = diurnal_seasonal.find_vars(hist_vars_with_qualifiers)

    assert expected_vars == actual_vars


def test_partitioning_by_days(diurnal_seasonal):
    timestamps = [
        b'201101010000',  # Jan 01
        b'201101150000',  # Jan 15
        b'201101310000',  # Jan 31
        b'201102150000',  # Feb 15
        b'201103020000'   # Mar 02
    ]

    expected_indices = [0, 2, 4]
    actual_indices = diurnal_seasonal.partitioning_by_days(
        timestamps, 30)

    assert expected_indices == actual_indices


def test_get_start_end_idxs(diurnal_seasonal):
    idxs = [0, 3, 10]
    vals = [x for x in range(20)]

    expected = [(0, 3), (3, 10), (10, 20)]
    actual = diurnal_seasonal._get_start_end_idxs(idxs, vals)

    assert expected == actual


def test_calculate_median(diurnal_seasonal):
    x_vals = [1, 1, 1, 2, 2, 2, 3, 3, 3]
    y_vals = [0, 5, 10, 0, 10, 20, 0, 20, 40]

    expected_x, expected_y = [1, 2, 3], [5, 10, 20]
    actual_x, actual_y = diurnal_seasonal.calculate_median(x_vals, y_vals)

    assert expected_x == actual_x
    assert expected_y == actual_y


def test_get_params_from_config(monkeypatch):
    monkeypatch.setattr(DiurnalSeasonalPattern, '__init__', do_nothing)

    shutil.copy('qaqc.cfg', '_temp_qaqc.cfg')
    test_cfg = os.path.join(
        'test', 'testdata', 'diurnal_seasonal_pattern', 'test_qaqc.cfg')
    shutil.copy(test_cfg, 'qaqc.cfg')

    ds = DiurnalSeasonalPattern(None, None, None)

    hist_dir_path, outer_band_error_threshold, \
        outer_band_warning_threshold, inner_band_error_threshold, \
        inner_band_warning_threshold, cross_cor_threshold = \
        ds.get_params_from_config()

    assert ds.hist_dir == '../diurnal_seasonal_range'

    assert hist_dir_path == os.path.join(os.getcwd(), ds.hist_dir)
    assert outer_band_error_threshold == 0.3
    assert outer_band_warning_threshold == 0.15
    assert inner_band_error_threshold == 0.15
    assert inner_band_warning_threshold == 0.3
    assert cross_cor_threshold == 0.4

    # Reset the default qaqc.cfg file
    shutil.copy('_temp_qaqc.cfg', 'qaqc.cfg')
    os.remove('_temp_qaqc.cfg')


def test_get_available_hist_vars(diurnal_seasonal):
    diurnal_seasonal.hist_dir_path = os.path.join(
        'test', 'testdata', 'diurnal_seasonal_pattern', 'historical_data')
    diurnal_seasonal.site_id = 'US-CRT'
    diurnal_seasonal.hist_names = ('LOWER1', 'LOWER2', 'MEDIAN',
                                   'UPPER1', 'UPPER2')
    diurnal_seasonal.doy_var = 'DOY2'
    diurnal_seasonal.hr_var = 'HR2'

    expected_hist_vars = ['CO2', 'H2O', 'FC', 'NEE', 'CH4', 'FCH4', 'H', 'LE',
                          'G_1_1_1', 'G_2_1_1', 'WS', 'USTAR', 'W_SIGMA',
                          'V_SIGMA', 'U_SIGMA', 'T_SONIC', 'T_SONIC_SIGMA',
                          'PA', 'RH', 'TA', 'TS_1_1_1', 'TS_2_1_1',
                          'NETRAD', 'PPFD_IN', 'SW_IN', 'SW_OUT', 'LW_IN',
                          'LW_OUT']

    actual_hist_vars = diurnal_seasonal.get_available_hist_vars()

    assert expected_hist_vars == actual_hist_vars


def test_get_latest_ver(diurnal_seasonal):
    ver_ls = ['1-1', '2']
    assert diurnal_seasonal._get_latest_ver(ver_ls) == '2-1'

    ver_ls = ['1-1', '1-2']
    assert diurnal_seasonal._get_latest_ver(ver_ls) == '1-1'

    ver_ls = ['1-1', '2-2']
    assert diurnal_seasonal._get_latest_ver(ver_ls) == '2-2'


def test_get_historic_data_idx(diurnal_seasonal):
    doy_list = [0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0]

    expected_idxs = [[0, 3], [3, 6], [6, 8]]
    actual_idxs = diurnal_seasonal.get_historic_data_idx(doy_list)

    assert expected_idxs == actual_idxs


def test_add_result_summary_stat(diurnal_seasonal):
    # All good
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.OK,
        outer_band_code=StatusCode.OK,
        inner_band_code=StatusCode.OK)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.OK

    # R_xy < -0.4
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.ERROR,
        outer_band_code=StatusCode.OK,
        inner_band_code=StatusCode.OK)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.ERROR

    # P_iqr < threshold1
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.OK,
        outer_band_code=StatusCode.OK,
        inner_band_code=StatusCode.ERROR)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.ERROR

    # P_95% < threshold1
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.OK,
        outer_band_code=StatusCode.ERROR,
        inner_band_code=StatusCode.OK)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.ERROR

    # abs(R_xy) > threshold0 and abs(t_max) > 0
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.WARNING,
        outer_band_code=StatusCode.OK,
        inner_band_code=StatusCode.OK)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.WARNING

    # P_iqr < threshold3
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.OK,
        outer_band_code=StatusCode.OK,
        inner_band_code=StatusCode.WARNING)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.WARNING

    # P_95% > threshold4
    var_status, year_status = create_test_stats(
        corr_code=StatusCode.OK,
        outer_band_code=StatusCode.WARNING,
        inner_band_code=StatusCode.OK)
    diurnal_seasonal.add_result_summary_stat([var_status])
    assert year_status.get_summary_stat('result') == StatusCode.WARNING


def create_test_stats(corr_code, outer_band_code, inner_band_code):
    """ Builds nested Status objects for use in
        test_add_result_summary_stat """

    qaqc_check = 'diurnal_seasonal_pattern-2012-FC-ccorr_check'
    corr_status = Status(
        status_code=corr_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if corr_code == StatusCode.WARNING else 0,
        n_error=1 if corr_code == StatusCode.ERROR else 0
    )
    corr_status.add_summary_stats({
        'time_lag': 1,
        'ccorr': 1
    })

    qaqc_check = 'diurnal_seasonal_pattern-2012-FC-outer_band_check'
    outer_status = Status(
        status_code=outer_band_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if outer_band_code == StatusCode.WARNING else 0,
        n_error=1 if outer_band_code == StatusCode.ERROR else 0
    )
    outer_status.add_summary_stat('outer_band', 1)

    qaqc_check = 'diurnal_seasonal_pattern-2012-FC-inner_band_check'
    inner_status = Status(
        status_code=inner_band_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if inner_band_code == StatusCode.WARNING else 0,
        n_error=1 if inner_band_code == StatusCode.ERROR else 0
    )
    inner_status.add_summary_stat('inner_band', 1)

    qaqc_check = 'diurnal_seasonal_pattern-2012-FC'
    log_obj = Logger().getLogger(qaqc_check)
    year_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            corr_status.get_qaqc_check(): corr_status,
            outer_status.get_qaqc_check(): outer_status,
            inner_status.get_qaqc_check(): inner_status
        }
    )

    qaqc_check = 'diurnal_seasonal_pattern-FC'
    log_obj = Logger().getLogger(qaqc_check)
    var_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            year_status.get_qaqc_check(): year_status
        }
    )

    return var_status, year_status


def parse_json(json_file_path: str):
    """ Parses the json file containing the test inputs & expected values
    to be formatted for use with pytest.mark.parametrize() """

    with open(json_file_path) as f:
        data = json.load(f)

    # Obtains the arguments that are required to call test_timeshift()
    input_vars = data['input_variables']

    vars = []  # Holds the argument values for each test id
    ids = []   # Holds the ids for each individual test
    for entry in data['tests']:
        ids.append(entry['id'])

        var_list = []
        for var_name in input_vars:
            if var_name in entry['variables']:
                var_list.append(entry['variables'][var_name])
            else:
                var_list.append(None)

        vars.append(tuple(var_list))

    # E.g.: ['var1', 'var2', 'var3'] -> 'var1, var2, var3'
    input_vars = ', '.join(input_vars)

    return input_vars, vars, ids


# Obtain the inputs for each test from the json file
input_vars, vars, ids = parse_json(json_file_path)


@pytest.mark.parametrize(input_vars, vars, ids=ids)
def test_e2e(monkeypatch, filename, site_id, expected_results):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(plt, 'savefig', do_nothing)

    process_id = 'TestProcess_###'
    resolution = 'HH'

    # Find the output folder
    config = ConfigParser()
    with open('qaqc.cfg') as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    # Removes any output dirs for the same site_id and process_id as above
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Setup logger to save to file
    process_type = 'BASE Generation'
    _log = Logger(True, process_id, site_id, process_type).getLogger(
        'BASE Generation')
    _log.get_log_dir()

    # Setup data file paths
    testdata_path = os.path.join(
        'test', 'testdata', 'diurnal_seasonal_pattern', 'input_files')
    pickle_path = os.path.join(testdata_path, filename.replace('.csv', '.npy'))
    filepath = os.path.join(
        'test', 'testdata',
        'diurnal_seasonal_pattern', 'input_files', filename)

    # Attempt to load from binary .npy file, otherwise re-process data
    d = DataReader()
    try:
        d.data = np.load(pickle_path, allow_pickle=True)

        header_file_path = filepath.replace('.csv', '') + '_headers.txt'
        with open(header_file_path) as infile:
            d.header_as_is = infile.read().split(',')
    except FileNotFoundError:
        # Read csv file into datareader object
        _log_test = Logger().getLogger('read_file')
        _log_test.resetStats()
        d.read_single_file(filepath, _log_test)

        # Write data to a file
        d.data.dump(os.path.join(filepath.replace('.csv', '.npy')))
        outfile = os.path.join(filepath.replace('.csv', '') + '_headers.txt')
        with open(outfile, 'w') as out:
            out.write(','.join(d.header_as_is))

    # Need to set proper data headers to log datareader info
    _log_test = Logger().getLogger('data_headers')
    _log_test.resetStats()
    d._check_data_header(d.header_as_is, _log_test)

    # Get paths for where to save plots
    p = PlotConfig(True)
    plot_dir = p.get_plot_dir_for_run(site_id, process_id)
    ftp_plot_dir = p.get_ftp_plot_dir_for_run(
        site_id, process_id, site_id)

    ds = DiurnalSeasonalPattern(
        site_id, process_id, resolution,
        plot_dir=plot_dir, ftp_plot_dir=ftp_plot_dir)
    ds.hist_dir_path = os.path.join(
        'test', 'testdata', 'diurnal_seasonal_pattern', 'historical_data')
    ds.url_path = ''

    statuses, _ = ds.driver(d)

    # Parse the json for expected statuses
    expected_statuses = expected_results['status_list']

    # Assert the statuses
    assert len(statuses) == len(expected_statuses)
    for status, expected_status in zip(statuses, expected_statuses):
        status.assert_status(expected_status)

    # Ensure the log file generated
    log_dir = os.path.join(output_dir, 'logs')
    assert os.path.exists(log_dir)

    # Only 1 log file should have generated
    assert len(os.listdir(log_dir)) == 1

    # Get the path of the log file generated
    log_file = os.listdir(log_dir)[0]
    log_file_path = os.path.join(log_dir, log_file)

    # Loop through the expected log lines and ensure they exist in the log file
    with open(log_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for log_text in expected_results['logs']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(log_text)) != -1

    # Ensure the summary folder generated
    summary_dir = os.path.join(output_dir, 'output', 'summary')
    assert os.path.exists(summary_dir)

    csv_file = 'diurnal_seasonal_pattern_summary.csv'
    csv_file_path = os.path.join(summary_dir, csv_file)
    assert os.path.exists(csv_file_path)

    # Loop through the expected lines and ensure they exist in the csv file
    with open(csv_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for csv_line in expected_results['csv_summary']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(csv_line)) != -1

    shutil.rmtree(output_dir)
