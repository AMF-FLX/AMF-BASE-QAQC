import json
import math
import matplotlib.pyplot as plt
import mmap
import numpy as np
import os
import pytest
import shutil
import sys

from configparser import ConfigParser
from data_reader import DataReader
from fp_vars import FPVariables
from logger import Logger
from multivariate_comparison import MultivariateComparison
from plot_config import PlotConfig
from status import Status, StatusCode, StatusGenerator

__author__ = 'You-Wei Cheah', 'Josh Geden, Sy-Toan Ngo'
__email__ = 'ycheah@lbl.gov', 'joshgeden10@gmail.com, sytoanngo@lbl.gov'


def do_nothing(*args, **kwargs):
    return


def mock_fit_plot(*args, **kwargs):
    return 'fit_plot'


def mock_composite_plot(*args, **kwargs):
    year = args[10]
    return f'composite_plot-{year}'


def mock_outlier_analysis(*args, **kwargs):
    return [(10, 10, b'201101010000')]


def mock_FPVariables_init(self):
    self.fp_vars = {
        'TIMESTAMP_END': 'dummy',
        'TIMESTAMP_START': 'dummy',
    }


@pytest.fixture
def mc(monkeypatch):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    return MultivariateComparison('', '')


def test_init(monkeypatch):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)

    m = MultivariateComparison('US-CRT', 'TestProcess_###')
    assert m.can_plot is False
    assert m.plot_dir is None
    assert m.url_path is None

    # Temporarily remove the ONEFLUX_VARIABLES & PHASE_II sections by copying a
    # temporary config file and test that the init fails as expected
    shutil.copy('qaqc.cfg', '_temp_qaqc.cfg')
    test_cfg = os.path.join(
        'test', 'testdata', 'multivariate_comparison', 'test_qaqc.cfg')
    shutil.copy(test_cfg, 'qaqc.cfg')

    m = MultivariateComparison(
        'US-CRT', 'TestProcess_###',
        plot_dir='output/US-CRT/TestProcess_###/output',
        ftp_plot_dir='/US-CRT/TestProcess_###/')
    assert m.site_id == 'US-CRT'
    assert m.process_id == 'TestProcess_###'
    assert m.can_plot is True
    assert m.plot_dir == ('output/US-CRT/TestProcess_###/output/'
                          'multivariate_comparison')
    assert m.base_plot_dir == 'output/US-CRT/TestProcess_###/output'
    assert m.plot_path == 'output'

    assert m.ppfd_in_sw_in_threshold == 4.5
    assert m.ppfd_in_sw_in_lo_threshold == 0.7
    assert m.ppfd_in_sw_in_up_threshold == 1.0
    assert m.ppfd_in_sw_in_delta_s_warning == 0.1
    assert m.ppfd_in_sw_in_delta_s_error == 0.2

    assert m.ta_t_sonic_threshold == 4.5
    assert m.ta_t_sonic_lo_threshold == 0.7
    assert m.ta_t_sonic_up_threshold == 1.0
    assert m.ta_t_sonic_delta_s_warning == 0.1
    assert m.ta_t_sonic_delta_s_error == 0.2

    assert m.ws_ustar_threshold == 4.5
    assert m.ws_ustar_lo_threshold == 0.5
    assert m.ws_ustar_up_threshold == 1.0
    assert m.ws_ustar_delta_s_warning == 0.1
    assert m.ws_ustar_delta_s_error == 0.2

    assert m.ta_rep_threshold == 4.5
    assert m.ta_level_threshold == 6

    assert m.slope_deviation_warning == 0.1
    assert m.slope_deviation_error == 0.2

    assert m.outlier_warning == 0.01

    assert m.years_required_for_deviation_check == 3

    # Reset the default qaqc.cfg file
    shutil.copy('_temp_qaqc.cfg', 'qaqc.cfg')
    os.remove('_temp_qaqc.cfg')


def test_store_analysis(mc):
    assert mc._analysis == {}

    mc.store_analysis('fit1', 1, 'x', 'y', b'201101010000', b'201201010000')
    assert mc._analysis == {
        ('x', 'y'): {
            (b'201101010000', b'201201010000'): ('fit1', 1)
        }
    }

    mc.store_analysis('fit2', 2, 'x', 'y', b'201201010000', b'201301010000')
    assert mc._analysis == {
        ('x', 'y'): {
            (b'201101010000', b'201201010000'): ('fit1', 1),
            (b'201201010000', b'201301010000'): ('fit2', 2)
        }
    }


def test_find_vars_using_base_var(mc):
    mc.input_data = np.array(
        [
            (b'201101010000', b'201101010030', 1, 1, 1, 1, 1, 1, 1, 1, 1),
        ],
        dtype=[('TIMESTAMP_START', 'S25'),
               ('TIMESTAMP_END', 'S25'),
               ('TA_1_1_1', '<f8'),
               ('TA_1_1_2', '<f8'),
               ('TA_1', '<f8'),
               ('TA_F_1_1_1', '<f8'),
               ('TA_2_1_1', '<f8'),
               ('T_SONIC_1_1_1', '<f8'),
               ('WS', '<f8'),
               ('WS_F', '<f8'),
               ('PPFD_IN_2_1_1', '<f8')])

    mc.d = DataReader()

    assert mc.find_vars_using_base_var('TA') == \
        ['TA_1_1_1', 'TA_1_1_2', 'TA_1', 'TA_2_1_1']
    assert mc.find_vars_using_base_var('T_SONIC') == ['T_SONIC_1_1_1']
    assert mc.find_vars_using_base_var('WS') == ['WS']
    assert mc.find_vars_using_base_var('SW_IN') == []
    assert mc.find_vars_using_base_var('PPFD_IN') == ['PPFD_IN_2_1_1']


def test_get_start_end_idxs(mc):
    idxs = [0, 3, 10]
    vals = [x for x in range(20)]

    expected = [(0, 3), (3, 10), (10, 20)]
    actual = mc._get_start_end_idxs(idxs, vals)

    assert expected == actual


def test_gen_warning_status(mc):
    assert mc.statuses == []

    log_obj = Logger().getLogger('TestLogger')
    status_msg = 'Test error'

    mc.gen_warning_status(log_obj, log_obj.getName(), status_msg)

    assert len(mc.statuses) == 1
    assert mc.statuses[0].get_status_code() == StatusCode.WARNING
    assert mc.statuses[0].get_src_logger_name() == log_obj.getName()
    assert mc.statuses[0].get_qaqc_check() == log_obj.getName()


def test_compute_sum_of_squares(mc):
    assert mc.compute_sum_of_squares([], []) == (-1, -1)
    assert mc.compute_sum_of_squares([0], [0]) == (0, 0)
    assert mc.compute_sum_of_squares(['a'], ['b']) == (-1, -1)
    assert mc.compute_sum_of_squares([0, 0, 1, 1], [1]) == (1, 0)


def test_gen_missing_variable_status(mc):
    assert mc.statuses == []

    log_obj = Logger().getLogger('Test_Missing_Var')
    mc.gen_missing_variable_status(
        log_obj=log_obj,
        var_1_ls=[],
        var_1='A',
        var_2_ls=[],
        var_2='B'
    )

    assert len(mc.statuses) == 1
    assert mc.statuses[0].get_status_code() == StatusCode.OK
    assert mc.statuses[0].get_qaqc_check() == log_obj.getName()
    assert mc.statuses[0].get_src_logger_name() == log_obj.getName()
    assert mc.statuses[0].get_warning_count() == 0
    assert mc.statuses[0].get_error_count() == 0

    status_msg = ('A and B variables are not present. A-B comparison '
                  'not performed.')
    assert mc.statuses[0].get_status_msg() == status_msg

    mc.gen_missing_variable_status(
        log_obj=log_obj,
        var_1_ls=[],
        var_1='C',
        var_2_ls=['D'],
        var_2='D'
    )

    assert len(mc.statuses) == 2
    assert mc.statuses[1].get_status_code() == StatusCode.OK
    assert mc.statuses[1].get_qaqc_check() == log_obj.getName()
    assert mc.statuses[1].get_src_logger_name() == log_obj.getName()
    assert mc.statuses[1].get_warning_count() == 0
    assert mc.statuses[1].get_error_count() == 0

    status_msg = 'C variable is not present. C-D comparison not performed.'
    assert mc.statuses[1].get_status_msg() == status_msg

    mc.gen_missing_variable_status(
        log_obj=log_obj,
        var_1_ls=['E'],
        var_1='E',
        var_2_ls=[],
        var_2='F'
    )

    assert len(mc.statuses) == 3
    assert mc.statuses[2].get_status_code() == StatusCode.OK
    assert mc.statuses[2].get_qaqc_check() == log_obj.getName()
    assert mc.statuses[2].get_src_logger_name() == log_obj.getName()
    assert mc.statuses[2].get_warning_count() == 0
    assert mc.statuses[2].get_error_count() == 0

    status_msg = 'F variable is not present. E-F comparison not performed.'
    assert mc.statuses[2].get_status_msg() == status_msg


def test_fit_odr(mc):
    x = [1, 2, 3]
    y = [-1, -2, -3]

    fit = mc.fit_odr(x, y)

    # The slope should be exactly -1
    assert math.isclose(fit.beta[0], -1, rel_tol=1e-15)

    # r2 should be exactly 1 because it's linear
    _, ss_total_y = mc.compute_sum_of_squares(x, y)
    r2 = 1 - (fit.sum_square / ss_total_y)
    assert r2 == 1


def test_fit_lin_regression(mc):
    x = [1, 2, 3]
    y = [-1, -2, -3]

    fit = mc.fit_lin_regression(x, y)
    assert fit.slope == -1
    assert fit.rvalue ** 2 == 1


def test_get_vertical_dist_from_regres_ln(mc):
    x = [1, 2, 3]
    y = [-1, -2, -3]
    fit = mc.fit_lin_regression(x, y)
    dist = mc.get_vertical_dist_from_regres_ln(1, 1, fit)
    assert dist - 4 < sys.float_info.epsilon


def test_get_ortho_dist_from_regres_ln(mc):
    x = [1, 2, 3]
    y = [-1, -2, -3]
    fit = mc.fit_odr(x, y)

    # Distance from (1, -1) to (1, 1) is sqrt(2)
    dist = mc.get_ortho_dist_from_regres_ln(1, 1, fit.beta)
    assert math.isclose(dist, math.sqrt(2), rel_tol=1e-15)


def test_find_initial_year_indices(mc):
    timestamps = [b'201101010000',
                  b'2011010100',
                  b'20110101']
    assert mc.find_initial_year_indices(timestamps) == [0, 1, 2]

    timestamps = [b'201101010000',
                  b'201101010030',
                  b'201201010000']
    assert mc.find_initial_year_indices(timestamps) == [0, 2]


def test_create_outlier_status(mc, monkeypatch):
    monkeypatch.setattr(
        MultivariateComparison, 'composite_plotter', mock_composite_plot)
    monkeypatch.setattr(
        MultivariateComparison, 'outlier_analysis', mock_outlier_analysis)

    mc.base_plot_dir = 'output'
    mc.url_path = 'output'
    status, _, _ = mc.create_outlier_status(
        x='TA', y='T_SONIC', year='2011', results=[None]*5,
        outlier_threshold=None, check_label=None, x_vals=None, y_vals=None,
        ts_start_vals=[1, 2, 3]
    )

    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_src_logger_name() == status.get_qaqc_check() == \
        'multivariate_comparison-2011-TA:T_SONIC-outlier_check'
    assert status.get_status_msg() == '1 / 3 (33.33%) are outliers'

    status, _, _ = mc.create_outlier_status(
        x='TA', y='T_SONIC', year='2011', results=[None]*5,
        outlier_threshold=None, check_label=None, x_vals=None, y_vals=None,
        ts_start_vals=[i for i in range(100)]
    )

    assert status.get_status_code() == StatusCode.OK
    assert status.get_src_logger_name() == status.get_qaqc_check() == \
        'multivariate_comparison-2011-TA:T_SONIC-outlier_check'
    assert status.get_status_msg() == '1 / 100 (1.0%) are outliers'


def test_create_r2_status(mc, monkeypatch):
    x = [1, 2, 3]
    y = [1, 2, 3]
    fit = mc.fit_odr(x, y)
    _, ss_total_y = mc.compute_sum_of_squares(x, y)

    status, _ = mc.create_r2_status(
        x='TA', y='T_SONIC', year='2011', ss_total_y=ss_total_y, fit=fit,
        lo_threshold=0.7, up_threshold=1.0, fig_loc='test.png')

    assert status.get_status_code() == StatusCode.ERROR
    assert status.get_src_logger_name() == status.get_qaqc_check() == \
        'multivariate_comparison-2011-TA:T_SONIC-r2_check'
    assert status.get_status_msg() == 'Calculated R2 1.0 has perfect fit of 1.'

    fit.sum_square = 1
    status, _ = mc.create_r2_status(
        x='TA', y='T_SONIC', year='2011', ss_total_y=ss_total_y, fit=fit,
        lo_threshold=0.7, up_threshold=1.0, fig_loc='test.png')

    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_src_logger_name() == status.get_qaqc_check() == \
        'multivariate_comparison-2011-TA:T_SONIC-r2_check'
    assert status.get_status_msg() == 'Calculated R2 0.5 is less than 0.7'

    fit.sum_square = -1
    status, _ = mc.create_r2_status(
        x='TA', y='T_SONIC', year='2011', ss_total_y=ss_total_y, fit=fit,
        lo_threshold=0.7, up_threshold=1.0, fig_loc='test.png')

    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_src_logger_name() == status.get_qaqc_check() == \
        'multivariate_comparison-2011-TA:T_SONIC-r2_check'
    assert status.get_status_msg() == \
        'Calculated R2 1.5 is greater or equal to 1.0'


def test_create_slope_status(mc):
    # Slope deviation error
    status, _ = mc.create_slope_status(
        x='TA', y='T_SONIC', year='2011', slope=1.3, mean_slope=1.00,
        fig_loc='test.png', delta_s_error=0.2, delta_s_warning=0.1,
        valid_years=3)

    assert status.get_status_code() == StatusCode.ERROR
    assert status.get_status_msg() == ('Slope: 1.3; slope deviation 30.0% '
                                       'is greater than error threshold 20.0%')

    # Slope deviation warning
    status, _ = mc.create_slope_status(
        x='TA', y='T_SONIC', year='2011', slope=1.15, mean_slope=1.00,
        fig_loc='test.png', delta_s_error=0.2, delta_s_warning=0.1,
        valid_years=3)

    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_status_msg() == ('Slope: 1.15; slope deviation 15.0% '
                                       'is greater than warning threshold '
                                       '10.0%')

    # Slope deviation ok
    status, _ = mc.create_slope_status(
        x='TA', y='T_SONIC', year='2011', slope=1.05, mean_slope=1.00,
        fig_loc='test.png', delta_s_error=0.2, delta_s_warning=0.1,
        valid_years=3)

    assert status.get_status_code() == StatusCode.OK
    assert status.get_status_msg() == 'Slope: 1.05; slope deviation: 5.0%'


def test_classify_outliers(mc):
    x = [1, 2, 3]
    y = [1, 2, 3]
    fit = mc.fit_odr(x, y)
    rse = math.sqrt(abs(fit.res_var))
    assert mc.classify_outliers(x=10, y=15, fit=fit, rse=rse)
    assert not mc.classify_outliers(x=10, y=10, fit=fit, rse=rse)


def test_odr_linear_function(mc):
    b, x = (1, 2), 1
    assert mc.odr_linear_function(b, x) == 1 * 1 + 2

    b, x = (5, 2), 8
    assert mc.odr_linear_function(b, x) == 5 * 8 + 2


def test_lin_reg_linear_function(mc):
    # y = 2x + 1
    x = [1, 2, 3]
    y = [3, 5, 7]
    fit = mc.fit_lin_regression(x, y)

    assert mc.lin_reg_linear_function(fit, x=5) == 2 * 5 + 1


def test_add_result_summary_stat(mc):
    # All good
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.OK,
        r2_status=StatusCode.OK,
        slope_status=StatusCode.OK)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.OK

    # R2 == 1
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.OK,
        r2_status=StatusCode.ERROR,
        slope_status=StatusCode.OK)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.ERROR

    # delta_s > error_threshold
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.OK,
        r2_status=StatusCode.OK,
        slope_status=StatusCode.ERROR)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.ERROR

    # R2 < threshold
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.OK,
        r2_status=StatusCode.WARNING,
        slope_status=StatusCode.OK)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.WARNING

    # outlier_percent > threshold
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.WARNING,
        r2_status=StatusCode.OK,
        slope_status=StatusCode.OK)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.WARNING

    # warning_threshold < delta_s < error_threshold
    var_status, year_status = create_test_stats(
        outlier_status=StatusCode.OK,
        r2_status=StatusCode.OK,
        slope_status=StatusCode.WARNING)
    mc.statuses = [var_status]
    mc.add_result_summary_stat()
    assert year_status.get_summary_stat('result') == StatusCode.WARNING


def create_test_stats(outlier_status, r2_status, slope_status):
    """ Builds nested Status objects for use in
        test_add_result_summary_stat """

    qaqc_check = 'multivariate_comparison-2011-TA:T_SONIC-outlier_check'
    outlier_status = Status(
        status_code=outlier_status,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if outlier_status == StatusCode.WARNING else 0)

    qaqc_check = 'multivariate_comparison-2011-TA:T_SONIC-r2_check'
    r2_status = Status(
        status_code=r2_status,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if r2_status == StatusCode.WARNING else 0,
        n_error=1 if r2_status == StatusCode.ERROR else 0)

    qaqc_check = 'multivariate_comparison-2011-TA:T_SONIC-slope_check'
    slope_status = Status(
        status_code=slope_status,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if slope_status == StatusCode.WARNING else 0,
        n_error=1 if slope_status == StatusCode.ERROR else 0)

    qaqc_check = 'multivariate_comparison-2011-TA:T_SONIC'
    log_obj = Logger().getLogger(qaqc_check)
    year_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            outlier_status.get_qaqc_check(): outlier_status,
            r2_status.get_qaqc_check(): r2_status,
            slope_status.get_qaqc_check(): slope_status
        }
    )

    qaqc_check = 'multivariate_comparison-TA:T_SONIC'
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


# This function is called from individual pytest files found in the
# multivariate_e2e_tests folder.
# These tests are seperated to help with load balancing when running on
# circleCI.
def e2e(monkeypatch, test_id):
    # Path to json file with test inputs & expected values for e2e tests
    json_file_path = os.path.join(
        'test', 'testdata', 'multivariate_comparison', 'expected_json_files',
        f'test_multivariate_comparison_{test_id}.json')

    # Obtain the inputs for each test from the json file
    _, vars, _ = parse_json(json_file_path)
    filename, site_id, expected_results = vars[0]

    # Disable web service calls and plotting
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(
        MultivariateComparison, 'composite_plotter', mock_composite_plot)
    monkeypatch.setattr(
        MultivariateComparison, 'fit_plotter', mock_fit_plot)
    monkeypatch.setattr(
        plt, 'savefig', do_nothing)

    process_id = 'TestProcess_###'

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
        'test', 'testdata', 'multivariate_comparison', 'input_files')
    filepath = os.path.join(testdata_path, filename)
    pickle_path = os.path.join(testdata_path, filename.replace('.csv', '.npy'))

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

    statuses, _ = MultivariateComparison(
            site_id, process_id, plot_dir, ftp_plot_dir).driver(d)

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

    csv_file = 'multivariate_comparison_summary.csv'
    csv_file_path = os.path.join(summary_dir, csv_file)
    assert os.path.exists(csv_file_path)

    # Loop through the expected lines and ensure they exist in the csv file
    with open(csv_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for csv_line in expected_results['csv_summary']:
            # The figure filenames are switched between figure 1 and 2
            csv_line_swap_figures = csv_line.split(',')
            csv_line_swap_figures[-1], csv_line_swap_figures[-2] = \
                csv_line_swap_figures[-2], csv_line_swap_figures[-1]
            csv_line_swap_figures = ','.join(csv_line_swap_figures)

            # Ensure log file contains log_text
            assert stream.find(str.encode(csv_line)) != -1 or \
                   stream.find(str.encode(csv_line_swap_figures)) != -1

    shutil.rmtree(output_dir)
