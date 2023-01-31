import json
import mmap
import numpy as np
import numpy.ma as ma
import os
import pytest
import shutil

from configparser import ConfigParser
from data_reader import DataReader
from fp_vars import FPVariables
from logger import Logger
from status import Status
from utils import VarUtil
from variable_coverage import VariableCoverage


__author__ = 'Josh Geden'
__email__ = 'joshgeden10@gmail.com'


# Path to json file with test inputs & expected values for e2e tests
json_file_path = os.path.join(
     'test', 'testdata', 'variable_coverage', 'test_variable_coverage.json')


def mock_FPVariables_init(self):
    self.fp_vars = {'FC': 'dummy', 'TIMESTAMP_END': 'dummy',
                    'TIMESTAMP_START': 'dummy'}


def mock_VariableCoverage_init(self):
    self.encoding = 'ascii'


def do_nothing(self, *args):
    pass


def test_init(monkeypatch):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)

    vc = VariableCoverage()

    # Ensure that the config file was correctly read
    assert 'required' in vc.vars.keys()
    assert isinstance(vc.vars['required'], tuple)

    assert 'encouraged' in vc.vars.keys()
    assert isinstance(vc.vars['encouraged'], tuple)

    assert 'suggested' in vc.vars.keys()
    assert isinstance(vc.vars['suggested'], tuple)

    assert vc.required_min != 0.0
    assert vc.encouraged_min != 0.0

    # Temporarily remove the ONEFLUX_VARIABLES & PHASE_II sections by copying a
    # temporary config file and test that the init fails as expected
    shutil.copy('qaqc.cfg', '_temp_qaqc.cfg')
    test_cfg = os.path.join(
        'test', 'testdata', 'variable_coverage', 'test_qaqc.cfg')
    shutil.copy(test_cfg, 'qaqc.cfg')

    vc = VariableCoverage()
    assert vc.plot_path is None
    assert vc.vars is None

    # Reset the default qaqc.cfg file
    shutil.copy('_temp_qaqc.cfg', 'qaqc.cfg')
    os.remove('_temp_qaqc.cfg')


def test_get_start_end_idx(monkeypatch):
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)

    expected_result = {
        '2011': {
            'start': 0,
            'end': 1,
        },
        '2012': {
            'start': 2,
            'end': 2,
        },
        '2013': {
            'start': 3,
            'end': 4,
        },
        'all_data': {
            'start': 0,
            'end': 4,
        }
    }

    # Mocks how dates are representeted in a DataReader object
    data = np.array(
        [
            (b'201101010000', b'201101010030'),
            (b'201101010030', b'201101010100'),
            (b'201201010000', b'201201010030'),
            (b'201301010000', b'201301010030'),
            (b'201301010030', b'201301010100'),
        ],
        dtype=[('TIMESTAMP_START', 'S25'), ('TIMESTAMP_END', 'S25')]
    )

    actual_result = VariableCoverage()._get_start_end_idx(data)
    assert actual_result == expected_result

    years = list(actual_result.keys())[:-1]
    assert len(years) == 3
    assert 'all_data' not in years


def test_get_days_in_year(monkeypatch):
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)
    assert VariableCoverage()._get_days_in_year('2020') == 366
    assert VariableCoverage()._get_days_in_year('2021') == 365


def test_calculate_coverage(monkeypatch):
    monkeypatch.setattr(DataReader, '__init__',  do_nothing)
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)

    vc = VariableCoverage()
    vc.var_util = VarUtil()

    vc.vars_list = ['CO2', 'CO2_F', 'G_1_1_1', 'G_2_1_1']
    vc.years = ['2011']
    vc.resolution = 'HH'

    vc.annual_idx = {
        '2011': {
            'start': 0,
            'end': 1
        },
        'all_data': {
            'start': 0,
            'end': 1
        },
    }

    vc.data = ma.array([(b'201101010000', b'201101010030', 1, 1, 1, 1),
                        (b'201101010030', b'201101010100', 1, 1, 1, -9999)],
                       mask=[(0, 0, 0, 0, 0, 0), (0, 0, 0, 0, 0, 1)],
                       dtype=[('TIMESTAMP_START', 'S25'),
                              ('TIMESTAMP_END', 'S25'),
                              ('CO2', '<f8'),
                              ('CO2_F', '<f8'),
                              ('G_1_1_1', '<f8'),
                              ('G_2_1_1', '<f8')])

    data_reader = DataReader()
    data_reader.var_util = VarUtil()

    coverage_dict, coverage_yearly, coverage_timestamps = \
        vc.calculate_coverage(data_reader)

    # 365 days with 24 hours a day and two half-hourly timestamps a day
    timestamps_per_year = 365 * 24 * 2

    # CO2, CO_F & G_1_1_1 have 2 valid entries for the year, G_2_1_1 only has 1
    assert np.array_equal(coverage_yearly, np.array([[2 / timestamps_per_year],
                                                     [2 / timestamps_per_year],
                                                     [2 / timestamps_per_year],
                                                     [1 / timestamps_per_year]]
                                                    ))
    assert np.array_equal(coverage_timestamps, np.array([[1], [1], [1],
                                                         [0.5]]))

    assert coverage_dict == {
        'CO2': {
            '2011': 2/timestamps_per_year
        },
        'CO2_F': {
            '2011': 2/timestamps_per_year
        },
        'G': {
            '2011': 2/timestamps_per_year
        }
    }


def test_plots(monkeypatch):
    monkeypatch.setattr(DataReader, '__init__', do_nothing)
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)

    vc = VariableCoverage()

    data_reader = DataReader()
    data_reader.var_util = VarUtil()

    site_id = 'US-CRT'
    process_id = 'TestProcess_###'

    coverage_by_timestamps = np.array([
        [0], [0.48], [0.52], [0.28], [0.99], [0.97], [0.65], [0.49], [0.52],
        [0.99], [0.99], [0.65], [0.52], [1.00], [1.00], [0.65], [0.97], [1.00],
        [0.96], [0.99], [0.99], [1.00], [0.99], [0.88], [0.65], [0.65], [0.65],
        [0.65], [0.65], [0.65], [0.65], [0.99], [0.65], [0.65]])

    coverage_by_year = coverage_by_timestamps

    vars = ['CH4', 'CO2', 'FC', 'FCH4', 'G_1_1_1', 'G_2_1_1', 'H', 'H2O', 'LE',
            'LW_IN', 'LW_OUT', 'MO_LENGTH', 'NEE', 'NETRAD', 'P', 'PA', 'RH',
            'SWC', 'SWP', 'SW_IN', 'SW_OUT', 'TA', 'TS_1_1_1', 'TS_2_1_1',
            'T_SONIC', 'T_SONIC_SIGMA', 'USTAR', 'U_SIGMA', 'V_SIGMA', 'WD',
            'WS', 'WTD', 'W_SIGMA', 'ZL']
    years = ['2011']

    vc.make_plots(data_reader, site_id, process_id, vars, years,
                  coverage_by_year, coverage_by_timestamps)

    plot_path = os.path.join(
        vc.plot_path, site_id, process_id, 'output', 'variable_coverage',
        'US-CRT-Variable_Coverage-by_Year.png')
    assert os.path.exists(plot_path)

    test_plot_path = os.path.join(
        os.getcwd(), 'test', 'testdata', 'variable_coverage',
        'test_plot-by_Year.png'
    )
    assert os.path.exists(test_plot_path)

    with open(plot_path, 'rb') as f:
        img_1 = hash(f.read())
    with open(test_plot_path, 'rb') as f:
        img_2 = hash(f.read())
    assert img_1 == img_2

    plot_path = os.path.join(
        vc.plot_path, site_id, process_id, 'output', 'variable_coverage',
        'US-CRT-Variable_Coverage-by_Reported_Timestamps.png')
    assert os.path.exists(plot_path)

    test_plot_path = os.path.join(
        os.getcwd(), 'test', 'testdata', 'variable_coverage',
        'test_plot-by_Reported_Timestamps.png'
    )
    assert os.path.exists(test_plot_path)

    with open(plot_path, 'rb') as f:
        img_1 = hash(f.read())
    with open(test_plot_path, 'rb') as f:
        img_2 = hash(f.read())
    assert img_1 == img_2


def test_get_status(monkeypatch):
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)

    vc = VariableCoverage()
    qaqc_check = 'Test Variable'
    log_obj = Logger().getLogger('Test-Logger')
    status_msg = 'Test Message'

    status = vc.get_status(qaqc_check, status_msg, log_obj)

    expected_status = Status(status_code=0,
                             qaqc_check='Test Variable',
                             src_logger_name='Test-Logger',
                             status_msg='Test Message')

    assert status == expected_status


def test_below_threshold(monkeypatch):
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)

    vc = VariableCoverage()
    vc.required_min = 0.50
    vc.encouraged_min = 0.50

    coverage_dict = {
        'CO2': {
            '2011': 0.99
        },
        'FC': {
            '2011': 0.01
        },
        'SW_IN': {
            '2011': 0.01,
            '2012': 0.01
        },
        'PPFD_IN': {
            '2011': 0.01,
            '2012': 0.99
        }
    }

    assert vc.below_threshold(
        coverage_dict, 'CO2', 'required', '2011') is False

    assert vc.below_threshold(
        coverage_dict, 'FC', 'required', '2011') is True

    assert vc.below_threshold(
        coverage_dict, ('SW_IN', 'PPFD_IN'), 'required', '2011') is True

    assert vc.below_threshold(
        coverage_dict, ('SW_IN', 'PPFD_IN'), 'required', '2012') is False


def test_is_missing(monkeypatch):
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)

    vc = VariableCoverage()
    coverage_dict = {
        'CO2': {
            '2011': 0.99
        },
        'SW_IN': {
            '2011': 0.99
        }
    }

    assert vc.is_missing('CO2', coverage_dict) is False
    assert vc.is_missing(('SW_IN', 'PPFD_IN'), coverage_dict) is False

    assert vc.is_missing('USTAR', coverage_dict) is True
    assert vc.is_missing(('A', 'B'), coverage_dict) is True


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
def test_e2e(monkeypatch, required_vars, required_min,
             encouraged_vars, encouraged_min,
             suggested_vars, expected_results):

    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    monkeypatch.setattr(VariableCoverage, '__init__',
                        mock_VariableCoverage_init)
    monkeypatch.setattr(VariableCoverage, 'make_plots', do_nothing)

    if required_vars is None:
        required_vars = []
    if required_min is None:
        required_min = 0.0
    if encouraged_vars is None:
        encouraged_vars = []
    if encouraged_min is None:
        encouraged_min = 0.0
    if suggested_vars is None:
        suggested_vars = []

    site_id = 'US-CRT'
    process_id = 'TestProcess_###'
    resolution = 'HH'
    process_type = 'BASE Generation'

    config = ConfigParser()
    with open('qaqc.cfg') as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    _log = Logger(True, process_id, site_id, process_type).getLogger(
        'BASE Generation')
    _log.get_log_dir()

    filepath = os.path.join(
        'test', 'testdata', 'variable_coverage',
        'US-CRT_HH_201101010000_201301010000_TestDataVariableCoverage00001.csv'
    )
    pickle_path = filepath.replace('.csv', '.npy')

    # Populate the data_reader with data and headers
    d = DataReader()
    # Attempt to load from binary .npy file, otherwise re-process data
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

    vc = VariableCoverage()
    vc.var_util = VarUtil()
    # Format the var tuples from json lists to python tuples
    vc.vars = {
        'required': tuple(tuple(x) if isinstance(x, list) else x
                          for x in required_vars),
        'encouraged': tuple(tuple(x) if isinstance(x, list) else x
                            for x in encouraged_vars),
        'suggested': tuple(tuple(x) if isinstance(x, list) else x
                           for x in suggested_vars),
    }
    vc.required_min = required_min
    vc.encouraged_min = encouraged_min
    statuses = vc.driver(d, site_id, process_id, resolution)

    # Ensure the log file generated
    log_dir = os.path.join(output_dir, 'logs')
    assert os.path.exists(log_dir)
    log_file = os.listdir(log_dir)[-1]
    log_file_path = os.path.join(log_dir, log_file)

    # Loop through the expected log lines and ensure they exist in the log file
    with open(log_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for log_text in expected_results['logs']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(log_text)) != -1
    os.remove(log_file_path)

    # Create a list of status objects from the json file
    expected_statuses = build_statuses(expected_results['statuses'])

    for stat, expected_stat in zip(statuses, expected_statuses):
        stat.assert_status(expected_stat, ignore_ok_status=True)


def build_statuses(statuses_dict):
    """ Converts the json "tests" object into a dict of Status object fields
        to be used for e2e testing """

    # Dict of the 5 types of status objects to be returned
    qaqc_checks = {
        'required_missing':
            'variable_coverage-required_variables_missing',
        'required_below_threshold':
            'variable_coverage-required_below_threshold',
        'encouraged_missing':
            'variable_coverage-encouraged_variables_missing',
        'encouraged_below_threshold':
            'variable_coverage-encouraged_below_threshold',
        'suggested_missing':
            'variable_coverage-suggested_variables_missing'
    }

    statuses = []

    for key in qaqc_checks:
        log_obj = Logger().getLogger(qaqc_checks[key])
        log_obj.resetStats()

        # Check if the key has been specified in the json file
        # That means we are expecting errors from that specific test
        if key in statuses_dict:
            status_msg = None
            if 'status_msg' in statuses_dict[key]:
                status_msg = statuses_dict[key]['status_msg']

            # Create any sub_status objects
            if 'sub_status' in statuses_dict[key]:
                sub_status = {}

                for var in statuses_dict[key]['sub_status']:
                    msg = statuses_dict[key]['sub_status'][var]['status_msg']
                    n_warn = statuses_dict[key]['sub_status'][var]['n_warning']

                    sub_status[var] = {
                        'status_code': -1,
                        'qaqc_check': var,
                        'src_logger_name': f'{qaqc_checks[key]}-{var}',
                        'n_warning': n_warn,
                        'n_error': 0,
                        'status_msg': msg,
                        'plot_paths': None,
                        'sub_status': None,
                        'report_type': 'single_msg',
                        'report_section': 'table',
                    }
            else:
                log_obj.warning_count = statuses_dict[key]['n_warning']
                sub_status = None

            statuses.append({
                'status_code': -1,
                'qaqc_check': qaqc_checks[key],
                'src_logger_name': qaqc_checks[key],
                'n_warning': statuses_dict[key]['n_warning'],
                'n_error': 0,
                'status_msg': status_msg,
                'plot_paths': None,
                'sub_status': sub_status,
                'report_type': 'single_msg',
                'report_section': 'table',
            })
        # If the key is not in the json file we don't expect any issues, create
        # default Status fields
        else:
            statuses.append({
                'status_code': 0,
                'qaqc_check': qaqc_checks[key],
                'src_logger_name': qaqc_checks[key],
                'n_warning': 0,
                'n_error': 0,
                'status_msg': None,
                'plot_paths': None,
                'sub_status': None,
                'report_type': 'single_msg',
                'report_section': 'table',
            })

    return statuses
