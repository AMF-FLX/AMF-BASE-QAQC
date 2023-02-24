import json
import mmap
import numpy as np
import os
import pytest
import shutil

from configparser import ConfigParser
from data_reader import DataReader
from fp_vars import FPVariables
from logger import Logger
from plot_config import PlotConfig
from status import Status, StatusCode, StatusGenerator
from physical_range import PhysicalRange, Var
from utils import VarUtil, WSUtil


__author__ = 'Josh Geden'
__email__ = 'joshgeden10@gmail.com'


# Relative path to json file with test inputs & expected values
json_file_path = os.path.join(
     'test', 'testdata', 'physical_range', 'test_physical_range.json')

# Stash the get_content function in case we need to regenerate the limits.json
# file
_log = Logger().getLogger(__name__)
_get_content = WSUtil(_log).get_content


def mock_physical_range_init(self):
    self._range_link = ''
    self.margin = 0.05
    self.var_util = VarUtil()
    self.soft_flag_threshold = 0.01
    self.hard_flag_threshold = 0.001


def mock_physical_range_plot(self, var_obj, year):
    try:
        return self.fig_name_fmt.format(
                y=var_obj.name, year=year, s=self.site_id, p=self.process_id)
    except Exception:
        return 'plot_path'


def mock_datareader_init(self):
    self.var_util = VarUtil()


def mock_get_base_header(self, var):
    vars = {
        'FC': ['FC', 'FC_1_1_1'],
        'PPFD_IN': ['PPFD_IN', 'PPFD_IN_1_1_1']
    }

    for base_var in vars:
        if var in vars[base_var]:
            return base_var

    raise Exception('Unsupported var used.')


def mock_FPVariables_init(self):
    self.fp_vars = {'FC': 'dummy', 'TIMESTAMP_END': 'dummy',
                    'TIMESTAMP_START': 'dummy'}


def get_test_limit_dict():
    """ Returns a sub-sample of the correctly formatted limit_dict """

    return {
        "CO2": {
            "min": 150.0,
            "max": 1200.0,
            "units": "micro-molCO2 mol-1",
            "margin": 0.05
        },
        "COND_WATER": {
            "min": 0.0,
            "max": 10000.0,
            "units": "micro-S cm-1",
            "margin": 0.05
        },
        "G": {
            "min": -250.0,
            "max": 400.0,
            "units": "W m-2",
            "margin": 0.05
        },
        "RH": {
            "min": 0.0,
            "max": 100.0,
            "units": "%",
            "margin": 0.05
        },
        "SB": {
            "min": -float('inf'),
            "max": float('inf'),
            "units": "W m-2",
            "margin": 0.05
        },
        "TIMESTAMP_START": {
            "min": -float('inf'),
            "max": float('inf'),
            "units": "YYYYMMDDHHMM",
            "margin": 0.05
        },
        "USTAR": {
            "min": 0.0,
            "max": 8.0,
            "units": "m s-1",
            "margin": 0.05
        },
        "ZL": {
            'min': -float('inf'),
            'max': float('inf'),
            'units': 'nondimensional',
            'margin': 0.05
        }
    }


def generate_data_file():
    outfile = os.path.join('test', 'testdata', 'physical_range',
                           'test_data_for_plot.npy')

    infile = os.path.join('test', 'testdata', 'physical_range',
                          'US-Rws_HH_201601010000_201710010000-'
                          'TestDataPhysicalRange0000005.csv')

    d = DataReader()

    # Read csv file into datareader object
    _log_test = Logger().getLogger('read_file')
    _log_test.resetStats()
    d.read_single_file(infile, _log_test)

    d.data.dump(outfile)


def mock_get_content(dummy_self, dummy_url):
    """ Returns correctly formmatted var dict that would come from web service
    """

    testfile = os.path.join(
        'test', 'testdata', 'physical_range', 'limits.json')

    # Try to read cached limits
    if os.path.exists(testfile):
        with open(testfile, 'r') as f:
            return f.read()
    # Otherwise get it from the webservice
    else:
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
        url = config.get('WEBSERVICES', 'fp_limits')
        limits = json.loads(_get_content(url))

        with open(testfile, 'w') as f:
            json.dump(limits, f)

        return json.dumps(limits)


def mock_Var_init(self):
    return


@pytest.mark.parametrize(
    'variable, expected_error',
    [
        ('COND_WATER', 500),    # Test usual case
        ('G_1_1_1', 32.5),      # Test case where var_name != base_name
        ('ZL', 5.0),            # Test case where there is no max/min
    ],
    ids=[
        'COND_WATER',
        'G_1_1_1',
        'ZL',
    ]
)
def test_var_init(monkeypatch, variable, expected_error):
    """ Uses a test limit_dict to ensure Var.__init__ works correctly """

    monkeypatch.setattr(DataReader, '__init__', mock_datareader_init)
    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)

    d = DataReader()
    base_name = d.get_base_header(variable)
    limit_dict = get_test_limit_dict()

    var_obj = Var(variable, base_name, limit_dict)

    assert var_obj.error == expected_error
    assert var_obj.max_lim == limit_dict[base_name]['max']
    assert var_obj.min_lim == limit_dict[base_name]['min']
    assert var_obj.units == limit_dict[base_name]['units']
    assert var_obj.margin == limit_dict[base_name]['margin']
    assert var_obj.error_max == var_obj.max_lim + var_obj.error
    assert var_obj.error_min == var_obj.min_lim - var_obj.error


def test_set_limit_dict(monkeypatch):
    """ Tests that Thresholds can generate a correctly formatted limit_dict """
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)
    monkeypatch.setattr(WSUtil, 'get_content', mock_get_content)

    limit_dict = PhysicalRange().set_limit_dict()
    expected_limit_dict = get_test_limit_dict()
    for key in expected_limit_dict:
        assert limit_dict[key] == expected_limit_dict[key]


def test_get_start_end_idx(monkeypatch):
    """ Tests that Thresholds can get start and end idx from an np.array """
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)

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

    actual_result = PhysicalRange()._get_start_end_idx(data)
    assert actual_result == expected_result


def test_outliers(monkeypatch):
    """ Ensure Thresholds can correctly identify errors/warnings outliers """

    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)
    monkeypatch.setattr(Var, '__init__', mock_Var_init)

    var_obj = Var()
    var_obj.error_min = 2   # when to send errors about variable min
    var_obj.min_lim = 4     # when to send warnings about variable min

    var_obj.error_max = 8   # when to send errors about variable max
    var_obj.max_lim = 6     # when to send warnings about variable max

    x_data = y_data = [x for x in range(11)]

    out_error, out_warning = PhysicalRange().identify_outliers(
        x_data, y_data, var_obj)

    assert out_error[0] == out_error[1] == [0, 1, 9, 10]
    assert out_warning[0] == out_warning[1] == [2, 3, 7, 8]


@pytest.mark.parametrize(
    'var_name, n_warnings, n_errors, expected_status',
    [
        ('COND_WATER', 0, 0,
         Status(status_code=StatusCode.OK,
                qaqc_check='TestLogger',
                src_logger_name='TestLogger',
                n_warning=0,
                n_error=0,
                status_msg=None,
                plot_paths=['plot_path']
                )
         ),
        ('COND_WATER', 1, 0,
         Status(status_code=StatusCode.WARNING,
                qaqc_check='TestLogger',
                src_logger_name='TestLogger',
                n_warning=1,
                n_error=0,
                status_msg='1 / 10 outside but within +/-5.0% of limits '
                '(0.0-10000.0 micro-S cm-1)',
                plot_paths=['plot_path']
                )
         ),
        ('COND_WATER', 0, 1,
         Status(status_code=StatusCode.ERROR,
                qaqc_check='TestLogger',
                src_logger_name='TestLogger',
                n_warning=0,
                n_error=1,
                status_msg='1 / 10 outside of limits '
                '(0.0-10000.0 micro-S cm-1)',
                plot_paths=['plot_path']
                )
         ),
        ('COND_WATER', 1, 1,
         Status(status_code=StatusCode.ERROR,
                qaqc_check='TestLogger',
                src_logger_name='TestLogger',
                n_warning=0,
                n_error=1,
                status_msg='1 / 10 outside and 1 / 10 outside but '
                'within +/-5.0% of limits (0.0-10000.0 micro-S cm-1)',
                plot_paths=['plot_path']
                )
         )
    ],
    ids=['no_warnings_no_errors',
         'one_warning_no_errors',
         'no_warnings_one_error',
         'one_warning_one_error'],
)
def test_get_status(monkeypatch, var_name, n_warnings, n_errors,
                    expected_status):
    """ Ensures Thresholds can correctly generate a status object from a Var
    object """

    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)
    monkeypatch.setattr(PhysicalRange, 'plot', mock_physical_range_plot)

    limit_dict = get_test_limit_dict()
    var_obj = Var(var_name, var_name, limit_dict)

    class TestLogger:
        def __init__(self):
            self.warning_count = 0
            self.error_count = 0
            self.fatal_count = 0

        def getName(self):
            return 'TestLogger'

        def warning(self, msg):
            self.warning_count += 1

        def error(self, msg):
            self.error_count += 1

    status, _ = PhysicalRange().get_status(
        var_obj,
        year='2011',
        n_warnings=n_warnings,
        n_errors=n_errors,
        log_obj=TestLogger(),
        total_count=10
    )

    assert status == expected_status


def test_plot():
    process_id = 'TestProcess_###'
    site_id = 'US-Rws'

    p = PlotConfig(True)
    plot_dir = p.get_plot_dir_for_run(site_id, process_id)
    ftp_plot_dir = p.get_ftp_plot_dir_for_run(
        site_id, process_id, site_id)

    t = PhysicalRange(
        site_id=site_id,
        process_id=process_id,
        plot_dir=plot_dir,
        ftp_plot_dir=ftp_plot_dir
    )

    data_path = os.path.join(
        'test', 'testdata', 'physical_range', 'test_data_for_plot.npy')

    if not os.path.exists(data_path):
        generate_data_file()

    t.data = np.load(data_path, allow_pickle=True)
    t.annual_idx = t._get_start_end_idx(t.data)

    var_obj = Var('RH', 'RH', get_test_limit_dict())

    t.plot(
        var_obj=var_obj,
        year='2016'
    )

    cwd = os.getcwd()
    config = ConfigParser()
    with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    plot_path = os.path.join(
        output_dir, 'output', 'physical_range',
        'US-Rws-TestProcess_###-physical_range-RH-2016.png'
    )

    test_plot_path = os.path.join(
        cwd, 'test', 'testdata', 'physical_range', 'test_plot_RH_2016.png'
    )

    with open(plot_path, 'rb') as f:
        img_1 = hash(f.read())
    with open(test_plot_path, 'rb') as f:
        img_2 = hash(f.read())
    assert img_1 == img_2


def test_add_result_summary_stat(monkeypatch):
    """ Tests the result code generated for summary_stats """

    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, '__init__', mock_physical_range_init)
    monkeypatch.setattr(DataReader, '__init__', mock_datareader_init)
    monkeypatch.setattr(DataReader, 'get_base_header', mock_get_base_header)

    t = PhysicalRange()
    d = DataReader()

    # All good
    year_status, var_status = create_test_stats(
        var='FC',
        is_ratio_code=StatusCode.OK,
        soft_flag_code=StatusCode.OK,
        hard_flag_code=StatusCode.OK
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.OK
    assert var_status.get_summary_stat('outlier_result') == StatusCode.OK

    # Unit error
    year_status, var_status = create_test_stats(
        var='FC_1_1_1',
        is_ratio_code=StatusCode.ERROR,
        soft_flag_code=StatusCode.OK,
        hard_flag_code=StatusCode.OK
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.ERROR
    assert 'outlier_result' not in var_status.get_summary_stats()

    # P_hard_flag > threshold1
    year_status, var_status = create_test_stats(
        var='FC',
        is_ratio_code=StatusCode.OK,
        soft_flag_code=StatusCode.OK,
        hard_flag_code=StatusCode.ERROR
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.OK
    assert var_status.get_summary_stat('outlier_result') == StatusCode.ERROR

    # P_soft_flag > threshold2
    year_status, var_status = create_test_stats(
        var='FC',
        is_ratio_code=StatusCode.OK,
        soft_flag_code=StatusCode.WARNING,
        hard_flag_code=StatusCode.OK
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.OK
    assert var_status.get_summary_stat('outlier_result') == StatusCode.WARNING

    # P_soft_flag > threshold2 but var is one to be ignored
    year_status, var_status = create_test_stats(
        var='PPFD_IN_1_1_1',
        is_ratio_code=StatusCode.OK,
        soft_flag_code=StatusCode.WARNING,
        hard_flag_code=StatusCode.OK
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.OK
    assert var_status.get_summary_stat('outlier_result') == StatusCode.OK

    # P_hard_flag > threshold1 and P_soft_flag > threshold2
    year_status, var_status = create_test_stats(
        var='FC',
        is_ratio_code=StatusCode.OK,
        soft_flag_code=StatusCode.WARNING,
        hard_flag_code=StatusCode.ERROR
    )
    t.add_result_summary_stat([year_status], d)
    assert var_status.get_summary_stat('ratio_result') == StatusCode.OK
    assert var_status.get_summary_stat('outlier_result') == StatusCode.ERROR


def create_test_stats(var, is_ratio_code, soft_flag_code, hard_flag_code):
    """ Builds nested Status objects for use in
        test_add_result_summary_stat """

    qaqc_check = f'physical_range-2011-{var}-unit_check'
    unit_status = Status(
        status_code=is_ratio_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_error=1 if is_ratio_code == StatusCode.ERROR else 0
    )

    qaqc_check = f'physical_range-2011-{var}-outlier_check'
    outlier_status = Status(
        status_code=min(soft_flag_code, hard_flag_code),
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if (soft_flag_code == StatusCode.WARNING or
                        hard_flag_code == StatusCode.WARNING) else 0,
        n_error=1 if (soft_flag_code == StatusCode.ERROR or
                      hard_flag_code == StatusCode.ERROR) else 0
    )

    statuses = {
        unit_status.get_qaqc_check(): unit_status
    }
    if unit_status.get_status_code() == StatusCode.OK:
        statuses[outlier_status.get_qaqc_check()] = outlier_status

    qaqc_check = f'physical_range-2015-{var}'
    year_status = StatusGenerator().composite_status_generator(
        logger=Logger().getLogger(qaqc_check), qaqc_check=qaqc_check,
        statuses=statuses
    )

    qaqc_check = f'physical_range-{var}'
    var_status = StatusGenerator().composite_status_generator(
        logger=Logger().getLogger(qaqc_check), qaqc_check=qaqc_check,
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
            var_list.append(entry['variables'][var_name])

        vars.append(tuple(var_list))

    # E.g.: ['site_id', 'file', 'rad_vars'] -> 'site_id, file, rad_vars'
    input_vars = ', '.join(input_vars)

    return input_vars, vars, ids


# Obtain the inputs for each test from the json file
input_vars, vars, ids = parse_json(json_file_path)


# Executes this test function for each test id assigned above
@pytest.mark.parametrize(input_vars, vars, ids=ids)
def test_e2e(monkeypatch, filename, site_id, expected_results):
    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    monkeypatch.setattr(PhysicalRange, 'plot', mock_physical_range_plot)
    monkeypatch.setattr(WSUtil, 'get_content', mock_get_content)

    process_id = 'TestPhysicalRangeProcess'

    # Get the output_dir based on the config file
    cwd = os.getcwd()
    config = ConfigParser()
    with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    # Removes any output dirs for the same site_id and process_id as above
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    process_type = 'BASE Generation'
    _log = Logger(True, process_id, site_id, process_type).getLogger(
        'BASE Generation')
    _log.get_log_dir()

    # Get paths for where to save plots
    p = PlotConfig(True)
    plot_dir = p.get_plot_dir_for_run(site_id, process_id)
    ftp_plot_dir = p.get_ftp_plot_dir_for_run(
        site_id, process_id, site_id)

    testdata_path = os.path.join('.', 'test', 'testdata', 'physical_range')
    pickle_path = os.path.join(testdata_path, filename.replace('.csv', '.npy'))
    filepath = os.path.join(
        '.', 'test', 'testdata', 'physical_range', filename)

    # Create necessary objects and vars to run the timeshift driver
    d = DataReader()
    if os.path.exists(pickle_path):
        d.data = np.load(pickle_path, allow_pickle=True)

        header_file_path = filepath.replace('.csv', '') + '_headers.txt'
        with open(header_file_path) as infile:
            d.header_as_is = infile.read().split(',')
    else:
        # Read csv file into datareader object
        _log_test = Logger().getLogger('read_file')
        _log_test.resetStats()
        d.read_single_file(filepath, _log_test)

        # Write data to a file
        d.data.dump(os.path.join(testdata_path,
                                 filename.replace('.csv', '.npy')))
        outfile = os.path.join(testdata_path,
                               filename.replace('.csv', '') + '_headers.txt')
        with open(outfile, 'w') as out:
            out.write(','.join(d.header_as_is))

    # Need to set proper data headers to log datareader info
    _log_test = Logger().getLogger('data_headers')
    _log_test.resetStats()
    d._check_data_header(d.header_as_is, _log_test)

    status_list, _ = PhysicalRange(
        site_id=site_id,
        process_id=process_id,
        plot_dir=plot_dir,
        ftp_plot_dir=ftp_plot_dir,
    ).driver(d)

    expected_status_list = expected_results['status_list']
    for stat, expected_stat in zip(status_list, expected_status_list):
        stat.assert_status(expected_stat)

    # Ensure the log file generated
    log_dir = os.path.join(output_dir, 'logs')
    assert os.path.exists(log_dir)

    # Only 1 log file should have generated
    assert len(os.listdir(log_dir)) == 1

    # Get the path of the log file generated
    log_file = os.listdir(log_dir).pop()
    log_file_path = os.path.join(log_dir, log_file)

    # Loop through the expected log lines and ensure they exist in the log file
    with open(log_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for log_text in expected_results['logs']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(log_text)) != -1

    # Ensure the summary folder generated
    summary_dir = os.path.join(plot_dir, 'summary')
    assert os.path.exists(summary_dir)

    for summary_type in ['', 'percent_ratio_']:
        # Ensure the limit/ratio summary file generated
        csv_file = f'physical_range_{summary_type}summary.csv'
        csv_file_path = os.path.join(summary_dir, csv_file)
        assert os.path.exists(csv_file_path)

        # Loop through the expected lines and ensure they exist in the csv file
        with open(csv_file_path) as f:
            stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            for csv_line in expected_results[f'{summary_type}csv_summary']:
                # Ensure log file contains log_text
                assert stream.find(str.encode(csv_line)) != -1

    shutil.rmtree(output_dir)
