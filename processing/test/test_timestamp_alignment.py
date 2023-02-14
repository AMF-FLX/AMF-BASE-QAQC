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
from status import Status, StatusCode, StatusGenerator
from sw_in_pot_gen import SW_IN_POT_Generator
from timestamp_alignment import TimestampAlignment
from typing import Tuple


__author__ = 'Josh Geden'
__email__ = 'joshgeden10@gmail.com'

# Relative path to json file with test inputs & expected values
json_file_path = os.path.join(
    '.', 'test', 'testdata', 'timestamp_alignment',
    'test_timestamp_alignment.json')

# Map used to index the Status._status field
# See status.py for how the Status._status field is created & indexed
status_index_lookup = {
    'status_code': 0,
    'n_warning': 3,
    'n_error': 4,
    'status_msg': 5,
    'n_plot_paths': 6,
    'sub_status': 7,
    'report_type': 8,
    'report_section': 9,
}


def mock_FPVariables_init(dummyself):
    return


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'FC': 'dummy',
            'TIMESTAMP_END': 'dummy', 'TIMESTAMP_START': 'dummy'}


def mock_SW_IN_POT_Generator_get_site_attrs(dummyself, site_id):
    if site_id == 'US-CRT':
        return ('41.6285', '-83.3471', '-5')
    else:
        raise Exception(f'get_site_attrs not defined for {site_id}')


def test_add_result_summary_stat(monkeypatch):
    """ Tests the code generated for the result column for summary_stats"""

    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    ts = TimestampAlignment()

    # All good
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.OK,
        night_code=StatusCode.OK)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.OK

    # max(abs(R_xy)) > threshold is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.WARNING, day_code=StatusCode.OK,
        night_code=StatusCode.OK)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.WARNING

    # 0 < P_day < threshold is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.WARNING,
        night_code=StatusCode.OK)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.WARNING

    # 0 < P_night < threshold is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.OK,
        night_code=StatusCode.WARNING)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.WARNING

    # P_day > threshold is ERROR
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.ERROR,
        night_code=StatusCode.OK)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.ERROR

    # P_night > threshold is ERROR
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.OK,
        night_code=StatusCode.ERROR)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.ERROR

    # R_xy > threshold && P_day > threshold is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.WARNING, day_code=StatusCode.WARNING,
        night_code=StatusCode.OK)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.ERROR

    # R_xy > threshold && P_night > threshold is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.WARNING, day_code=StatusCode.OK,
        night_code=StatusCode.WARNING)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.ERROR

    # warning for P_day && warning for P_night is WARNING
    year_status, var_status = create_test_stats(
        corr_code=StatusCode.OK, day_code=StatusCode.WARNING,
        night_code=StatusCode.WARNING)
    ts.add_result_summary_stat([year_status])
    assert var_status.get_summary_stat('result') == StatusCode.WARNING


def create_test_stats(corr_code, day_code, night_code):
    """ Builds nested Status objects for use in
        test_add_result_summary_stat """

    qaqc_check = 'timestamp_alignment-2011-SW_IN-ccorr'
    corr_status = Status(
        status_code=corr_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if corr_code == StatusCode.WARNING else 0,
        n_error=1 if corr_code == StatusCode.ERROR else 0
    )

    qaqc_check = 'timestamp_alignment-2011-SW_IN-daystats'
    day_status = Status(
        status_code=day_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if day_code == StatusCode.WARNING else 0,
        n_error=1 if day_code == StatusCode.ERROR else 0
    )

    qaqc_check = 'timestamp_alignment-2011-SW_IN-nightstats'
    night_status = Status(
        status_code=night_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if night_code == StatusCode.WARNING else 0,
        n_error=1 if night_code == StatusCode.ERROR else 0
    )

    qaqc_check = 'timestamp_alignment-2011-SW_IN'
    log_obj = Logger().getLogger(qaqc_check)
    var_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            corr_status.get_qaqc_check(): corr_status,
            day_status.get_qaqc_check(): day_status,
            night_status.get_qaqc_check(): night_status
        }
    )

    qaqc_check = 'timestamp_alignment-2011'
    log_obj = Logger().getLogger(qaqc_check)
    year_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            var_status.get_qaqc_check(): var_status
        }
    )

    return year_status, var_status


def parse_json(json_file_path: str) -> Tuple[str, list, list]:
    """ Parses the json file containing the test inputs & expected values
    to be formatted for use with pytest.mark.parametrize() """
    with open(json_file_path) as f:
        data = json.load(f)

    # Obtains the arguments that are required to call
    #    test_timestamp_alignment()
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
def test_timestamp_alignment(
        monkeypatch,
        filename: str,
        site_id: str,
        resolution: str,
        rad_vars: list,
        ts_start: str,
        ts_end: str,
        expected_results: dict) -> None:

    """ Runs the timestamp_alignment module using the file specified
        and asserts that it calculates the expected results  """

    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)
    monkeypatch.setattr(SW_IN_POT_Generator, 'get_site_attrs',
                        mock_SW_IN_POT_Generator_get_site_attrs)

    filepath = os.path.join('.', 'test', 'testdata',
                            'timestamp_alignment', filename)
    testdata_path = os.path.join('.', 'test', 'testdata',
                                 'timestamp_alignment')
    process_type = 'BASE Generation'
    process_id = 'TestProcess_TimestampAlignment_###'

    # Get the output_dir based on the config file
    cwd = os.getcwd()
    config = ConfigParser()
    with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    # Removes any output dirs with the same site_id and process_id as above
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    _log = Logger(True, process_id, site_id, process_type).getLogger(
        'BASE Generation')
    _log.get_log_dir()

    # Create necessary objects and vars to run the timestamp_alignment driver
    d = DataReader()

    # Attempt to read data from file rather than computing
    pickle_path = filepath.replace('.csv', '.npy')
    if os.path.exists(pickle_path):
        d.data = np.load(pickle_path, allow_pickle=True)

        header_file_path = filepath.replace('.csv', '') + '_headers.txt'
        with open(header_file_path) as infile:
            d.header_as_is = infile.read().split(',')
    # Otherwise compute the data from the .csv file
    else:
        # Read csv file into datareader object
        _log_test = Logger().getLogger('read_file')
        _log_test.resetStats()
        d.read_single_file(filepath, _log_test)

        gen = SW_IN_POT_Generator()
        # Merge remaining sw_pot_data with rest of data
        d.data = gen.merge_data(d, site_id, resolution, process_id,
                                ts_start, ts_end)

        # Leaving this here in case we need to recompute the files in the
        # future
        # # Write data to a file
        # d.data.dump(os.path.join(testdata_path,
        #                          filename.replace('.csv', '.npy')))
        # outfile = os.path.join(testdata_path,
        #                        filename.replace('.csv', '') + '_headers.txt')
        # with open(outfile, 'w') as out:
        #     out.write(','.join(d.header_as_is))

    # Need to set proper data headers to log datareader info
    _log_test = Logger().getLogger('data_headers')
    _log_test.resetStats()
    d._check_data_header(d.header_as_is, _log_test)

    # Generate remaining potential rad variables and merge with rest of data
    # Only needed for test 008.csv because that is the only test with
    # incomplete data
    if '008.csv' in filename:
        # Attempt to load remaining sw_pot_data from pickle
        rem_pickle = os.path.join(testdata_path, 'rem_pickle_008.npy')
        if os.path.exists(rem_pickle):
            rem_sw_in_pot_data = np.load(rem_pickle, allow_pickle=True)
        else:
            rem_sw_in_pot_data = gen.gen_rem_sw_in_pot_data(
                d, process_id, resolution, site_id, ts_start, ts_end)
    else:
        rem_sw_in_pot_data = None

    # Executes the timestamp_alignment driver and store the statuses returned
    statuses, _ = TimestampAlignment().driver(
        data_reader=d,
        rem_sw_in_data=rem_sw_in_pot_data,
        site_id=site_id,
        resolution=resolution,
        radiation_vars=rad_vars,
        output_dir=output_dir,
        ftp_plot_dir='',
    )

    # Ensure the log file generated
    log_dir = os.path.join(output_dir, 'logs')
    assert os.path.exists(log_dir)
    log_file = os.listdir(log_dir)[0]
    log_file_path = os.path.join(log_dir, log_file)

    # Loop through the expected log lines and ensure they exist in the log file
    with open(log_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for log_text in expected_results['logs']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(log_text)) != -1

    # Ensure the timestamp_alignment driver generated
    #    the expected # of Status objects
    expected_statuses = expected_results['status_list']
    assert len(statuses) == len(expected_statuses)

    # Ensure each Status object's fields match the expected values
    for stat, expected_stat in zip(statuses, expected_statuses):
        stat.assert_status(expected_stat)

    # Ensure the expected figures were generated
    for filename in expected_results['created_files']:
        path = os.path.join(output_dir, 'timestamp_alignment', filename)
        assert os.path.exists(path)

    # Ensure the summary folder generated
    summary_dir = os.path.join(output_dir, 'summary')
    assert os.path.exists(summary_dir)

    csv_file = 'timestamp_alignment_summary.csv'
    csv_file_path = os.path.join(summary_dir, csv_file)
    assert os.path.exists(csv_file_path)

    # Loop through the expected lines and ensure they exist in the csv file
    with open(csv_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for csv_line in expected_results['csv_summary']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(csv_line)) != -1

    shutil.rmtree(output_dir)
