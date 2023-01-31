import numpy as np
import pytest

from data_reader import DataReader
from fp_vars import FPVariables
from gap_fill import GapFilled
from logger import Logger

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def get_test_array(data_list, variable_name, test_mask):
    test_array = np.array(
        [('202001010000', '202001010030', data_list[0][0], data_list[1][0]),
         ('202001010030', '202001010100', data_list[0][1], data_list[1][1]),
         ('202001010100', '202001010130', data_list[0][2], data_list[1][2])],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25'),
               (variable_name[0], 'f8',), (variable_name[1], 'f8',)])
    test_array_masked = np.ma.array(
        test_array, mask=[(0, 0, test_mask[0][0], test_mask[1][0]),
                          (0, 0, test_mask[0][1], test_mask[1][1]),
                          (0, 0, test_mask[0][2], test_mask[1][2])])
    return test_array_masked


def get_loggers():
    sub_log1 = Logger().getLogger('gap_fill')
    sub_log2 = Logger().getLogger('mand_gap_fill')
    sub_log3 = Logger().getLogger('mand_nonfill')
    return sub_log1, sub_log2, sub_log3


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'FC': 'dummy', 'SWC': 'dummy',
            'TIMESTAMP_END': 'dummy', 'TIMESTAMP_START': 'dummy'}


def mock_FPVariables_load_fp_vars_dict(dummyself):
    return {}


def mock_data_reader_get_data_no_gaps_non_mandatory(dummyself):
    return get_test_array(
        [[45.87, 57.25, 37.2], [45.87, 57.25, 37.2]], ['SWC', 'SWC_F'],
        [[0, 0, 0], [0, 0, 0]])


def mock_data_reader_get_data_gaps_non_mandatory(dummyself):
    return get_test_array(
        [[45.87, -9999, -9999], [45.87, 57.25, 37.2]], ['SWC', 'SWC_F'],
        [[0, 1, 1], [0, 0, 0]])


def mock_data_reader_get_data_no_gaps_mandatory(dummyself):
    return get_test_array(
        [[45.87, 57.25, 37.2], [45.87, 57.25, 37.2]], ['FC', 'FC_F'],
        [[0, 0, 0], [0, 0, 0]])


def mock_data_reader_get_data_gaps_mandatory(dummyself):
    return get_test_array(
        [[45.87, -9999, -9999], [45.87, 57.25, 37.2]], ['FC', 'FC_F'],
        [[0, 1, 1], [0, 0, 0]])


def mock_data_reader_get_data_no_unfilled_mandatory(dummyself):
    return get_test_array(
        [[45.87, 57.25, 37.2], [45.87, 57.25, 37.2]], ['SWC_F', 'FC_F'],
        [[0, 0, 0], [0, 0, 0]])


@pytest.mark.parametrize(
    'mock_get_data, expected_results1, expected_results2',
    [(mock_data_reader_get_data_no_gaps_non_mandatory,
     ('SWC', '', None), ('SWC', '', None)),
     (mock_data_reader_get_data_gaps_non_mandatory,
     (None, None, None), (None, None, None)),
     (mock_data_reader_get_data_no_gaps_mandatory,
     ('FC', '', None), ('', 'FC', None)),
     (mock_data_reader_get_data_gaps_mandatory,
     (None, None, None), (None, None, None)),
     (mock_data_reader_get_data_no_unfilled_mandatory,
     (None, None, 'FC_F'), (None, None, 'FC_F'))],
    ids=['no_gaps_non_mandatory',
         'gaps_non_mandatory',
         'no_gaps_mandatory',
         'gaps_mandatory',
         'no_unfilled_mandatory'])
def test_gap_filled(monkeypatch, mock_get_data,
                    expected_results1, expected_results2):

    monkeypatch.setattr(FPVariables, '_load_fp_vars_dict',
                        mock_FPVariables_load_fp_vars_dict)
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)
    monkeypatch.setattr(DataReader, 'get_data', mock_get_data)

    data_reader = DataReader()
    sub_log1, sub_log2, sub_log3 = get_loggers()
    results = GapFilled().gap_fill_detector(
        data_reader, sub_log1, sub_log2, sub_log3, qaqc_mode='format')
    assert results == expected_results1

    results = GapFilled().gap_fill_detector(
        data_reader, sub_log1, sub_log2, sub_log3, qaqc_mode='base')
    assert results == expected_results2
