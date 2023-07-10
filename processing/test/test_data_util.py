import pytest
from utils import DataUtil

__author__ = 'Danielle Christianson, Sy-Toan Ngo'
__email__ = 'dschristianson@lbl.gov, sytoanngo@lbl.gov'


@pytest.fixture
def data_util():
    ''' Initializes DataUtil '''
    return DataUtil()


def test_check_invalid_missing_value_format(data_util):
    assert data_util.check_invalid_missing_value_format('-9999') is False
    assert data_util.check_invalid_missing_value_format('82.45') is False
    assert data_util.check_invalid_missing_value_format('-99.99') is False
    assert data_util.check_invalid_missing_value_format('-10000') is False
    assert data_util.check_invalid_missing_value_format('-999900') is False
    assert data_util.check_invalid_missing_value_format('-999.9') is True
    assert data_util.check_invalid_missing_value_format('-9999.0') is True
    assert data_util.check_invalid_missing_value_format('-9999.000000') is True
    assert data_util.check_invalid_missing_value_format('-6999') is True
    assert data_util.check_invalid_missing_value_format('-699.99') is True
    assert data_util.check_invalid_missing_value_format('NaN') is True
    assert data_util.check_invalid_missing_value_format('NA') is True
    assert data_util.check_invalid_missing_value_format('') is True
    assert data_util.check_invalid_missing_value_format('Inf') is True
    assert data_util.check_invalid_missing_value_format('-Inf') is True
    assert data_util.check_invalid_missing_value_format('inf') is True
    assert data_util.check_invalid_missing_value_format('-inf') is True
    assert data_util.check_invalid_missing_value_format('nan') is True
    assert data_util.check_invalid_missing_value_format('na') is True
    assert data_util.check_invalid_missing_value_format(' ') is True
    assert data_util.check_invalid_missing_value_format('  ') is True
    assert data_util.check_invalid_missing_value_format('Infinity') is True
    assert data_util.check_invalid_missing_value_format('-Infinity') is True
    assert data_util.check_invalid_missing_value_format('0+100i') is True
    assert data_util.check_invalid_missing_value_format('153+7i') is True


def test_check_invalid_data_row(data_util):
    assert data_util.check_invalid_data_row('201801010000') is False
    assert data_util.check_invalid_data_row('201801010000.000000') is False
    assert data_util.check_invalid_data_row('400.32') is False
    assert data_util.check_invalid_data_row('-3.7890') is False
    assert data_util.check_invalid_data_row('-9999') is False
    assert data_util.check_invalid_data_row('') is True
    assert data_util.check_invalid_data_row('YYYYMMDDHHMM') is True
    assert data_util.check_invalid_data_row('TIMESTAMP') is True
    assert data_util.check_invalid_data_row('# Site: US-Ton') is True
    assert data_util.check_invalid_data_row('mol^3/mol^3') is True


def test_are_all_headers_with_quotes(data_util):
    test_vars = [['TIMESTAMP_START', 'TIMESTAMP_END', 'FC', 'SW_IN'],
                 ['TIMESTAMP_START', 'TIMESTAMP_END', 'FC', '"SW_IN"'],
                 ['TIMESTAMP_START', 'TIMESTAMP_END', '"FC"', '"SW_IN"'],
                 ['"TIMESTAMP_START"', '"TIMESTAMP_END"', '"FC"', '"SW_IN"']]
    results = [False, False, False, True]
    for test_var, result in zip(test_vars, results):
        assert data_util.are_all_headers_with_quotes(test_var) is result
