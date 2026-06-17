import pytest
from utils import DataUtil

__author__ = 'Danielle Christianson, Sy-Toan Ngo'
__email__ = 'dschristianson@lbl.gov, sytoanngo@lbl.gov'


@pytest.fixture
def data_util():
    ''' Initializes DataUtil '''
    return DataUtil()


def test_check_invalid_missing_value_format(data_util):
    assert data_util.check_invalid_missing_value_format('-9999') \
        == (False, None)
    assert data_util.check_invalid_missing_value_format('82.45') \
        == (False, None)
    assert data_util.check_invalid_missing_value_format('-99.99') \
        == (False, None)
    assert data_util.check_invalid_missing_value_format('-10000') \
        == (False, None)
    assert data_util.check_invalid_missing_value_format('-999900') \
        == (False, None)
    assert data_util.check_invalid_missing_value_format('-999.9') \
        == (True, 'Only 6 and 9 value')
    assert data_util.check_invalid_missing_value_format('-9999.0') \
        == (True, 'Only 6 and 9 value')
    assert data_util.check_invalid_missing_value_format('-9999.000000') \
        == (True, 'Only 6 and 9 value')
    assert data_util.check_invalid_missing_value_format('-6999') \
        == (True, 'Only 6 and 9 value')
    assert data_util.check_invalid_missing_value_format('-699.99') \
        == (True, 'Only 6 and 9 value')
    assert data_util.check_invalid_missing_value_format('NaN') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('NA') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('Inf') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('-Inf') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('inf') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('-inf') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('nan') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('na') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format(' ') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('  ') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('Infinity') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('-Infinity') \
        == (True, 'Invalid common value')
    assert data_util.check_invalid_missing_value_format('0+100i',
                                                        ['imaginary_value']) \
        == (True, 'Imaginary value')
    assert data_util.check_invalid_missing_value_format('153+7i',
                                                        ['imaginary_value']) \
        == (True, 'Imaginary value')
    assert data_util.check_invalid_missing_value_format('3!',
                                                        ['factorial_value']) \
        == (True, 'Factorial value')
    assert data_util.check_invalid_missing_value_format('10e5',
                                                        ['scientific_value']) \
        == (True, 'Scientific value')


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
