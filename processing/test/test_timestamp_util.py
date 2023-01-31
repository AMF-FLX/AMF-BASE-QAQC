import pytest
from utils import TimestampUtil, TimestampUtilException

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'


@pytest.fixture
def ts_util():
    ''' Initializes TimestampUtil '''
    return TimestampUtil()


@pytest.fixture
def get_epoch_time():
    ''' Returns ISO timestamp of epoch '''
    return '197001010000'


def test_get_ISO_timestamp_with_invalid_input(ts_util):
    with pytest.raises(TimestampUtilException):
        ts_util.get_ISO_str_timestamp('70')


def test_get_ISO_timestamp_with_invalid_YYYYMM_input(ts_util):
    with pytest.raises(TimestampUtilException):
        ts_util.get_ISO_str_timestamp('197023')


def test_get_ISO_timestamp(ts_util, get_epoch_time):
    assert ts_util.get_ISO_str_timestamp('1970') == get_epoch_time
    assert ts_util.get_ISO_str_timestamp('197001') == get_epoch_time
    assert ts_util.get_ISO_str_timestamp('19700101') == get_epoch_time
    assert ts_util.get_ISO_str_timestamp('1970010100') == get_epoch_time
    assert ts_util.get_ISO_str_timestamp('197001010000') == get_epoch_time
    assert ts_util.get_ISO_str_timestamp('19700101000000') == get_epoch_time


def test_check_scientific_notation(ts_util):
    assert ts_util.check_scientific_notation(ts='201801010000') is False
    assert ts_util.check_scientific_notation(ts='201801010000.000000') is False
    assert ts_util.check_scientific_notation(ts='20180101') is False
    assert ts_util.check_scientific_notation(ts='-9999') is False
    assert ts_util.check_scientific_notation(ts='5.367') is False
    assert ts_util.check_scientific_notation(ts='3.4838E+1') is True
    assert ts_util.check_scientific_notation(ts='3.48E+10') is True
    assert ts_util.check_scientific_notation(ts='-1.4E+10') is False
    # negative numbers are not allowed for timestamps
    assert ts_util.check_scientific_notation(ts='-13.4E+5') is False
    # only a single digit before the decimal is allowed
