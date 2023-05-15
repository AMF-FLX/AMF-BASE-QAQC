import datetime as dt
import logging
import numpy as np
import pytest

from logger import Logger
from timestamp_checks import TimestampChecks

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'

log = logging.Logger('test')


@pytest.fixture
def ts_check():
    ''' Initializes TimestampChecks '''
    return TimestampChecks()


def test_check_datetime_length(ts_check, capsys):
    len_err_msg_prefix = 'Datetime string length is of length'
    datetime_err_msg_prefix = 'Unable to get length of timestamp string'

    ts_value = '1970'
    ts_len = len(ts_value)
    ts_check._check_datetime_length(ts_value, log)
    captured = capsys.readouterr()
    assert f'{len_err_msg_prefix} {ts_len}.' in captured.err

    ts_value = '197001010000'
    ts_len = len(ts_value)
    ts_check._check_datetime_length(ts_value, log)
    captured = capsys.readouterr()
    assert '' in captured.err and '' in captured.out

    # If length cannot be determined
    ts_value = 197001010000
    ts_check._check_datetime_length(ts_value, log)
    captured = capsys.readouterr()
    assert f'{datetime_err_msg_prefix} {ts_value}.' in captured.err


def test_has_identical_elements(ts_check, capsys):
    test_value = [1, 1, 2]
    ts_check._has_identical_elements(test_value, log)
    captured = capsys.readouterr()
    count = 1
    assert f'Found {count} duplicate timestamps' in captured.err

    test_value = [1, 1, 2, 2, 3]
    ts_check._has_identical_elements(test_value, log)
    captured = capsys.readouterr()
    count = 2
    assert f'Found {count} duplicate timestamps' in captured.err

    test_value = [1, 2, 3]
    result = ts_check._has_identical_elements(test_value, log)
    captured = capsys.readouterr()
    assert result == 0
    assert '' in captured.err and '' in captured.out


def test_check_forward_filled(caplog):
    check_log = Logger().getLogger('ts_filled_forward')
    timestamp_checker = TimestampChecks()
    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202001010030', '202001010100'),
         ('202001010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_forward_filled_timestamps(check_log)
        captured = [rec.message for rec in caplog.records]
        assert len(captured) == 1
        assert 'Forward-filled' not in captured[0]

    caplog.clear()
    timestamp_checker.current_date = dt.datetime.strptime('201912010000',
                                                          '%Y%m%d%H%M')
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_forward_filled_timestamps(check_log)
        captured = [rec.message for rec in caplog.records]
        assert len(captured) == 2
        assert 'Forward-filled' in captured[0]

    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202001010030', '202001010100'),
         ('202001000100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_forward_filled_timestamps(check_log)
        captured = [rec.message for rec in caplog.records]
        assert len(captured) == 2
        assert 'Fail to cast' in captured[0]
