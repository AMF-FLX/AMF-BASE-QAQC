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
    # timestamp data is casted to a25
    # because data_reader module used this format
    # to import the data file.
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


def test_check_timestamp_format(caplog):
    timestamp_checker = TimestampChecks()
    check_log_ts_start = Logger().getLogger('timestamp_format_TIMESTAMP_START')
    check_log_ts_end = Logger().getLogger('timestamp_format_TIMESTAMP_END')

    # timestamp error at the beginning of the TIMESTAMP_START
    timestamp_checker.data = np.array(
        [('202000010000', '202001010030'),
         ('202001010030', '202001010100'),
         ('202001010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_START',
                                                 check_log_ts_start)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_start.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202000010000\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured

    # timestamp error in the middle of the TIMESTAMP_START
    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202021010030', '202001010100'),
         ('202001010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_START',
                                                 check_log_ts_start)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_start.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202021010030\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured

    # timestamp error at the end of the TIMESTAMP_START
    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202001010030', '202001010100'),
         ('202021010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_START',
                                                 check_log_ts_start)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_start.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202021010100\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured

    # timestamp error at the beginning of the TIMESTAMP_END
    timestamp_checker.data = np.array(
        [('202001010000', '202021010030'),
         ('202001010030', '202001010100'),
         ('202001010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_END',
                                                 check_log_ts_end)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_end.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202021010030\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured

    # timestamp error in the middle of the TIMESTAMP_END
    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202001010030', '202021010100'),
         ('202001010100', '202001010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_END',
                                                 check_log_ts_end)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_end.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202021010100\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured

    # timestamp error at the end of the TIMESTAMP_END
    timestamp_checker.data = np.array(
        [('202001010000', '202001010030'),
         ('202001010030', '202001010100'),
         ('202001010100', '202021010130')],
        dtype=[('TIMESTAMP_START', 'a25'), ('TIMESTAMP_END', 'a25')])

    caplog.clear()
    with caplog.at_level(logging.INFO):
        timestamp_checker.check_timestamp_format('TIMESTAMP_END',
                                                 check_log_ts_end)
        captured = [rec.message for rec in caplog.records
                    if check_log_ts_end.getName() in rec.name]
        error_msg = ('Fail to cast perceived timestamp '
                     'b\'202021010130\' as datetime with decoding.')
        assert len(captured) == 1
        assert error_msg in captured
