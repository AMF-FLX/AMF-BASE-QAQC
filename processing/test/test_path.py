import pytest
import logging

from data_reader import DataReader
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from pathlib import PureWindowsPath, PurePosixPath
from messages import Messages

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

log = logging.Logger('test')


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'FC': 'dummy',
            'TIMESTAMP_END': 'dummy', 'TIMESTAMP_START': 'dummy'}


def mock_FPVariables_load_fp_vars_dict(dummyself):
    return {}


def mock_read_single_file(self, file_path, check_log, usemask=True,
                          missing_values='-9999',
                          datatype=None, usecols=None):
    check_log.info(f'Reading in single file with filename {self.filename}')
    return []


def mock__check_timestamp_header(self, header, check_log):
    return []


def mock__check_all_headers_quotes(self, header_as_is, check_log):
    return []


def mock__check_data_header(self, header_as_is, check_log):
    return []


def mock__check_any_valid_header(self, check_log):
    return []


def mock__check_mandatory_data_headers(self, check_log):
    return []


@pytest.fixture
def data_reader(monkeypatch):
    monkeypatch.setattr(FPVariables, '_load_fp_vars_dict',
                        mock_FPVariables_load_fp_vars_dict)
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)
    monkeypatch.setattr(DataReader, 'read_single_file',
                        mock_read_single_file)
    monkeypatch.setattr(DataReader, '_check_timestamp_header',
                        mock__check_timestamp_header)
    monkeypatch.setattr(DataReader, '_check_all_headers_quotes',
                        mock__check_all_headers_quotes)
    monkeypatch.setattr(DataReader, '_check_data_header',
                        mock__check_data_header)
    monkeypatch.setattr(DataReader, '_check_any_valid_header',
                        mock__check_any_valid_header)
    monkeypatch.setattr(DataReader, '_check_mandatory_data_headers',
                        mock__check_mandatory_data_headers)
    ff = DataReader(test_mode=True)
    return ff


def test_data_reader(data_reader, caplog):
    test_path = PureWindowsPath('D:\\test\\test_file.csv')
    caplog.clear()
    with caplog.at_level(logging.INFO):
        data_reader.driver(test_path, 'o')
        captured = [rec.message for rec in caplog.records]
        assert '\\' not in captured[0] or '/' not in captured[0]
        assert '\\' not in captured[1] or '/' not in captured[1]

    test_path = PurePosixPath('/home/test/test_file.csv')
    caplog.clear()
    with caplog.at_level(logging.INFO):
        data_reader.driver(test_path, 'o')
        captured = [rec.message for rec in caplog.records]
        assert '\\' not in captured[0] or '/' not in captured[0]
        assert '\\' not in captured[1] or '/' not in captured[1]


def mock_messages_init(dummyself):
    dummyself.msgs = []
    dummyself.checknames = {}


@pytest.fixture
def file_name_verifier(monkeypatch):

    monkeypatch.setattr(Messages, '__init__', mock_messages_init)

    return FileNameVerifier()


def test_file_name_verifier(file_name_verifier, caplog):
    test_path = PureWindowsPath('D:\\test\\test_file.csv')
    caplog.clear()
    with caplog.at_level(logging.INFO):
        file_name_verifier.driver(test_path)
        captured = [rec.message for rec in caplog.records]
        assert '\\' not in captured[0] or '/' not in captured[0]
        assert '\\' not in captured[1] or '/' not in captured[1]

    test_path = PurePosixPath('/home/test/test_file.csv')
    caplog.clear()
    with caplog.at_level(logging.INFO):
        file_name_verifier.driver(test_path)
        captured = [rec.message for rec in caplog.records]
        assert '\\' not in captured[0] or '/' not in captured[0]
        assert '\\' not in captured[1] or '/' not in captured[1]
