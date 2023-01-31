#!/usr/bin/env python

from file_name_verifier import FileNameVerifier
from messages import Messages
from site_attrs import SiteAttributes
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_site_attrs_get_site_dict(dummyvar):
    return {'US-Ton': 'Tonzi'}


def mock_messages_init(dummyvar):
    pass


@pytest.fixture
def file_name_verifier(monkeypatch):

    monkeypatch.setattr(Messages, '__init__', mock_messages_init)

    return FileNameVerifier()


def test_has_csv_ext(file_name_verifier):
    assert file_name_verifier.has_csv_ext('.csv') is True
    assert file_name_verifier.has_csv_ext('.dat') is False
    assert file_name_verifier.has_csv_ext('.xlsx') is False


def test_is_AMF_site_id(file_name_verifier, monkeypatch):

    monkeypatch.setattr(SiteAttributes, 'get_site_dict',
                        mock_site_attrs_get_site_dict)

    assert file_name_verifier.is_AMF_site_id('US-Ton') is True
    assert file_name_verifier.is_AMF_site_id('US_Ton') is False
    assert file_name_verifier.is_AMF_site_id('other') is False


def test_has_no_optional_param(file_name_verifier):

    assert file_name_verifier.has_no_optional_param(None) is True
    assert file_name_verifier.has_no_optional_param('NS') is False
    assert file_name_verifier.has_no_optional_param('v1') is False


def test_set_fname_attrs(monkeypatch):

    monkeypatch.setattr(Messages, '__init__', mock_messages_init)

    fnv = FileNameVerifier()
    fname_pieces = ['US-Ton', 'HH', '201001010000', '201112312330']
    fnv.set_fname_attrs(fname_pieces)
    assert fnv.fname_attrs['site_id'] == fname_pieces[0]
    assert fnv.fname_attrs['resolution'] == fname_pieces[1]
    assert fnv.fname_attrs['ts_start'] == fname_pieces[2]
    assert fnv.fname_attrs['ts_end'] == fname_pieces[3]

    fnv = FileNameVerifier()
    fname_pieces.append('NS')
    fnv.set_fname_attrs(fname_pieces)
    assert fnv.fname_attrs['site_id'] == fname_pieces[0]
    assert fnv.fname_attrs['resolution'] == fname_pieces[1]
    assert fnv.fname_attrs['ts_start'] == fname_pieces[2]
    assert fnv.fname_attrs['ts_end'] == fname_pieces[3]
    assert fnv.fname_attrs['optional'] == fname_pieces[4]


def test_is_valid_resolution(file_name_verifier):
    assert file_name_verifier.is_valid_resolution(
        'HH') is True
    assert file_name_verifier.is_valid_resolution(
        'HR') is True
    assert file_name_verifier.is_valid_resolution(
        'HT') is False


def test_is_valid_timestamp(file_name_verifier):
    assert file_name_verifier.is_valid_timestamp(
        '201001010000', 'ts_start') is True
    assert file_name_verifier.is_valid_timestamp(
        '201001010000', 'ts_end') is True
    assert file_name_verifier.is_valid_timestamp(
        '20100101000', 'ts_start') is False
    assert file_name_verifier.is_valid_timestamp(
        '20100101000A', 'ts_start') is False


def test_check_fname_pieces(file_name_verifier, monkeypatch):
    file_attrs = {'site_id': 'US-Ton', 'resolution': 'HH', 'ext': '.csv',
                  'ts_start': '201001010000', 'ts_end': '201112312330'}

    monkeypatch.setattr(SiteAttributes, 'get_site_dict',
                        mock_site_attrs_get_site_dict)

    assert file_name_verifier.check_fname_pieces(file_attrs) is True

    file_attrs['optional'] = 'NS'
    assert file_name_verifier.check_fname_pieces(file_attrs) is False


def test_is_filename_FPIn_compliant(monkeypatch):

    monkeypatch.setattr(Messages, '__init__', mock_messages_init)

    monkeypatch.setattr(SiteAttributes, 'get_site_dict',
                        mock_site_attrs_get_site_dict)

    filenames = ['US-Ton_HH_201001010000_201112312330_NS-uploadtime.csv',
                 'US-Ton_HH_201001010000_201112312330_v1-uploadtime.csv',
                 'other_HH_201001010000_201112312330-uploadtime.csv',
                 'US_Ton_HH_201001010000_201112312330-uploadtime.csv',
                 'US-Ton_HH_20100101003A_201112312330-uploadtime.csv',
                 'US-Ton_HH_20100101000_201112312300-uploadtime.csv',
                 'US-Ton_H_201001010000_201112312330-uploadtime.csv',
                 'US-Ton_HH__201001010000_201112312330-uploadtime.csv',
                 'US-Ton_HH-201001010000_201112312330-uploadtime.csv',
                 'US-Ton_HH_201001010000_201112312330-uploadtime.csv']
    results = [False, False, False, False, False, False,
               False, False, False, True]

    for filename, result in zip(filenames, results):
        assert FileNameVerifier().is_filename_FPIn_compliant(filename) is \
            result


def test_make_filename(file_name_verifier):
    file_attrs = {'site_id': 'US-Ton', 'resolution': 'HH', 'ext': '.csv',
                  'ts_start': '201001010000', 'ts_end': '201112312330',
                  'ts_upload': 'uploadtime'}
    file_name_verifier.fname_attrs = file_attrs

    assert file_name_verifier.make_filename() == \
        'US-Ton_HH_201001010000_201112312330-uploadtime.csv'


def test_trailing_underscore_filname(monkeypatch):
    monkeypatch.setattr(Messages, '__init__', mock_messages_init)

    monkeypatch.setattr(SiteAttributes, 'get_site_dict',
                        mock_site_attrs_get_site_dict)

    assert FileNameVerifier().is_filename_FPIn_compliant(
        'US-Ton_HH_201001010000_201112312330_-uploadtime.csv') is False
