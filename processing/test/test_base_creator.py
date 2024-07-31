from base_creator import BASECreator
from db_handler import NewDBHandler
from process_states import ProcessStateHandler

import json
import pytest


__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'


def mock_process_states_qaqc_process_lookup(
        dummy, initiate_lookup=False):
    with open(file='./test/resources/state_cv_type.json', mode='r') as f:
        return json.load(f)


@pytest.fixture
def base_creator(monkeypatch):
    monkeypatch.setattr(ProcessStateHandler, '_qaqc_process_lookup',
                        mock_process_states_qaqc_process_lookup)

    b = BASECreator()
    if not hasattr(b, 'new_db_handler'):
        b.new_db_handler = NewDBHandler('test', 'test', 'test', 'test')

    if not hasattr(b, 'filename_checksum_lookup'):
        filenamev10_1 = b.BASE_fname_fmt.format(
            sid='US-UMB', res='HH', ver='10-1')
        filenamev10_5 = b.BASE_fname_fmt.format(
            sid='US-UMB', res='HH', ver='10-5')
        b.filename_checksum_lookup = {
            filenamev10_1: '94988170f096e6c6fb280a6bfd3ee075',
            filenamev10_5: '94988170f096e6c6fb280a6bfd3ee075'}
    return b


def test_assign_new_data_version(base_creator, monkeypatch):
    def mock_db_handler_get_input_files(dummyself, dummyconn, process_id):
        if process_id == 1:
            return {123, 234, 345}
        elif process_id == 2:
            return {123, 234, 345}
        elif not process_id:
            pytest.raises(
                Exception,
                message='Failed test: no previous process_ID to check')
        else:
            return {123, 678}

    monkeypatch.setattr(NewDBHandler, 'get_input_files',
                        mock_db_handler_get_input_files)

    assert base_creator.assign_new_data_version(
        conn=None,
        resolution='HH', last_base_version='10-1', last_processID=1,
        is_last_ver_cdiac=False, site_id='US-UMB',
        md5sum='94988170f096e6c6fb280a6bfd3ee075', processID=2) == (
        '10-' + base_creator.code_major_ver)
    assert base_creator.assign_new_data_version(
        conn=None,
        resolution='HH', last_base_version='10-1', last_processID=1,
        is_last_ver_cdiac=False, site_id='US-UMB',
        md5sum='94988170f096e6c6fb280a6bfd3e3245', processID=3) == (
        '11-' + base_creator.code_major_ver)
    assert base_creator.assign_new_data_version(
        conn=None,
        resolution='HH', last_base_version=None, last_processID=None,
        is_last_ver_cdiac=False, site_id='US-UMB',
        md5sum='94988170f096e6c6fb280a6bfd3ee075', processID=3) == (
        '1-' + base_creator.code_major_ver)
    assert base_creator.assign_new_data_version(
        conn=None,
        resolution='HR', last_base_version='3-1', last_processID=None,
        is_last_ver_cdiac=True, site_id='US-UMB',
        md5sum='94988170f096e6c6fb280a6bfd3e3245', processID=3) == (
        '4-' + base_creator.code_major_ver)
    last_base_version = '-'.join(['10', base_creator.code_major_ver])
    err_msg = f'Last BASE version ({last_base_version}) is the same as the ' \
              f'new version ({last_base_version}).'
    with pytest.raises(Exception):
        base_creator.assign_new_data_version(
            resolution='HH', last_base_version=last_base_version,
            last_processID=1, is_last_ver_cdiac=False, site_id='US-UMB',
            md5sum='94988170f096e6c6fb280a6bfd3ee075', processID=2)
        pytest.fail(err_msg)


def test_get_last_base_version(base_creator):

    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version='3-5', last_cdiac_HH=None,
        last_cdiac_HR=None) == ('3-5', False)
    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version='3-5', last_cdiac_HH='2-1',
        last_cdiac_HR=None) == ('3-5', False)
    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version='3-5', last_cdiac_HH=None,
        last_cdiac_HR='2-1') == ('3-5', False)
    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version='3-5', last_cdiac_HH='1-1',
        last_cdiac_HR='2-1') == ('3-5', False)
    assert base_creator.get_last_base_version(
        resolution='HR', last_base_version=None, last_cdiac_HH=None,
        last_cdiac_HR='2-1') == ('2-1', True)
    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version=None, last_cdiac_HH='2-1',
        last_cdiac_HR=None) == ('2-1', True)
    assert base_creator.get_last_base_version(
        resolution='HH', last_base_version=None, last_cdiac_HH='1-1',
        last_cdiac_HR='2-1') == ('1-1', True)
    no_past_base, is_last_ver_cdiac = base_creator.get_last_base_version(
        resolution='HH', last_base_version=None, last_cdiac_HH=None,
        last_cdiac_HR=None)
    assert no_past_base is None
    assert is_last_ver_cdiac is False
    with pytest.raises(Exception, match='.* 2-1 .*'):
        base_creator.get_last_base_version(
            resolution='HH', last_base_version=None, last_cdiac_HH=None,
            last_cdiac_HR='2-1')
