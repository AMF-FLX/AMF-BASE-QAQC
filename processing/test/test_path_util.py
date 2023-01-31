import os
import pytest
from path_util import PathUtil

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl..gov'


@pytest.fixture
def path_util():
    """ Initializes PathUtil """
    return PathUtil()


_cwd = os.getcwd()


def test_create_valid_path(path_util):
    test_dir_name = 'fools_test_xxx'
    test_dir_path = os.path.join(_cwd, test_dir_name)

    assert not os.path.exists(test_dir_path)
    new_path = path_util.create_valid_path(_cwd, test_dir_name)
    assert new_path == test_dir_path
    assert os.path.exists(test_dir_path)
    os.rmdir(test_dir_path)
    assert not os.path.exists(test_dir_path)


def test_create_valid_existing_path(path_util):
    test_dir_name = 'fools_test_yyy'
    test_dir_path = os.path.join(_cwd, test_dir_name)
    os.mkdir(test_dir_path)
    assert os.path.exists(os.path.join(_cwd, test_dir_name))
    new_path = path_util.create_valid_path(_cwd, test_dir_name)
    assert new_path == test_dir_path
    assert os.path.exists(test_dir_path)
    os.rmdir(test_dir_path)
    assert not os.path.exists(test_dir_path)


def test_create_dir_for_run(path_util):
    site_id = 'XX-xxx'
    process_id = '8'*9
    test_site_id_path = os.path.join(_cwd, site_id)
    test_process_id_path = os.path.join(test_site_id_path, process_id)
    assert not os.path.exists(test_site_id_path)
    assert not os.path.exists(test_process_id_path)

    dir_run_path = path_util.create_dir_for_run(
        site_id, process_id, _cwd)

    assert dir_run_path == test_process_id_path
    assert dir_run_path != test_site_id_path
    assert os.path.exists(test_process_id_path)

    os.rmdir(test_process_id_path)
    assert not os.path.exists(test_process_id_path)
    os.rmdir(test_site_id_path)
    assert not os.path.exists(test_site_id_path)


def test_get_base_ver_from_fname(path_util):
    _test_func = path_util.get_base_ver_from_hist_fname
    test_fname = 'AMF_BR-Sa1_BASE_HR_4-1_LOWER1.csv'
    assert _test_func(test_fname) == '4-1'
    test_fname = 'xxx.csv'
    assert _test_func(test_fname) is None
    test_fname = 'xxx_2.csv'
    assert _test_func(test_fname) is None
    test_fname = 'AMF_BR-Sa1_BASE_HR_4-1.csv'
    assert _test_func(test_fname) is None
