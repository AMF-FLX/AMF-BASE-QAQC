import pytest
from utils import FileUtil

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'


@pytest.fixture
def file_util():
    ''' Initializes FileUtil '''
    return FileUtil()


def test_get_md5(file_util):
    test_json_file_path = './test/testdata/test_summarize_vars.json'
    test_json_md5sum = '2d213f0b3f8bd62c8182f4f340bc8af4'
    assert file_util.get_md5(test_json_file_path) == test_json_md5sum
