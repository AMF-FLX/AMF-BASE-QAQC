import collections
import pytest
from fp_vars import FPVariables
from join_site_data import JoinSiteData

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'TA': 'dummy', 'SW_IN': 'dummy', 'FETCH_70': 'dummy',
            'FETCH_MAX': 'dummy', 'CO2': 'dummy'}


@pytest.fixture
def join_site_data(monkeypatch):
    """ Initializes JoinSiteData """
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)
    v = JoinSiteData()
    return v


def test_get_valid_variables(join_site_data):
    valid_variables = [
        'TIMESTAMP_START',
        'TIMESTAMP_END',
        'SW_IN',
        'SW_IN_1_1_1',
        'SW_IN_1_1_A',
        'SW_IN_1_1_A_SD',
        'SW_IN_1_1_A_N',
        'SW_IN_1',
        'SW_IN_1_SD',
        'SW_IN_1_N',
        'SW_IN_F',
        'SW_IN_F_1_1_1',
        'SW_IN_F_1_1_A',
        'SW_IN_F_1_1_A_SD',
        'SW_IN_F_1_1_A_N',
        'SW_IN_F_1',
        'SW_IN_F_1_SD',
        'SW_IN_F_1_N',
    ]
    file_var_dict = {
        'file1': valid_variables + ['NONONON',
                                    'SW_IN_1_1',
                                    'SW_IN_1_1_1_1'],
        'file2': valid_variables + ['SW_IN_A',
                                    'SW_IN_SD',
                                    'SW_IN_N',
                                    'TIMESTAMP_START_1'],
        'file3': valid_variables + ['TIMESTAMP_START_1',
                                    'SW_IN_F_SD',
                                    'SW_IN_N_F',
                                    'SW_IN_d1']
    }
    assert join_site_data.get_valid_variables(file_var_dict) == valid_variables


@pytest.mark.parametrize(
    ('site_id, expected_file_order_names, expected_skip_list_names, '
     'expected_start_dates, expected_end_dates'),
    [('US-CMW',
      ['US-CMW_HH_200012312330_200512312330-2020082018504020.csv',
       'US-CMW_HH_200512312330_201012312330-2020082109341394.csv',
       'US-CMW_HH_201012312330_201512312330-2020082111555299.csv',
       'US-CMW_HH_201512312330_201912312330-2020082112055722.csv'],
      ['US-CMW_HH_200012312330_200512312330-2020070116330589.csv'],
      ['200012312330', '200512312330', '201012312330', '201512312330'],
      ['200512312330', '201012312330', '201512312330', '201912312330']),
     ('US-xTA',
      ['US-xTA_HH_201708311800_201901311800-2019091622495087.csv',
       'US-xTA_HH_201811010000_202005311800-2020082412585814.csv'],
      [],
      ['201708311800', '201811010000'],
      ['201811010000', '202005311800']),
     ('PE-QFR',
      ['PE-QFR_HH_201801010000_201901010000-2020080706435000.csv',
       'PE-QFR_HH_201901010000_202001010000-2020080706441289.csv'],
      [],
      ['201801010000', '201901010000'],
      ['201901010000', '202001010000'])],
    ids=['out of order, double', 'overlap', 'no issues'])
def test_get_file_order(join_site_data, site_id,
                        expected_file_order_names, expected_skip_list_names,
                        expected_start_dates, expected_end_dates):
    candidates_data = get_candidates_data(site_id)
    file_order, skip_list = join_site_data.get_file_order(candidates_data)
    assert [x.name for x in file_order] == expected_file_order_names
    assert [x.name for x in skip_list] == expected_skip_list_names
    assert [x.start for x in file_order] == expected_start_dates
    assert [x.end for x in file_order] == expected_end_dates


def get_candidates_data(site_id):
    FileInfo = collections.namedtuple(
        'FileInfo',
        'start end upload name status proc_id original_name prior_proc_id')
    if site_id == 'US-CMW':
        return [
            FileInfo(start='201512312330', end='201912312330',
                     upload='2020082112055722',
                     name=('US-CMW_HH_201512312330_201912312330'
                           '-2020082112055722.csv'),
                     status=None, proc_id=51892, original_name=None,
                     prior_proc_id=0),
            FileInfo(start='200012312330', end='200512312330',
                     upload='2020082018504020',
                     name=('US-CMW_HH_200012312330_200512312330'
                           '-2020082018504020.csv'),
                     status=None, proc_id=51874, original_name=None,
                     prior_proc_id=0),
            FileInfo(start='201012312330', end='201512312330',
                     upload='2020082111555299',
                     name=('US-CMW_HH_201012312330_201512312330'
                           '-2020082111555299.csv'),
                     status=None, proc_id=51889, original_name=None,
                     prior_proc_id=0),
            FileInfo(start='200512312330', end='201012312330',
                     upload='2020082109341394',
                     name=('US-CMW_HH_200512312330_201012312330'
                           '-2020082109341394.csv'),
                     status=None, proc_id=51875, original_name=None,
                     prior_proc_id=0),
            FileInfo(start='200012312330', end='200512312330',
                     upload='2020070116330589',
                     name=('US-CMW_HH_200012312330_200512312330'
                           '-2020070116330589.csv'),
                     status=None, proc_id=50509, original_name=None,
                     prior_proc_id=0)]
    if site_id == 'US-xTA':
        return [
            FileInfo(start='201811010000', end='202005311800',
                     upload='2020082412585814',
                     name=('US-xTA_HH_201811010000_202005311800'
                           '-2020082412585814.csv'),
                     status=None, proc_id=51913, original_name=None,
                     prior_proc_id=0),
            FileInfo(start='201708311800', end='201901311800',
                     upload='2019091622495087',
                     name=('US-xTA_HH_201708311800_201901311800'
                           '-2019091622495087.csv'),
                     status=None, proc_id=38637,
                     original_name=('US-xTA_HH_201708311800_201901311800'
                                    '-2019073117533580.csv'),
                     prior_proc_id=38606)]
    if site_id == 'PE-QFR':
        return [
            FileInfo(start='201801010000', end='201901010000',
                     upload='2020080706435000',
                     name=('PE-QFR_HH_201801010000_201901010000'
                           '-2020080706435000.csv'),
                     status=None, proc_id=51766,
                     original_name=('PE_QFR_HH_201801010000_201901010000'
                                    '-2020080706430437.csv'),
                     prior_proc_id=51764),
            FileInfo(start='201901010000', end='202001010000',
                     upload='2020080706441289',
                     name=('PE-QFR_HH_201901010000_202001010000'
                           '-2020080706441289.csv'),
                     status=None, proc_id=51767,
                     original_name=('PE_QFR_HH_201901010000_202001010000'
                                    '-2020080706430437.csv'),
                     prior_proc_id=51765)]
    return []
