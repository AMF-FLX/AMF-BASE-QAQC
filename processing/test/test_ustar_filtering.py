import itertools
import json
import matplotlib.dates as dates
import mmap
import numpy as np
import numpy.ma as ma
import os
import pytest
import shutil

from configparser import ConfigParser
from data_reader import DataReader
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from logger import Logger
from plot_config import PlotConfig
from status import Status, StatusCode, StatusGenerator
from sw_in_pot_gen import SW_IN_POT_Generator
from timestamp_checks import TimestampChecks
from ustar_filtering import USTARFiltering
from utils import VarUtil

__author__ = 'Danielle Christianson', 'Josh Geden'
__email__ = 'dschristianson@lbl.gov', 'joshgeden10@gmail.com'


# Path to json file with test inputs & expected values for e2e tests
json_file_path = os.path.join(
    'test', 'testdata', 'ustar_filtering', 'test_ustar_filtering.json')


@pytest.fixture
def ustar(monkeypatch):
    ''' Initializes VarUtil '''
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)

    uf = USTARFiltering(site_id='TestSite', process_id='TestProcess')
    # Mock var dict if unable to get from WS
    if not uf.var_util.var_dict:
        uf.var_util.var_dict = {
            'PPFD_IN': 'dummy',
            'SW_IN': 'dummy',
            'USTAR': 'dummy',
            'FC': 'dummy',
            'CO2': 'dummy'
        }
    return uf


def mock_FPVariables_init(self):
    self.fp_vars = {
        'FC': 'dummy',
        'TIMESTAMP_END': 'dummy',
        'TIMESTAMP_START': 'dummy',
        'SW_IN': 'dummy',
        'PPFD_IN': 'dummy',
    }


def mock_datareader_init(self):
    self.var_util = VarUtil()


def mock_plot(self, year, ustar_var, fc_var, plot_data):
    # Determine where to save the figure
    return os.path.join(f'{self.site_id}-{self.process_id}-'
                        f'ustar_filtering-{ustar_var}-{fc_var}-{year}.png')


def test_init(monkeypatch):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)

    plot_dir = os.path.join('output', 'US-CRT', 'TestProcess_###', 'output')
    ftp_plot_dir = os.path.join('US-CRT', 'TestProcess_###')
    uf = USTARFiltering(
        site_id='TestSite_###', process_id='TestProcess_###',
        plot_dir=plot_dir, ftp_plot_dir=ftp_plot_dir)

    # Test instance variables
    assert uf.data is None
    assert uf.url_path == ftp_plot_dir
    assert uf.site_id == 'TestSite_###'
    assert uf.process_id == 'TestProcess_###'
    assert uf.rad_in_var is None
    assert uf.qaqc_name == 'ustar_filtering'

    # Test that util objects were created
    assert hasattr(uf, 'plot_config')
    assert hasattr(uf, 'ts_util')
    assert hasattr(uf, 'var_util')

    # Check that plotting is enabled and paths were generated
    assert uf.can_plot is True
    assert uf.plot_dir == os.path.join(plot_dir, uf.qaqc_name)
    assert uf.base_plot_dir == plot_dir

    # Check that config file variables were properly read
    assert uf.lower_bound_warn == 0.02
    assert uf.lower_bound_error == 0.10
    assert uf.difference_warn == 0.02
    assert uf.difference_error == 0.10
    assert uf.sw_day_night_cutoff == 5
    assert uf.ppfd_day_night_cutoff == 10

    # Temporarily remove the ONEFLUX_VARIABLES & PHASE_II sections by copying a
    # temporary config file and test that the init fails as expected
    shutil.copy('qaqc.cfg', '_temp_qaqc.cfg')
    test_cfg = os.path.join(
        'test', 'testdata', 'ustar_filtering', 'test_qaqc.cfg')
    shutil.copy(test_cfg, 'qaqc.cfg')

    uf = USTARFiltering(site_id='TestSite_###', process_id='TestProcess_###')
    assert uf.can_plot is False
    assert uf.plot_dir is None
    assert uf.base_plot_dir is None

    assert uf.lower_bound_warn == .01
    assert uf.lower_bound_error == .05
    assert uf.difference_warn == .01
    assert uf.difference_error == .05
    assert uf.sw_day_night_cutoff == 5
    assert uf.ppfd_day_night_cutoff == 10

    # Reset the default qaqc.cfg file
    shutil.copy('_temp_qaqc.cfg', 'qaqc.cfg')
    os.remove('_temp_qaqc.cfg')


def test_select_rad_var(ustar):
    """
    Test radiation variable selection
    :param ustar: instance of USTARFiltering
    :return:
    """
    rad_vars = ('SW_IN', 'PPFD_IN', 'SW_IN_POT')
    header_map = {
        'SW_IN': ['SW_IN_F'],
        'PPFD_IN': ['PPFD_IN_F'],
        'SW_IN_POT': ['SW_IN_POT']
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_POT'

    rad_vars = ('SW_IN', 'PPFD_IN')
    # all filled variables
    header_map = {
        'SW_IN': ['SW_IN_F'],
        'PPFD_IN': ['PPFD_IN_F']
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_F'
    # filled and non-filled versions
    header_map = {
        'SW_IN': ['SW_IN_F', 'SW_IN'],
        'PPFD_IN': ['PPFD_IN_F', 'PPFD_IN'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN'
    # multiple vertical positions
    header_map = {
        'PPFD_IN': ['PPFD_IN_1_1_1', 'PPFD_IN_1_2_1'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'PPFD_IN_1_1_1'
    # filled one and non-filled other
    header_map = {
        'SW_IN': ['SW_IN_F'],
        'PPFD_IN': ['PPFD_IN'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_F'
    # replicates with filled and non-filled versions
    header_map = {
        'SW_IN': ['SW_IN_1_1_2', 'SW_IN_1_1_1',
                  'SW_IN_F_1_1_1', 'SW_IN_F_1_1_2'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_1_1_1'
    # multiple horizontal locations
    header_map = {
        'SW_IN': ['SW_IN_1_1_1', 'SW_IN_2_1_1'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_1_1_1'
    # replicate aggregation
    header_map = {
        'SW_IN': ['SW_IN_1_1_1', 'SW_IN_1_1_2', 'SW_IN_1_1_A'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_1_1_A'
    # horizontal layer aggregation
    header_map = {
        'SW_IN': ['SW_IN_1_1_1', 'SW_IN_1'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_1'
    # no radiation variables submitted
    rad_vars = ('SW_IN_POT')
    header_map = {
        'SW_IN_POT': ['SW_IN_POT']
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'SW_IN_POT'
    # This is not a real case but rather to test the code
    rad_vars = ('SW_IN', 'PPFD_IN')
    header_map = {
        'SW_IN': [],
        'PPFD_IN': ['PPFD_IN'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'PPFD_IN'
    # This is not a real case but rather to test the code
    header_map = {
        'SW_IN': ['SW_IN_YO'],
        'PPFD_IN': ['PPFD_IN'],
    }
    assert ustar._select_rad_var(
        header_map=header_map, rad_vars=rad_vars) == 'PPFD_IN'


def test_check_ustar_vars_present(monkeypatch, ustar):

    def mock_get_base_headers(self):
        return {
            'USTAR': ['USTAR_1_1_1', 'USTAR_2_1_1'],
            'FC': ['FC_F', 'FC_1_1_1'],
            'SW_IN_POT': ['SW_IN_POT']
        }

    monkeypatch.setattr(DataReader, 'get_base_headers', mock_get_base_headers)

    assert ustar._check_ustar_vars_present(
        DataReader(), ['USTAR', 'FC', 'SW_IN_POT']) is None

    src_logger_name = f'{ustar.qaqc_name}-all_data-CO2-var_present'
    status_msg = ('Data file does not contain required '
                  'variable: CO2')

    status = ustar._check_ustar_vars_present(DataReader(), ['CO2'])
    assert isinstance(status, Status)

    assert status.get_status_code() == StatusCode.FATAL
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg


def test_get_var_non_gapfilled(monkeypatch, ustar):
    monkeypatch.setattr(DataReader, '__init__', mock_datareader_init)
    d = DataReader()
    d.base_headers = {
        'USTAR': ['USTAR_F', 'USTAR_1_1_1'],
        'FC': ['FC_F'],
        'SW_IN_POT': ['SW_IN_POT']
    }
    assert ustar._get_vars_non_gapfilled(
        d.get_base_headers())['FC'] == ['FC_F']
    assert ustar._get_vars_non_gapfilled(
        d.get_base_headers())['USTAR'] == ['USTAR_1_1_1']
    assert ustar._get_vars_non_gapfilled(
        d.get_base_headers())['SW_IN_POT'] == ['SW_IN_POT']


def test_get_start_end_idx(ustar):
    expected_result = {
        '2011': {
            'start': 0,
            'end': 1,
        },
        '2012': {
            'start': 2,
            'end': 2,
        },
        '2013': {
            'start': 3,
            'end': 4,
        },
        'all_data': {
            'start': 0,
            'end': 4,
        }
    }

    # Mocks how dates are representeted in a DataReader object
    data = np.array(
        [
            (b'201101010000', b'201101010030'),
            (b'201101010030', b'201101010100'),
            (b'201201010000', b'201201010030'),
            (b'201301010000', b'201301010030'),
            (b'201301010030', b'201301010100'),
        ],
        dtype=[('TIMESTAMP_START', 'S25'), ('TIMESTAMP_END', 'S25')]
    )

    actual_result = ustar._get_start_end_idx(data)
    assert actual_result == expected_result

    years = list(actual_result.keys())[:-1]
    assert len(years) == 3
    assert 'all_data' not in years


def test_calculate_ustar_metrics(monkeypatch, ustar):
    monkeypatch.setattr(DataReader, '__init__', mock_datareader_init)

    def int_timestamp(timestamp):
        return dates.date2num(ustar.ts_util.cast_as_datetime(timestamp))

    d = DataReader()
    d.base_headers = {
        'USTAR': ['USTAR'],
        'FC': ['FC'],
        'SW_IN_POT': ['SW_IN_POT']
    }
    d.data = ma.array(
        [
            (b'201101010000', b'201101010030', 0.7, 1, 0),
            (b'201101010030', b'201101010100', 0.6, -9999, 0),
            (b'201101010100', b'201101010130', 0.9, 1, 100),
            (b'201101010130', b'201101010200', 0.8, -9999, 100)
        ],
        mask=[
            (0, 0, 0, 0, 0),
            (0, 0, 0, 1, 0),
            (0, 0, 0, 0, 0),
            (0, 0, 0, 1, 0),
        ],
        dtype=[
            ('TIMESTAMP_START', 'S25'),
            ('TIMESTAMP_END', 'S25'),
            ('USTAR', '<f8'),
            ('FC', '<f8'),
            ('SW_IN_POT', '<f8')
        ]
    )

    ustar.data = d.get_data()
    ustar.rad_in_var = ustar._select_rad_var(
        d.get_base_headers(),
        rad_vars=['SW_IN_POT', 'SW_IN', 'PPFD_IN'])

    yearly_metrics, plot_data = ustar._calculate_ustar_metrics(
        d.get_base_headers())

    assert yearly_metrics == {
        '2011': {
            'USTAR': {
                'base': {
                    'day': 0.8,
                    'night': 0.6
                },
                'FC': {
                    'day': 0.9,
                    'night': 0.7
                }
            }
        }
    }

    assert plot_data == {
        '2011': {
            'USTAR': {
                'base': {
                    'day': {
                        int_timestamp(b'201101010100'): 0.9,
                        int_timestamp(b'201101010130'): 0.8
                    },
                    'night': {
                        int_timestamp(b'201101010000'): 0.7,
                        int_timestamp(b'201101010030'): 0.6
                    }
                },
                'FC': {
                    'day': {
                        int_timestamp(b'201101010100'): 0.9
                    },
                    'night': {
                        int_timestamp(b'201101010000'): 0.7
                    }
                }
            }
        }
    }


def test_get_filtering_status(ustar):
    # Filtering error (fc_var_min > 0.10)
    src_logger_name = 'ustar_filtering-2011-USTAR:FC-filter_check_day'
    status_msg = ('Possible USTAR filtering detected with respect to FC; '
                  'minimum USTAR value is 0.5')
    status = ustar._get_ustar_filtering_status(
        year='2011', ustar_var='USTAR', fc_var='FC',
        period='day', fc_var_min=0.5)
    assert status.get_status_code() == StatusCode.ERROR
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_summary_stat('min_USTAR:FC_day') == 0.5

    # Filtering warning (fc_var_min > 0.02)
    src_logger_name = 'ustar_filtering-2011-USTAR:FC-filter_check_night'
    status_msg = ('Possible USTAR filtering detected with respect to FC; '
                  'minimum USTAR value is 0.05')
    status = ustar._get_ustar_filtering_status(
        year='2011', ustar_var='USTAR', fc_var='FC',
        period='night', fc_var_min=0.05)
    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_summary_stat('min_USTAR:FC_night') == 0.05

    # Filtering info (fc_var_min < warning < error)
    src_logger_name = 'ustar_filtering-2011-USTAR:FC-filter_check_day'
    status_msg = ('Minimum USTAR value with respect to FC is 0.01')
    status = ustar._get_ustar_filtering_status(
        year='2011', ustar_var='USTAR', fc_var='FC',
        period='day', fc_var_min=0.01)
    assert status.get_status_code() == StatusCode.OK
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_summary_stat('min_USTAR:FC_day') == 0.01


def test_get_filtering_msg(ustar):
    # base + filtering
    msg = ustar._get_filtering_msg('base', 0.5)
    assert msg == ('Possible USTAR filtering detected; '
                   'minimum USTAR value is 0.5')

    # fc + filtering
    msg = ustar._get_filtering_msg('FC', 0.5)
    assert msg == ('Possible USTAR filtering detected with respect to FC; '
                   'minimum USTAR value is 0.5')

    # base + no filtering
    msg = ustar._get_filtering_msg('base', 0.01, filtering=False)
    assert msg == ('Minimum USTAR value is 0.01')

    # fc + no filtering
    msg = ustar._get_filtering_msg('FC', 0.01, filtering=False)
    assert msg == ('Minimum USTAR value with respect to FC is 0.01')


def test_get_ustar_diff_status(ustar):
    # diff error (diff > 0.10)
    src_logger_name = 'ustar_filtering-2011-USTAR:FC-diff_check_day'
    status_msg = ustar._get_ustar_diff_msg(fc_var='FC', period='day', diff=0.5)

    status = ustar._get_ustar_diff_status(
        year='2011', ustar_var='USTAR', fc_var='FC', period='day',
        ustar_min=1.0, fc_ustar_min=0.5, figure_link='test_link'
    )
    assert status.get_status_code() == StatusCode.ERROR
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_plot_paths() == ['test_link']
    assert status.get_summary_stat('diff') == 0.5

    # diff warning (diff > 0.02)
    src_logger_name = 'ustar_filtering-2011-USTAR:FC-diff_check_night'
    status_msg = ustar._get_ustar_diff_msg(
        fc_var='FC', period='night', diff=0.05)
    status = ustar._get_ustar_diff_status(
        year='2011', ustar_var='USTAR', fc_var='FC', period='night',
        ustar_min=1.0, fc_ustar_min=0.95, figure_link='test_link'
    )
    assert status.get_status_code() == StatusCode.WARNING
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_plot_paths() == ['test_link']
    assert status.get_summary_stat('diff') == 0.05

    # diff info
    src_logger_name = ('ustar_filtering-2011-'
                       'USTAR_1_1_1:FC_1_1_1-diff_check_day')
    status_msg = ustar._get_ustar_diff_msg(
        fc_var='FC_1_1_1', period='day', diff=0.01)
    status = ustar._get_ustar_diff_status(
        year='2011', ustar_var='USTAR_1_1_1', fc_var='FC_1_1_1', period='day',
        ustar_min=1.0, fc_ustar_min=0.99, figure_link=None
    )
    assert status.get_status_code() == StatusCode.OK
    assert status.get_src_logger_name() == src_logger_name
    assert status.get_status_msg() == status_msg
    assert status.get_plot_paths() is None
    assert status.get_summary_stat('diff') == 0.01


def test_get_ustar_diff_msg(ustar):
    # diff > difference_error or difference_warn
    msg = ustar._get_ustar_diff_msg(fc_var='FC', period='day', diff=0.5)
    assert msg == ('day time difference in lower bounds exceeds limits '
                   'with respect to FC; difference is 0.5')

    # diff is ok
    msg = ustar._get_ustar_diff_msg(
        fc_var='FC_1_1_1', period='night', diff=0.01)
    assert msg == ('night time difference in lower bounds with respect to '
                   'FC_1_1_1 is 0.01')


def generate_plot_data():
    infile = os.path.join(
        'test', 'testdata', 'ustar_filtering',
        'US-CRT_HH_201101010000_201201010000_TestUSTARFiltering000001.csv')

    d = DataReader()

    # Read csv file into datareader object
    _log_test = Logger().getLogger('read_file')
    _log_test.resetStats()
    d.read_single_file(infile, _log_test)

    # Need to call this check to populate base_headers variable
    _log_test = Logger().getLogger('data_headers')
    _log_test.resetStats()
    d._check_data_header(d.header_as_is, _log_test)

    uf = USTARFiltering(site_id='US-CRT', process_id='TestProcess_###')
    uf.data = d.get_data()
    uf.rad_in_var = 'SW_IN'

    _, plot_data = uf._calculate_ustar_metrics(
        d.get_base_headers())

    return plot_data


def test_make_plot():
    uf = USTARFiltering(site_id='US-CRT', process_id='TestProcess_###')
    figure_link = uf._make_plot(
        year='2011', ustar_var='USTAR', fc_var='base', plot_data=None)
    assert figure_link is None

    plot_data_file = os.path.join(
        'test', 'testdata', 'ustar_filtering',
        'plot_data.json')

    if not os.path.exists(plot_data_file):
        plot_data = generate_plot_data()

        with open(plot_data_file, 'w') as f:
            json.dump(plot_data, f)
    else:
        with open(plot_data_file) as f:
            json_data = json.load(f)

        years = list(json_data.keys())
        ustar_vars = list(json_data[years[0]].keys())
        fc_vars = list(json_data[years[0]][ustar_vars[0]].keys())
        periods = ['day', 'night']

        plot_data = {}

        # A bit obnoxious, but the keys must be floats. So here we create
        # a new plot_data dict with float keys instead of str keys
        for year, ustar_var, fc_var, period in itertools.product(
                years, ustar_vars, fc_vars, periods):

            for key, val in json_data[year][ustar_var][fc_var][period].items():
                if year not in plot_data:
                    plot_data[year] = {}
                if ustar_var not in plot_data[year]:
                    plot_data[year][ustar_var] = {}
                if fc_var not in plot_data[year][ustar_var]:
                    plot_data[year][ustar_var][fc_var] = {}
                if period not in plot_data[year][ustar_var][fc_var]:
                    plot_data[year][ustar_var][fc_var][period] = {}

                plot_data[year][ustar_var][fc_var][period][float(key)] = val

        del json_data

    site_id = 'US-CRT'
    process_id = 'TestProcess_###'

    cwd = os.getcwd()
    config = ConfigParser()
    with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    plot_dir = os.path.join(root_output_dir, site_id, process_id, 'output')

    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)

    uf = USTARFiltering(site_id, process_id, plot_dir, ftp_plot_dir=plot_dir)

    plot_path = uf._make_plot(
        year='2011', ustar_var='USTAR', fc_var='FC', plot_data=plot_data)
    assert plot_path is not None
    assert os.path.exists(plot_path)

    test_plot_path = os.path.join(
        'test', 'testdata', 'ustar_filtering',
        'test_plot.png')
    assert os.path.exists(test_plot_path)

    with open(plot_path, 'rb') as f:
        img_1 = hash(f.read())
    with open(test_plot_path, 'rb') as f:
        img_2 = hash(f.read())
    assert img_1 == img_2


def test_add_result_summary_stat(ustar):
    # All good
    create_and_test_stats(ustar, expected_value=StatusCode.OK)

    # min(USTAR)_day > threshold1
    create_and_test_stats(ustar, base_filter_day_code=StatusCode.ERROR,
                          expected_value=StatusCode.ERROR)

    # min(USTAR)_night > threshold1
    create_and_test_stats(ustar, base_filter_night_code=StatusCode.ERROR,
                          expected_value=StatusCode.ERROR)

    # min(USTAR|FC)_day > threshold1
    create_and_test_stats(ustar, fc_filter_day_code=StatusCode.ERROR,
                          fc_filter_night_code=StatusCode.WARNING,
                          expected_value=StatusCode.ERROR)

    # min(USTAR|FC)_night > threshold1
    create_and_test_stats(ustar, fc_filter_night_code=StatusCode.ERROR,
                          fc_diff_day_code=StatusCode.WARNING,
                          expected_value=StatusCode.ERROR)

    # diff day > threshold2
    create_and_test_stats(ustar, fc_diff_day_code=StatusCode.ERROR,
                          expected_value=StatusCode.ERROR)

    # diff night > threshold2
    create_and_test_stats(ustar, fc_filter_night_code=StatusCode.ERROR,
                          fc_diff_night_code=StatusCode.WARNING,
                          expected_value=StatusCode.ERROR)

    # min(USTAR)_day > threshold3
    create_and_test_stats(ustar, base_filter_day_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)

    # min(USTAR)_night > threshold3
    create_and_test_stats(ustar, base_filter_night_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)

    # min(USTAR|FC)_day > threshold3
    create_and_test_stats(ustar, fc_filter_day_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)

    # min(USTAR|FC)_night > threshold3
    create_and_test_stats(ustar, fc_filter_night_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)

    # diff day > threshold4
    create_and_test_stats(ustar, fc_diff_day_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)

    # diff night > threshold4
    create_and_test_stats(ustar, fc_filter_night_code=StatusCode.WARNING,
                          expected_value=StatusCode.WARNING)


def create_and_test_stats(ustar,
                          base_filter_day_code=StatusCode.OK,
                          base_filter_night_code=StatusCode.OK,
                          fc_filter_day_code=StatusCode.OK,
                          fc_filter_night_code=StatusCode.OK,
                          fc_diff_day_code=StatusCode.OK,
                          fc_diff_night_code=StatusCode.OK,
                          expected_value=StatusCode.OK):
    """ Builds nested Status objects for use in
        test_add_result_summary_stat """

    qaqc_check = 'ustar_filtering-2011-USTAR:FC-filter_check_day'
    fc_filter_day = Status(
        status_code=fc_filter_day_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if fc_filter_day_code == StatusCode.WARNING else 0,
        n_error=1 if fc_filter_day_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC-filter_check_night'
    fc_filter_night = Status(
        status_code=fc_filter_night_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if fc_filter_night_code == StatusCode.WARNING else 0,
        n_error=1 if fc_filter_night_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC-diff_check_day'
    fc_diff_day = Status(
        status_code=fc_diff_day_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if fc_diff_day_code == StatusCode.WARNING else 0,
        n_error=1 if fc_diff_day_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC-diff_check_night'
    fc_diff_night = Status(
        status_code=fc_diff_night_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if fc_diff_night_code == StatusCode.WARNING else 0,
        n_error=1 if fc_diff_night_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC_base-filter_check_day'
    base_filter_day = Status(
        status_code=base_filter_day_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if base_filter_day_code == StatusCode.WARNING else 0,
        n_error=1 if base_filter_day_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC_base-filter_check_night'
    base_filter_night = Status(
        status_code=base_filter_night_code,
        qaqc_check=qaqc_check,
        src_logger_name=qaqc_check,
        n_warning=1 if base_filter_night_code == StatusCode.WARNING else 0,
        n_error=1 if base_filter_night_code == StatusCode.ERROR else 0)

    qaqc_check = 'ustar_filtering-2011-USTAR:FC'
    fc_status = StatusGenerator().composite_status_generator(
        logger=Logger().getLogger(qaqc_check),
        qaqc_check=qaqc_check,
        statuses={
            fc_filter_day.get_qaqc_check(): fc_filter_day,
            fc_filter_night.get_qaqc_check(): fc_filter_night,
            fc_diff_day.get_qaqc_check(): fc_diff_day,
            fc_diff_night.get_qaqc_check(): fc_diff_night,
            base_filter_day.get_qaqc_check(): base_filter_day,
            base_filter_night.get_qaqc_check(): base_filter_night
        }
    )

    qaqc_check = 'ustar_filtering-2011'
    log_obj = Logger().getLogger(qaqc_check)
    year_status = StatusGenerator().composite_status_generator(
        logger=log_obj, qaqc_check=qaqc_check,
        statuses={
            fc_status.get_qaqc_check(): fc_status
        }
    )

    ustar._add_result_summary_stat([year_status])
    assert fc_status.get_summary_stat('result') == expected_value


def parse_json(json_file_path: str):
    """ Parses the json file containing the test inputs & expected values
    to be formatted for use with pytest.mark.parametrize() """

    with open(json_file_path) as f:
        data = json.load(f)

    # Obtains the arguments that are required to call test_timeshift()
    input_vars = data['input_variables']

    vars = []  # Holds the argument values for each test id
    ids = []   # Holds the ids for each individual test
    for entry in data['tests']:
        ids.append(entry['id'])

        var_list = []
        for var_name in input_vars:
            if var_name in entry['variables']:
                var_list.append(entry['variables'][var_name])
            else:
                var_list.append(None)

        vars.append(tuple(var_list))

    # E.g.: ['var1', 'var2', 'var3'] -> 'var1, var2, var3'
    input_vars = ', '.join(input_vars)

    return input_vars, vars, ids


# Obtain the inputs for each test from the json file
input_vars, vars, ids = parse_json(json_file_path)


@pytest.mark.parametrize(input_vars, vars, ids=ids)
def test_e2e(monkeypatch, filename, site_id, expected_results):
    # Monkeypatch plotting
    # Monkeypatch FP_Variables?
    monkeypatch.setattr(FPVariables, '__init__',
                        mock_FPVariables_init)
    monkeypatch.setattr(USTARFiltering, '_make_plot', mock_plot)

    process_id = 'TestProcess_###'
    resolution = 'HH'

    # Find the output folder
    config = ConfigParser()
    with open('qaqc.cfg') as cfg:
        config.read_file(cfg)
        root_output_dir = config.get('PHASE_II', 'output_dir')
    output_dir = os.path.join(root_output_dir, site_id, process_id)

    # Removes any output dirs for the same site_id and process_id as above
    if os.path.isdir(output_dir):
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Setup logger to save to file
    process_type = 'BASE Generation'
    _log = Logger(True, process_id, site_id, process_type).getLogger(
        'BASE Generation')
    _log.get_log_dir()

    # Setup data file paths
    testdata_path = os.path.join('.', 'test', 'testdata', 'ustar_filtering')
    pickle_path = os.path.join(testdata_path, filename.replace('.csv', '.npy'))
    filepath = os.path.join('test', 'testdata', 'ustar_filtering', filename)

    # Attempt to load from binary .npy file, otherwise re-process data
    d = DataReader()
    try:
        d.data = np.load(pickle_path, allow_pickle=True)

        header_file_path = filepath.replace('.csv', '') + '_headers.txt'
        with open(header_file_path) as infile:
            d.header_as_is = infile.read().split(',')
    except FileNotFoundError:
        # Read csv file into datareader object
        _log_test = Logger().getLogger('read_file')
        _log_test.resetStats()
        d.read_single_file(filepath, _log_test)

        fnv = FileNameVerifier()
        fnv.driver(filename)
        _, ts_start, ts_end = TimestampChecks().driver(d, fnv.fname_attrs)

        gen = SW_IN_POT_Generator()
        gen.gen_rem_sw_in_pot_data(d, process_id, resolution,
                                   site_id, ts_start, ts_end)
        # Merge POT_data with rest of the data
        d.data = gen.merge_data(
            d, site_id, resolution, process_id, ts_start, ts_end)

        # Write data to a file
        d.data.dump(os.path.join(filepath.replace('.csv', '.npy')))
        outfile = os.path.join(filepath.replace('.csv', '') + '_headers.txt')
        with open(outfile, 'w') as out:
            out.write(','.join(d.header_as_is))

    # Add SW_IN_POT as a valid data header
    d.base_headers['SW_IN_POT'] = ['SW_IN_POT']

    # Need to set proper data headers to log datareader info
    _log_test = Logger().getLogger('data_headers')
    _log_test.resetStats()
    d._check_data_header(d.header_as_is, _log_test)

    # Get paths for where to save plots
    p = PlotConfig(True)
    plot_dir = p.get_plot_dir_for_run(site_id, process_id)
    ftp_plot_dir = p.get_ftp_plot_dir_for_run(
        site_id, process_id, site_id)

    # Call the driver function
    # TODO speed up this function (currently takes ~75% of the time per run)
    statuses = USTARFiltering(
        site_id=site_id,
        process_id=process_id,
        plot_dir=plot_dir,
        ftp_plot_dir=ftp_plot_dir,
    ).driver(d)

    # Parse the json for expected statuses
    expected_statuses = expected_results['status_list']

    # Assert the statuses
    assert len(statuses) == len(expected_statuses)
    for status, expected_status in zip(statuses, expected_statuses):
        status.assert_status(expected_status)

    # Ensure the log file generated
    log_dir = os.path.join(output_dir, 'logs')
    assert os.path.exists(log_dir)

    # Only 1 log file should have generated
    assert len(os.listdir(log_dir)) == 1

    # Get the path of the log file generated
    log_file = os.listdir(log_dir)[0]
    log_file_path = os.path.join(log_dir, log_file)

    # Loop through the expected log lines and ensure they exist in the log file
    with open(log_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for log_text in expected_results['logs']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(log_text)) != -1

    # Ensure the summary folder generated
    summary_dir = os.path.join(output_dir, 'output', 'summary')
    assert os.path.exists(summary_dir)

    csv_file = 'ustar_filtering_summary.csv'
    csv_file_path = os.path.join(summary_dir, csv_file)
    assert os.path.exists(csv_file_path)

    # Loop through the expected lines and ensure they exist in the csv file
    with open(csv_file_path) as f:
        stream = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        for csv_line in expected_results['ustar_filtering_summary']:
            # Ensure log file contains log_text
            assert stream.find(str.encode(csv_line)) != -1

    shutil.rmtree(output_dir)
