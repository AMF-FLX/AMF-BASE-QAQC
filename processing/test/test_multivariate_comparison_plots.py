import numpy as np
import os
import pytest

from data_reader import DataReader
from fp_vars import FPVariables
from logger import Logger
from multivariate_comparison import MultivariateComparison

original_composite_plotter = MultivariateComparison.composite_plotter


def do_nothing(*args, **kwargs):
    return


def mock_FPVariables_init(self):
    self.fp_vars = {
        'TIMESTAMP_END': 'dummy',
        'TIMESTAMP_START': 'dummy',
    }


def mock_composite_plotter(self, masked_x, masked_y, masked_ts, fit, x_label,
                           y_label, outlier_ls, check_label, plot_log, yr,
                           x_val, y_val, ts_start):

    s = masked_ts[0][0]
    e = masked_ts[-1][-1]

    if s != b'201101010000' or e != b'201201010000':
        return ''

    return original_composite_plotter(
        self, masked_x, masked_y, masked_ts, fit, x_label, y_label,
        outlier_ls, check_label, plot_log, yr, x_val, y_val, ts_start)


def generate_data_file(filepath):
    outfile = os.path.join(filepath.replace('.csv', '.npy'))

    d = DataReader()

    # Read csv file into datareader object
    _log_test = Logger().getLogger('read_file')
    _log_test.resetStats()
    d.read_single_file(filepath, _log_test)

    d.data.dump(outfile)
    outfile = os.path.join(filepath.replace('.csv', '') + '_headers.txt')
    with open(outfile, 'w') as out:
        out.write(','.join(d.header_as_is))


@pytest.fixture
def mc(monkeypatch):
    monkeypatch.setattr(FPVariables, '__init__', mock_FPVariables_init)
    return MultivariateComparison('', '')


def test_plots(mc, monkeypatch):
    monkeypatch.setattr(
        MultivariateComparison, 'ws_ustar_cross_check', do_nothing)
    monkeypatch.setattr(
        MultivariateComparison, 'ppfd_in_sw_in_cross_check', do_nothing)
    monkeypatch.setattr(
        MultivariateComparison, 'ta_cross_level_check', do_nothing)
    monkeypatch.setattr(
        MultivariateComparison, 'ta_cross_replicate_check', do_nothing)
    monkeypatch.setattr(
        MultivariateComparison, 'write_summary', do_nothing)

    monkeypatch.setattr(MultivariateComparison, 'composite_plotter',
                        mock_composite_plotter)

    mc.can_plot = True
    mc.plot_dir = os.path.join('output', 'US-CRT', 'TestProcess_###', 'output',
                               'multivariate_comparison')
    if not os.path.exists(mc.plot_dir):
        os.makedirs(mc.plot_dir)
    mc.base_plot_dir = mc.plot_dir
    mc.url_path = 'output'
    mc.site_id = 'US-CRT'
    mc.process_id = 'TestProcess_###'

    filename = ('US-CRT_HH_201101010000_201401010000_'
                'TestMultivariateComparison000002.csv')
    testdata_path = os.path.join(
        'test', 'testdata', 'multivariate_comparison', 'input_files')

    filepath = os.path.join(testdata_path, filename)
    pickle_path = os.path.join(testdata_path, filename.replace('.csv', '.npy'))

    d = DataReader()
    if not os.path.exists(pickle_path):
        generate_data_file(filepath)
    d.data = np.load(pickle_path, allow_pickle=True)

    header_file_path = filepath.replace('.csv', '') + '_headers.txt'
    with open(header_file_path) as infile:
        d.header_as_is = infile.read().split(',')

    mc.driver(d)

    plot_path = os.path.join(
        mc.plot_dir,
        'US-CRT-TestProcess_###-multivariate_comparison-TA-T_SONIC-2011.png')
    assert os.path.exists(plot_path)

    test_plot_path = os.path.join(
        'test', 'testdata', 'multivariate_comparison',
        'test_plot_analysis.png')
    assert os.path.exists(test_plot_path)

    with open(plot_path, 'rb') as f:
        img_1 = hash(f.read())
    with open(test_plot_path, 'rb') as f:
        img_2 = hash(f.read())
    assert img_1 == img_2
