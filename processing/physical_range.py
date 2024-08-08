import os
import json
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np
from configparser import ConfigParser
from fp_vars import FPVariables
from logger import Logger
from output_stats import OutputStats
from plot_config import PlotConfig
from status import StatusCode, StatusGenerator
from utils import TimestampUtil, WSUtil

__author__ = 'Fianna O''Brien, You-Wei Cheah'
__email__ = 'flobrien@lbl.gov, ycheah@lbl.gov'

_log = Logger().getLogger(__name__)


class Var:
    def __init__(self, var_name, base_name, limit_dict):
        self.name = var_name
        self.base_name = base_name
        if base_name not in limit_dict:
            _log.error(f'Limits not found for {var_name} with '
                       f'basename = {base_name}.')
        # letting function fail for now in the following line if there
        #    is no key for an invalid variable name
        # May want to rework with Data QA/QC automation
        var_attr = limit_dict.get(base_name)
        self.max_lim = var_attr.get('max')
        self.min_lim = var_attr.get('min')
        self.units = var_attr.get('units')
        self.margin = var_attr.get('margin')
        if (self.max_lim - self.min_lim) * self.margin != float('inf'):
            self.error = (self.max_lim - self.min_lim) * self.margin
        else:
            self.error = self.margin * 100
        self.error_max = self.max_lim + self.error
        self.error_min = self.min_lim - self.error


class PhysicalRange:
    def __init__(
            self, site_id, process_id, plot_dir=None,
            ftp_plot_dir=None, margin=0.05):
        self.qaqc_name = 'physical_range'
        self.margin = margin
        self.site_id = site_id
        self.process_id = process_id
        self.fig_name_fmt = '{s}-{p}-physical_range-{y}-{year}.png'
        self.plot_config = PlotConfig()
        self.ts_util = TimestampUtil()
        config = ConfigParser()

        if plot_dir:
            self.can_plot = True
            self.plot_dir = self.plot_config.get_plot_dir_for_check(
                plot_dir, self.qaqc_name)
            self.base_plot_dir = plot_dir
        else:
            self.can_plot = False
        if not os.path.exists(self.plot_dir):
            os.mkdir(self.plot_dir)
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            if config.has_section('PHASE_II'):
                self.plot_path = config.get('PHASE_II', 'output_dir')
            else:
                self.plot_path = None
                _log.critical('Cannot find data QAQC output '
                              'directory from config.')
            if config.has_section('WEBSERVICES'):
                self._range_link = config.get('WEBSERVICES', 'fp_limits')
            else:
                self._range_fname = None
                _log.critical('Cannot find web services from config.')
            if config.has_section('PHYSICAL_RANGE'):
                self.soft_flag_threshold = config.getfloat(
                    'PHYSICAL_RANGE', 'soft_flag_threshold')
                self.hard_flag_threshold = config.getfloat(
                    'PHYSICAL_RANGE', 'hard_flag_threshold')
            else:
                self.soft_flag_threshold = 0.01
                self.hard_flag_threshold = 0.001
                _log.info('Cannot find physical range threshold values from '
                          'config. Using default values of '
                          f'soft_flag_threshold = {self.soft_flag_threshold} '
                          'and hard_flag_threshold = '
                          f'{self.hard_flag_threshold}.')
        self.url_path = ftp_plot_dir

    def set_limit_dict(self):
        url = self._range_link
        limits = json.loads(WSUtil(_log).get_content(url))
        variable_limits = {}
        for d in limits:
            variable, min_lim, max_lim, unit = (
                d['Name'], d['Min'], d['Max'], d['Units'])
            # Housen wants margin of error added to dict
            if min_lim is None:
                min_lim = float('-inf')
            if max_lim is None:
                max_lim = float('inf')
            unit = str(unit.replace(u'\xb5', 'micro-'))
            variable_limits[str(variable)] = {
                "min": min_lim, "max": max_lim,
                "units": unit, "margin": self.margin}
        return variable_limits

    def _get_start_end_idx(self, data):
        """Returns start and end indecies for years in data"""
        """Start by finding start indicies"""
        ts_indices = []
        for idx, t in enumerate(data['TIMESTAMP_START']):
            if t.decode('ascii').endswith('01010000'):
                ts_indices.append(idx)
        annual_seg_idxs = {}
        for idx, v in enumerate(ts_indices):
            if idx + 1 < len(ts_indices):
                start = ts_indices[idx]
                end = ts_indices[idx + 1] - 1
            else:
                start = ts_indices[idx]
                end = len(data) - 1
            name = data['TIMESTAMP_START'][start][:4].decode('ascii')
            annual_seg_idxs[name] = {'start': start, 'end': end}
        annual_seg_idxs['all_data'] = {'start': 0, 'end': len(data) - 1}
        return annual_seg_idxs

    def driver(self, d):
        stat_gen = StatusGenerator()
        self.data = d.get_data()
        self.annual_idx = self._get_start_end_idx(self.data)
        variables = self.data.dtype.names
        self.limit_dict = self.set_limit_dict()
        # list to hold all the variable status objects
        status_objects = []  # set first element to dummy that is replaced
        for variable in variables[2:]:
            var_log = Logger().getLogger(f'{self.qaqc_name}-{variable}')
            var_log.resetStats()
            var_sub_dict = {}  # dictionary to hold annual substatus objects
            all_plots = []
            base_name = d.get_base_header(variable)
            var_obj = Var(variable, base_name, self.limit_dict)
            for year in self.annual_idx.keys():
                start, end = (self.annual_idx[year]['start'],
                              self.annual_idx[year]['end'])
                annual_data = self.data[var_obj.name][start:end + 1]

                """
                CHECK FOR EXCEPTIONAL CASES
                """

                yr_log = Logger().getLogger(
                    f'{self.qaqc_name}-{year}-{var_obj.name}')
                yr_log.resetStats()

                percent_max = 1 + var_obj.margin
                percent_min = 0 - var_obj.margin
                if annual_data.all() is np.ma.masked:
                    status_msg = ('All values missing; '
                                  'physical range not tested.')
                    yr_log.info(status_msg)
                    var_sub_dict[yr_log.getName()] = \
                        stat_gen.status_generator(
                            logger=yr_log, qaqc_check=yr_log.getName(),
                            status_msg=status_msg, report_type='single_msg')
                elif var_obj.units == '%' and all(
                        [(i <= percent_max and i >= percent_min)
                         for i in annual_data if not np.ma.is_masked(i)]):

                    # Create is_percent status object
                    check_log = Logger().getLogger(
                        f'{self.qaqc_name}-{year}-{var_obj.name}-unit_check')
                    check_log.resetStats()
                    plot_path = self.plot(var_obj, year)
                    status_msg = (f'{var_obj.name}\'s units look like a '
                                  'fraction; should be a percentage')
                    check_log.error(status_msg)

                    percent_ratio_stat = stat_gen.status_generator(
                        logger=check_log, qaqc_check=check_log.getName(),
                        status_msg=status_msg, plots=[plot_path])
                    percent_ratio_stat.add_summary_stat('is_percent', False)

                    # Create outlier status object
                    check_log = Logger().getLogger(
                        f'{self.qaqc_name}-{year}-'
                        f'{var_obj.name}-outlier_check')
                    status_msg = 'Outlier check not performed'
                    check_log.info(status_msg)

                    outlier_stat = stat_gen.status_generator(
                        logger=check_log, qaqc_check=check_log.getName(),
                        status_msg=status_msg)

                    # Create yearly status object
                    year_stat = stat_gen.composite_status_generator(
                        logger=yr_log, qaqc_check=yr_log.getName(),
                        plot_paths=[plot_path],
                        statuses={
                            percent_ratio_stat.get_qaqc_check():
                                percent_ratio_stat,
                            outlier_stat.get_qaqc_check(): outlier_stat
                        }
                    )

                    var_sub_dict[year_stat.get_qaqc_check()] = year_stat
                    all_plots.append(plot_path)
                else:
                    # Get number of warnings and errors, write log,
                    # make status obj

                    # initiate dict for yearly statuses
                    year_status = {}

                    # Create is_percent status if unit is %
                    if var_obj.units == '%':
                        check_log = Logger().getLogger(
                            f'{self.qaqc_name}-{year}-'
                            f'{var_obj.name}-unit_check')
                        percent_ratio_stat = \
                            StatusGenerator().status_generator(
                                logger=check_log,
                                qaqc_check=check_log.getName())
                        percent_ratio_stat.add_summary_stat('is_percent', True)
                        year_status.update({
                            percent_ratio_stat.get_qaqc_check():
                            percent_ratio_stat})
                    else:
                        _log.info('Unit is not %. Not creating percent-ratio '
                                  'status object.')

                    n_warning = sum(
                        [var_obj.error_min <= i < var_obj.min_lim
                         or var_obj.max_lim < i <= var_obj.error_max
                         for i in annual_data
                         if not(np.ma.is_masked(i))])
                    n_error = sum([i < var_obj.error_min or
                                   var_obj.error_max < i for i in
                                   annual_data if not(np.ma.is_masked(i))])
                    total_count = len(annual_data)

                    # Create outlier status
                    check_log = Logger().getLogger(
                        f'{self.qaqc_name}-{year}-'
                        f'{var_obj.name}-outlier_check')
                    outlier_stat, plot_paths = self.get_status(
                        var_obj=var_obj, year=year, n_warnings=n_warning,
                        n_errors=n_error, log_obj=check_log,
                        total_count=total_count)

                    outlier_stat.add_summary_stats({
                        'hard_flag': round((n_error / total_count)*100, 3),
                        'soft_flag': round((n_warning / total_count)*100, 3)
                    })

                    year_status.update(
                        {outlier_stat.get_qaqc_check(): outlier_stat})

                    # Create the overall yearly status object
                    year_stat = StatusGenerator().composite_status_generator(
                        logger=yr_log, qaqc_check=yr_log.getName(),
                        plot_paths=[plot_paths],
                        statuses=year_status
                    )

                    var_sub_dict[year_stat.get_qaqc_check()] = year_stat
                    all_plots.extend(year_stat.get_plot_paths())
            # status object for variable
            status_objects.append(stat_gen.composite_status_generator(
                logger=var_log, qaqc_check=var_obj.name,
                plot_paths=all_plots, statuses=var_sub_dict,
                report_type='sub_status_row'))

        self.add_result_summary_stat(status_objects, d)
        self.write_summary(status_objects)
        return status_objects, self.plot_dir

    def identify_outliers(self, x_data, y_data, var_obj):
        out_error, out_warning = [], []
        for x, y in zip(x_data, y_data):
            if np.ma.is_masked(y):
                continue
            if var_obj.error_min <= y < var_obj.min_lim\
               or var_obj.max_lim < y <= var_obj.error_max:
                out_warning.append((x, y))
            if y < var_obj.error_min or var_obj.error_max < y:
                out_error.append((x, y))
        out_error = [list(x) for x in zip(*out_error)]
        out_warning = [list(x) for x in zip(*out_warning)]
        return out_error, out_warning

    def make_plot(self, ax, x_data, y_data, var_obj, year, thresholds=True):
        y_name = year.replace('_', ' ')

        if thresholds:
            range_type = 'Actual Data Range'
            y_max = np.nanmax(y_data)
            y_min = np.nanmin(y_data)
        else:
            range_type = 'Physical Range'
            if var_obj.error_max == float('inf'):
                y_max = np.nanmax(y_data)
            else:
                y_max = var_obj.error_max

            if var_obj.error_min == float('-inf'):
                y_min = np.nanmin(y_data)
            else:
                y_min = var_obj.error_min

        delta = (2 * var_obj.error)
        y_max += delta
        y_min -= delta
        title = f'Plot of {var_obj.name} with {range_type}'

        ax.set_ylim((y_min, y_max))
        ax.set_xlabel(f'Time ({y_name})',
                      fontsize=self.plot_config.plot_title_fontsize)
        ax.set_ylabel(f'{var_obj.name} ({var_obj.units})',
                      fontsize=self.plot_config.plot_title_fontsize)
        ax.set_title(title,
                     fontsize=self.plot_config.plot_title_fontsize)
        ax.plot_date(x_data, y_data, color='0.75', ms=2, ls='', lw=0)
        error_outliers, warning_outliers = \
            self.identify_outliers(x_data, y_data, var_obj)
        if error_outliers:
            ax.plot_date(error_outliers[0], error_outliers[1],
                         'o', markersize=6, markeredgewidth=1,
                         markerfacecolor='None', markeredgecolor='r')
        if warning_outliers:
            ax.plot_date(warning_outliers[0], warning_outliers[1],
                         'o', markersize=6, markeredgewidth=1,
                         markerfacecolor='None', markeredgecolor='orange')
        ax.axhline(y=var_obj.max_lim, linestyle='-',
                   linewidth=1, color='orange')
        ax.axhline(y=var_obj.min_lim, linestyle='-',
                   linewidth=1, color='orange')
        ax.axhline(y=var_obj.error_max, linestyle='-',
                   linewidth=1, color='r')
        ax.axhline(y=var_obj.error_min, linestyle='-',
                   linewidth=1, color='r')

    def plot(self, var_obj, year):
        start, end = self.annual_idx[year]['start'], \
            self.annual_idx[year]['end']
        annual_data = self.data[var_obj.name][start:end + 1]
        time_data = [self.ts_util.cast_as_datetime(t)
                     for t in self.data['TIMESTAMP_START'][start:end + 1]]
        fig, (subplot1, subplot2) = plt.subplots(
            2, 1, figsize=(12, 13))  # Create figure
        y_name = year.replace("_", " ")
        fig.suptitle(f'Physical Range of {var_obj.name} throughout {y_name}',
                     fontsize=self.plot_config.plot_suptitle_fontsize)
        self.make_plot(subplot1, time_data, annual_data, var_obj,
                       year, thresholds=True)
        self.make_plot(subplot2, time_data, annual_data, var_obj,
                       year, thresholds=False)
        lab_warn = f'Expected Range ({var_obj.min_lim}-{var_obj.max_lim})'
        margin_percent = str(var_obj.margin * 100)
        lab_err = (f'Expected Range +/- {margin_percent}% '
                   f'({var_obj.error_min}-{var_obj.error_max})')

        legend_info = [('orange', lab_warn), ('r', lab_err)]
        handles = [mlines.Line2D([], [], color=c, label=l)
                   for c, l in legend_info]  # convert legend info to Patches
        labels = [l for c, l in legend_info]

        # Add labels and colors for point styling
        handles += [mlines.Line2D([], [], color='None', marker='o',
                                  markerfacecolor='0.75', markersize=10,
                                  label='data'),
                    mlines.Line2D([], [], color='None', marker='o',
                                  markerfacecolor='None',
                                  markeredgecolor='orange',
                                  markersize=10,
                                  label=('data outside the expected '
                                         'physical range')),
                    mlines.Line2D([], [], color='None', marker='o',
                                  markerfacecolor='None', markeredgecolor='r',
                                  markersize=10,
                                  label=(
                                      'data outside the expected physical '
                                      f'range +/- {margin_percent}%'))]
        labels += ['data', 'data outside the expected physical range',
                   'data outside the expected physical range']
        fig.legend(handles, labels, loc='lower center', ncol=2)

        fig_name = self.fig_name_fmt.format(y=var_obj.name, year=year,
                                            s=self.site_id, p=self.process_id)
        fig_loc = os.path.join(self.plot_dir, fig_name)

        plt.savefig(fig_loc, dpi=self.plot_config.plot_default_dpi)
        plt.close()
        return fig_loc.replace(self.base_plot_dir, self.url_path)

    def get_status(self, var_obj=None, year='', n_warnings=0,
                   n_errors=0, log_obj=None, total_count=0):
        # use only for annual
        stat_gen = StatusGenerator()
        n_warnings = int(n_warnings)
        n_errors = int(n_errors)
        plot_paths = self.plot(var_obj, year)
        limits = (f'({str(var_obj.min_lim)}-{str(var_obj.max_lim)} '
                  f'{var_obj.units})')

        percent_warnings = n_warnings / total_count
        percent_errors = n_errors / total_count

        margin_percent = str((var_obj.margin) * 100)
        if percent_warnings > self.soft_flag_threshold and \
           percent_errors > self.hard_flag_threshold:
            status_message = (
                f'{n_errors} / {total_count} outside and '
                f'{n_warnings} / {total_count} outside'
                f' but within +/-{margin_percent}% of limits {limits}')
            log_obj.error(status_message)
        elif percent_errors > self.hard_flag_threshold and \
                percent_warnings <= self.soft_flag_threshold:
            status_message = (f'{n_errors} / {total_count} '
                              f'outside of limits {limits}')
            log_obj.error(status_message)
        elif percent_warnings > self.soft_flag_threshold and \
                percent_errors <= self.hard_flag_threshold:
            status_message = (f'{n_warnings} / {total_count} outside '
                              f'but within +/-{margin_percent}% '
                              f'of limits {limits}')
            log_obj.warning(status_message)
        elif percent_warnings <= self.soft_flag_threshold and \
                percent_errors <= self.hard_flag_threshold:
            status_message = None
        else:
            status_message = ('Unexpected error messaging. '
                              'Check Threshold code.')
        return stat_gen.status_generator(logger=log_obj,
                                         qaqc_check=log_obj.getName(),
                                         status_msg=status_message,
                                         plots=[plot_paths],
                                         report_type='single_msg'), plot_paths

    def add_result_summary_stat(self, status_objects, data_reader):
        # Collection of variables to ignore outlier_check warnings for
        variables_to_ignore = [
            'D_SNOW', 'PPFD_IN', 'PPFD_OUT', 'PPFD_BC_IN',
            'PPFD_BC_OUT', 'PPFD_DIF', 'PPFD_DIR', 'PPFD_UW_IN',
            'SW_IN', 'SW_OUT', 'SW_BC_IN', 'SW_BC_OUT', 'SW_DIF',
            'SW_DIR']
        # <VAR> NOT a variable with unit %
        fp_vars = FPVariables().get_fp_vars_dict()
        for var, unit in fp_vars.items():
            if '%' in unit:
                variables_to_ignore.append(var)

        for variable_status in status_objects:
            if variable_status.get_sub_status() is None:
                continue

            for year_status in variable_status.get_sub_status().values():
                if year_status.get_sub_status() is None:
                    continue

                for key, check_stat in year_status.get_sub_status().items():
                    *_, var, check = key.split('-')

                    base_var = data_reader.get_base_header(var)

                    result_code = check_stat.get_status_code()
                    if check == 'unit_check':
                        year_status.add_summary_stat(
                            'percent_ratio_result', result_code)
                    elif check == 'outlier_check':
                        if result_code == StatusCode.ERROR:
                            year_status.add_summary_stat(
                                'outlier_result', result_code)
                        elif result_code == StatusCode.WARNING and \
                                base_var not in variables_to_ignore:
                            year_status.add_summary_stat(
                                'outlier_result', result_code)
                        else:
                            year_status.add_summary_stat(
                                'outlier_result', StatusCode.OK)

    def write_summary(self, status_objects):
        table = OutputStats(status_objects, sort_by_header='var')

        summary_dir = os.path.join(self.base_plot_dir, 'summary')
        if not os.path.exists(summary_dir):
            os.makedirs(summary_dir)

        csv_headers = {
            'year': 'Period',
            'var': 'Variable',
            'percent_ratio_result': 'Result',
            'is_percent': 'Is percent',
            'figure': 'Figure link'
        }
        filename = os.path.join(
            summary_dir, f'{self.qaqc_name}_percent_ratio_summary.csv')
        table.write_to_csv(filename, csv_headers)

        csv_headers = {
            'year': 'Period',
            'var': 'Variable',
            'outlier_result': 'Result',
            'hard_flag': 'Hard flag (%)',
            'soft_flag': 'Soft flag (%)',
            'figure': 'Figure link'
        }
        filename = os.path.join(summary_dir, f'{self.qaqc_name}_summary.csv')
        table.write_to_csv(filename, csv_headers)
