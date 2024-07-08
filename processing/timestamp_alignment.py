#!/usr/bin/env python

import os
import sys
import itertools
import matplotlib
import datetime as dt
import numpy as np

from collections import Counter
from configparser import ConfigParser
from copy import deepcopy
from logger import Logger
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib import gridspec
from matplotlib import pyplot as plt
from matplotlib import ticker as plticker
from output_stats import OutputStats
from plot_config import PlotConfig
from status import StatusCode, StatusGenerator
from utils import TimestampUtil, StatsUtil, VarUtil

__author__ = ("Gilberto Z. Pastorello, Carlo Trotta, "
              "Alessio Ribeca, Sigrid Dengel, Dario Papale, "
              "You-Wei Cheah, Josh Geden, "
              "Danielle Christianson")
__email__ = ("gzpastorello@lbl.gov, trottacarlo@unitus.it, "
             "a.ribeca@unitus.it, sdengel@lbl.gov, darpap@unitus.it, "
             "ycheah@lbl.gov, joshgeden10@gmail.com, "
             "dschristianson@lbl.gov")

# removes smoothing, force plotting all points
matplotlib.rcParams['path.simplify'] = False

_log = Logger().getLogger(__name__)


class TimestampAlignmentError(Exception):
    """ Base error/exception class for timestamp_alignment """
    pass


class TimestampAlignment(object):
    """
    Plot measured / calculated radiations
    """

    def __init__(self):
        self.qaqc_name = 'timestamp_alignment'
        self.stats_util = StatsUtil()
        self.ts_util = TimestampUtil()
        self.plot_config = PlotConfig()
        self.ppfd_unit_convert, self.cross_cor_threshold, self.doy_interval, \
            self.night_buffer, self.n_row, self.n_col, self.dpi, \
            self.occasion_per_window_threshold \
            = self._get_params_from_config()
        self.fig_width, self.fig_height = 22, 16
        self.stat_gen = StatusGenerator()

    def _get_params_from_config(self):
        """ function to read in config file parameters """
        ppfd_unit_convert = None
        cross_cor_threshold = None
        doy_interval = None
        night_buffer = None
        n_row = None
        n_col = None
        dpi = None
        occasion_per_window_threshold = None

        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'TIMESTAMP_ALIGNMENT'
            if config.has_section(cfg_section):
                ppfd_unit_convert = config.getfloat(
                    cfg_section, 'ppfd_unit_convert')
                cross_cor_threshold = config.getfloat(
                    cfg_section, 'cross_cor_threshold')
                doy_interval = config.getfloat(cfg_section, 'doy_interval')
                night_buffer = config.getfloat(cfg_section, 'night_buffer')
                n_row = config.getint(cfg_section, 'n_plot_row')
                n_col = config.getint(cfg_section, 'n_plot_col')
                dpi = config.getint(cfg_section, 'fig_dpi')
                occasion_per_window_threshold = config.getfloat(
                    cfg_section, 'occasion_per_window_threshold')
            else:
                warning_msg = 'Cannot get parameters from config.'
                _log.warning(warning_msg)

        param_vars = ['ppfd_unit_convert', 'cross_cor_threshold',
                      'occasion_per_window_threshold', 'doy_interval',
                      'n_plot_row', 'n_plot_col', 'fig_dpi']
        _log.info('Params from config file')
        for k, v in locals().items():
            try:
                if k in param_vars:
                    _log.info(' '.join([str(k), str(v)]))
            except Exception:
                continue

        return (ppfd_unit_convert, cross_cor_threshold, doy_interval,
                night_buffer, n_row, n_col, dpi, occasion_per_window_threshold)

    def _get_starting_idx(self, last_doy_center, doy_interval, resolution):
        if last_doy_center is None:
            return 0
        next_doy_center = last_doy_center + dt.timedelta(days=doy_interval)
        first_day_of_next_year = dt.datetime(last_doy_center.year, 12, 31)
        diff = first_day_of_next_year - next_doy_center
        return -diff.days * self._get_step(resolution)

    def _get_step(self, resolution):
        if resolution == 'HH':
            return self.ts_util.NUMBER_OF_HOURS_IN_DAY * 2
        else:
            return self.ts_util.NUMBER_OF_HOURS_IN_DAY

    def _get_max_values_in_window(self, unique_time_pairs, mask_doy, hours,
                                  minutes, var, range_maxs, data):
        for h, m in unique_time_pairs:
            mask_time = mask_doy & (hours == h) & (minutes == m)
            if 'ppfd' in var.lower():
                range_maxs[var].append(
                    np.nanmax(data[mask_time][var])
                    * self.ppfd_unit_convert)
            else:
                range_maxs[var].append(
                    np.nanmax(data[mask_time][var]))

    def gen_timestamp_shift(
            self, data_reader, rem_sw_in_data, resolution,
            radiation_variables=['SW_IN', 'PPFD_IN'],
            output_dir='.', base_output_dir='.',
            output_fname_template='CC-SSS_timestamp_alignment',
            show=False, ftp_plot_dir=None):
        """
        Plots potential and measured radiations to look for shifts.
        Saves one figure per year with 18 subplots in each.

        Using method and parameters for computing monthly plot from
        FLUXNET pipeline:
        #../flux-processing/src/qc_auto/src/main.c:
        ...
        for ( doy_loop = 10;
              doy_loop < (dataset->leap_year ? 366 : 365);
              doy_loop += 20 ) ...
        ...
        if ( dataset->rows[i+y].value[SWIN] > maxs[i] ) {
             maxs[i] = dataset->rows[i+y].value[SWIN];
        }
        if ( dataset->rows[i+y].value[PPFD] > maxs_ppfd[i] ) {
             maxs_ppfd[i] = dataset->rows[i+y].value[PPFD];
        }
        ...

        and

        #../flux-processing/src/Dataset_obj2.m:
        figure2,[obj.dirGen,strrep(obj.dirQCV_ShiftHunter_plot_PI,'\',
        obj.filesep),obj.siteCode,'_qcv_shift_solar_noon_',obj.siteYear,'_',
        obj.varname_doy,'_',num2str(doySel(t))],'png');

        :param data_reader: data_reader object which has
            a numpy record array (NaN for missing)
        :type data_reader: data_reader object
        :param rem_sw_in_data: potential radiation data for portions of a year
            that are missing in the data
        :type rem_sw_in_data: array
        :param resolution: resolution of the data
        :type resolution: str
        :param radiation_variables: list of variables to be plotted
        :type radiation_variables: list (of str)
        :param output_dir: directory to save figures
        :type output_dir: str
        :param base_output_dir: base directory where the figures are saved
        :type base_output_dir: str
        :param output_fname_template: template for filename to be suffixed with
                                      year and .png extension
        :type output_fname_template: str
        :param show: flag to show interactive matplotlib plot
        :type show: bool
        :param ftp_plot_dir: base directory of the url path for figures
        :type ftp_plot_dir: str
        """
        var_util = VarUtil()
        data = data_reader.get_filled_data()
        t_start_label = 'TIMESTAMP_START'
        sw_in_pot_label = 'SW_IN_POT'
        color_palette = self.plot_config.hi_contrast_palette
        _c_dt = self.ts_util.cast_as_datetime
        radiation_variables_ext = radiation_variables + [sw_in_pot_label, ]
        status_objects = []
        all_plots = {}
        msg_combiner = '<br>'
        ptr = 0

        threshold_denom, sunrise_set_pts = 48.0, 6
        if resolution == 'HR':
            threshold_denom, sunrise_set_pts = 24.0, 6
        step = self._get_step(resolution)
        percent_threshold = (100 * self.occasion_per_window_threshold /
                             ((threshold_denom - sunrise_set_pts) / 2))

        for var in radiation_variables_ext:
            no_vars = []
            if var not in data.dtype.names:
                no_vars.append(var)
            if len(no_vars) > 0:
                no_vars_str = ' and '.join(no_vars)
                msg = (f'Variable {no_vars_str} not present. {_log.getName()} '
                       'analysis INCOMPLETE.')
                _log.fatal(msg)
                status_objects.append(
                    self.stat_gen.status_generator(
                        logger=_log, qaqc_check=_log.getName(),
                        status_msg=msg, report_section='high_level'))
                return status_objects
        base_rad_vars = [
            data_reader.get_base_header(v)
            for v in radiation_variables]
        top_level_rad_vars = []
        base_rad_vars_mapping = {}
        for b, v in zip(base_rad_vars, radiation_variables):
            if var_util.is_var_with_general_qualifiers(v):
                continue
            if base_rad_vars_mapping.get(b):
                base_rad_vars_mapping[b].append(v)
            else:
                base_rad_vars_mapping[b] = [v]
        for k in base_rad_vars_mapping:
            var = base_rad_vars_mapping.get(k)
            if len(var) == 1:
                top_level_rad_vars.append(var.pop())
            else:
                top_level_var_ls = var_util.get_top_level_variables(
                    var_ls=var, log=_log)
                lowest_h_var_ls = var_util.get_lowest_horiz_variables(
                    var_ls=top_level_var_ls, log=_log)
                top_level_var = var_util.get_lowest_r_variable(
                    top_level_var_ls=lowest_h_var_ls, log=_log)
                if top_level_var:
                    top_level_rad_vars.append(top_level_var)

        if top_level_rad_vars == []:
            log_obj = Logger().getLogger(self.qaqc_name)
            status_msg = ('No top level rad vars selected. '
                          'Skipping timestamp_alignment checks.')
            log_obj.info(status_msg)

            return [self.stat_gen.status_generator(
                logger=log_obj, qaqc_check=log_obj.getName(),
                status_msg=status_msg)]

        ts_start = [_c_dt(i) for i in data[t_start_label]]
        years = np.array([t.year for t in ts_start])
        doys = np.array([t.timetuple().tm_yday for t in ts_start])
        hours = np.array([t.hour for t in ts_start])
        minutes = np.array([t.minute for t in ts_start])

        first_ts = ts_start[0]
        last_ts = ts_start[-1]
        interval = last_ts - first_ts
        if interval.days < 28:
            msg = (f'Insufficient data ({interval} days from {first_ts} '
                   f'to {last_ts}). At least one month is required. '
                   f'{_log.getName()} not performed.')

            _log.info(msg)
            status_objects.append(self.stat_gen.status_generator(
                logger=_log, qaqc_check=_log.getName(),
                status_msg=msg, report_section='high_level'))
            return status_objects

        first_year, last_year = years[0], years[-1]
        interval = self.doy_interval // 2
        start_ts = first_ts
        ts_doy_center = None
        yr_sub_dict = {}
        for year in range(first_year, last_year + 1):
            daytime_comps = {}
            nighttime_comps = {}
            yr_log = Logger().getLogger(f'{self.qaqc_name}-{year}')
            yr_log.resetStats()
            if year != start_ts.year:
                start_ts = _c_dt(self.ts_util.get_ISO_str_timestamp(str(year)))
            mask = (years == year)

            days_in_year = sum(mask) / step
            if days_in_year < self.doy_interval + 1:
                yr_log.info('Insufficient data for analysis.')
                status_objects.append(self.stat_gen.composite_status_generator(
                    logger=yr_log, qaqc_check=yr_log.getName(),
                    report_type='sub_status_row'))
                continue

            annual_sw_in_pot = []
            annual_var_data = {}
            for v in radiation_variables:
                annual_var_data[v] = []
                daytime_comps[v] = []
                nighttime_comps[v] = []
            plt.close('all')
            output_filename = f'{output_fname_template}_{year}.png'
            fig_filename = os.path.join(output_dir, output_filename)
            figure = plt.figure()
            suptitle = f'Timestamp Alignment Analysis for year {year}'
            figure.suptitle(suptitle,
                            fontsize=self.plot_config.plot_title_fontsize)
            figure.text(
                0.5, .95, 'DATE_START - DATE_END', ha='center', va='center')
            figure.set_figwidth(self.fig_width)
            figure.set_figheight(self.fig_height)
            figure.text(0.5, 0.04, t_start_label, ha='center', va='center')
            canvas = FigureCanvas(figure)
            gs = gridspec.GridSpec(self.n_row, self.n_col)
            gs.update(
                left=0.06, right=0.96, top=0.94, bottom=0.08,
                hspace=0.01, wspace=0.01)
            unique_hours = np.unique(hours)
            unique_minutes = np.unique(minutes)
            unique_time_pairs = [
                (i, j) for i in unique_hours for j in unique_minutes]
            last_row_idx = (self.n_row - 1) * self.n_col
            y_label = 'Radiation (W m-2)'

            for i, range_day in enumerate(
                    range(round(interval), 360 + 1, int(self.doy_interval))):
                start_date = self.ts_util.get_ISO_date_from_datetime(
                    start_ts, self.ts_util.DATE_ONLY_TS_FORMAT)
                ax = plt.subplot(gs[i // self.n_col, i % self.n_col])
                ax.set_ylim(-100, 1500)
                props = dict(
                    boxstyle='round', facecolor='#eae3dd',
                    edgecolor='none', alpha=0.7)
                end_ts = start_ts + dt.timedelta(days=self.doy_interval)
                if (end_ts + dt.timedelta(days=self.doy_interval)).year > year:
                    end_date = self.ts_util.get_ISO_date_from_datetime(
                        dt.timedelta(days=-1)
                        + _c_dt(self.ts_util.get_ISO_str_timestamp(
                            str(year + 1))),
                        self.ts_util.DATE_ONLY_TS_FORMAT)
                else:
                    end_date = self.ts_util.get_ISO_date_from_datetime(
                        end_ts, self.ts_util.DATE_ONLY_TS_FORMAT)
                ax.text(
                    0.5, .98, f'{start_date} - {end_date}',
                    transform=ax.transAxes, fontsize=12, ha='center', va='top',
                    color='#997755', bbox=props)

                # Reassign first timestamp for next window
                start_ts = end_ts

                formatter = matplotlib.dates.DateFormatter('%H:%M')
                loc = plticker.MultipleLocator(base=0.125)
                if i == last_row_idx:
                    ax.tick_params(
                        bottom=True, top=False, left=True, right=False,
                        labelleft=True, labelbottom=True)
                    # bottom='on', top='off', left='on', right='off',
                    # labelleft='on', labelbottom='on')
                    ax.set_ylabel(y_label)
                    ax.xaxis.set_major_formatter(formatter)
                    xaxis = ax.xaxis
                    xaxis.set_major_locator(loc)
                    for idx, label in enumerate(xaxis.get_ticklabels()):
                        label.set(rotation=90)
                elif (i % self.n_col) == 0:
                    ax.tick_params(
                        bottom=False, top=False, left=True, right=False,
                        labelleft=True, labelbottom=False)
                    # bottom='off', top='off', left='on', right='off',
                    # labelleft='on', labelbottom='off')
                    ax.set_ylabel(y_label)
                elif i > last_row_idx:
                    ax.tick_params(
                        bottom=True, top=False, left=False, right=False,
                        labelleft=False, labelbottom=True)
                    # bottom='on', top='off', left='off', right='off',
                    # labelleft='off', labelbottom='on')
                    ax.xaxis.set_major_formatter(formatter)
                    xaxis = ax.xaxis
                    xaxis.set_major_locator(loc)
                    for idx, label in enumerate(xaxis.get_ticklabels()):
                        label.set(rotation=90)
                else:
                    ax.tick_params(
                        bottom=False, top=False, left=False, right=False,
                        labelleft=False, labelbottom=False)
                    # bottom='off', top='off', left='off', right='off',
                    # labelleft='off', labelbottom='off')
                mask_doy = mask & (
                    (doys >= range_day - interval)
                    & (doys <= range_day + interval))
                mask_doy_center = mask & (doys == range_day)
                idx_doy_center = np.where(mask_doy_center)[0]

                # set up dictionary for max values
                range_maxs = {v: [] for v in radiation_variables_ext}

                # Check for timestamps in the window
                if len(idx_doy_center) == 0:
                    msg = (f'Plotting data from {sw_in_pot_label} for rest of '
                           f'year {year}: window start date {start_date}')
                    _log.info(msg)

                    if ptr == 0:
                        ptr = self._get_starting_idx(
                            ts_doy_center[0], self.doy_interval, resolution)
                    else:
                        ptr += int(self.doy_interval) * step
                    # We can use a generic x-axis
                    ts_doy_center = ts_start[0:step]

                    if rem_sw_in_data is not None:
                        leg_pot, = ax.plot_date(
                            ts_doy_center, rem_sw_in_data[ptr:ptr+step],
                            lw=1.0, ls='-', marker='.', markersize=3,
                            color='red', markeredgecolor='red',
                            alpha=1.0, label=sw_in_pot_label)
                    continue
                else:
                    # get max values for the potential radiation
                    self._get_max_values_in_window(
                        unique_time_pairs, mask_doy, hours,
                        minutes, sw_in_pot_label, range_maxs, data)

                    sw_in_pot_data = range_maxs[sw_in_pot_label]
                    n_sw_in_pot = len(sw_in_pot_data)
                    annual_sw_in_pot.extend(sw_in_pot_data)

                    if len(idx_doy_center) != step:
                        ts_doy_center = [d + dt.timedelta(days=range_day-1)
                                         for d in ts_start[0:step]]
                        full_doy_center = False
                        # data_doy_center_sw_in_pot = \
                        #     data[mask_doy_center][sw_in_pot_label]
                        # data_as_list = data_doy_center_sw_in_pot.tolist()
                        data_as_list = deepcopy(sw_in_pot_data)
                        while len(data_as_list) < step:
                            data_as_list.append(np.NaN)
                        data_doy_center_sw_in_pot = np.asarray(data_as_list)
                    else:
                        ts_doy_center = ts_start[
                            idx_doy_center[0]:idx_doy_center[-1] + 1]
                        full_doy_center = True
                        # data_doy_center_sw_in_pot = \
                        #     data[mask_doy_center][sw_in_pot_label]
                        data_doy_center_sw_in_pot = np.asarray(sw_in_pot_data)

                leg_pot, = ax.plot_date(
                    ts_doy_center, data_doy_center_sw_in_pot,
                    linewidth=1.0, linestyle='-', marker='.', markersize=3,
                    fmt='r', markeredgecolor='red',
                    alpha=1.0, label=sw_in_pot_label)

                # get max values for the radiation variables
                for radiation_variable in radiation_variables:
                    self._get_max_values_in_window(
                        unique_time_pairs, mask_doy, hours,
                        minutes, radiation_variable, range_maxs, data)

                leg_vars = {}
                leg_vars_less = {}

                for i, var in enumerate(radiation_variables):
                    annual_var_data[var].extend(range_maxs[var])
                    var_data = np.asarray(range_maxs[var])
                    leg_vars[var], = ax.plot_date(
                        ts_doy_center, var_data, linewidth=1.0,
                        fmt='.-', markersize=3,
                        color=color_palette[i],
                        markeredgecolor=color_palette[i],
                        alpha=1.0, label=var)
                    nighttime = []
                    daytime = []
                    idx = 0
                    daytime_mode = False
                    while idx < n_sw_in_pot:
                        cur_val = var_data[idx]
                        # determine day or night
                        # skipping the transition timestamp along with the
                        # one prior and after the transition
                        if idx + 2 < n_sw_in_pot:
                            # for the transition to day:
                            # the cur_val is before the transition (==0)
                            if (sw_in_pot_data[idx] == 0
                                    # the one ahead is the transition (>0)
                                    and sw_in_pot_data[idx+1] > 0
                                    # the one 2 ahead is light (>0)
                                    and sw_in_pot_data[idx+2] > 0):
                                # switch to daytime
                                daytime_mode = True
                                # set index beyond the transition
                                idx += 3
                                # skip cur_val and jump beyond transition
                                # thus 3 timesteps are skipped
                                continue
                            # for transition back to night
                            # if the cur_val > 0
                            if (sw_in_pot_data[idx] > 0
                                    # the one ahead is the transition (>0)
                                    and sw_in_pot_data[idx+1] > 0
                                    # the one 2 ahead is dark (==0)
                                    and sw_in_pot_data[idx+2] == 0):
                                # switch to night
                                daytime_mode = False
                                # set index beyond transition
                                idx += 3
                                # skip cur_val and jump beyond transition
                                # thus 3 timesteps are skipped
                                continue
                        if daytime_mode and not np.isnan(cur_val):
                            daytime.append(cur_val > sw_in_pot_data[idx])
                        else:
                            if not np.isnan(cur_val):
                                # Flag a night time point if it is > SW_IN_POT
                                # and > than the night buffer value
                                nighttime.append(
                                    cur_val > sw_in_pot_data[idx] and
                                    cur_val > self.night_buffer)
                        idx += 1
                    daytime_comps[var].extend(daytime)
                    nighttime_comps[var].extend(nighttime)
                    if full_doy_center:
                        var_data_lt = (var_data > sw_in_pot_data)
                    else:
                        var_data_lt = []
                        idx = 0
                        while idx < step:
                            if idx > len(sw_in_pot_data) - 1:
                                var_data_lt.append(False)
                            elif var_data[idx] > sw_in_pot_data[idx]:
                                var_data_lt.append(True)
                            else:
                                var_data_lt.append(False)
                            idx += 1
                    var_data[np.invert(var_data_lt)] = np.NaN
                    leg_vars_less[var], = ax.plot_date(
                        ts_doy_center, var_data, linewidth=1.0,
                        fmt='o', linestyle='', markersize=8,
                        color=color_palette[i],
                        markeredgecolor=color_palette[i],
                        markeredgewidth=1.5, markerfacecolor='none',
                        alpha=1.0, label=var)

            has_valid_plt = sum(v == [] for v in annual_var_data.values()) == 0
            if has_valid_plt:
                fig_url = fig_filename.replace(base_output_dir, ftp_plot_dir)
                if year not in all_plots:
                    all_plots[year] = []
                all_plots[year].append(fig_url)
                fig_url = [fig_url]
            else:
                fig_url = None
            _log.info(f'Top level variables {top_level_rad_vars}')
            text = []
            for var in top_level_rad_vars:
                ccorr_log = Logger().getLogger(f'{_log.getName()}-{year}-'
                                               f'{var}-ccorr')
                stat_msgs = []
                summary_stats = {}
                var_data = annual_var_data.get(var)
                is_valid, n_nan_annual_sw_in_pot_pts, n_nan_var_pts = \
                    self.stats_util.is_valid_input_for_corr_cond(
                        annual_sw_in_pot, var_data)
                check_statuses = {}
                if not is_valid:
                    msg = 'Insufficient data for calculating cross correlation'
                    ccorr_log.info(msg)
                    text.append(f'{var}: {msg}')
                    stat_msgs.append(f'{msg} (Number of nans = '
                                     f'{str(n_nan_var_pts)}).')

                    summary_stats['corr'] = 'not_calculated'
                    summary_stats['time_lag'] = 'not_calculated'
                else:
                    lag = None
                    lags, corr = self.stats_util.ccorr(
                        annual_sw_in_pot, var_data, is_HR=(resolution == 'HR'))
                    lag = int(lags[0])
                    max_corr = -float('inf')
                    for l_lag, c in zip(lags, corr):
                        try:
                            if float(c) > max_corr:
                                lag = int(l_lag)
                                max_corr = float(c)
                        # Ignore if c cannot be cast to float
                        except ValueError:
                            pass
                    info_msg = f'Cross-correlation with {sw_in_pot_label}'
                    ccorr_log.info(info_msg)
                    if lag is not None:
                        if (abs(float(max_corr)) < self.cross_cor_threshold or
                                max_corr == -float('inf')):
                            msg = 'No significant cross correlation was found'
                            fig_msg = (f'{var}: {msg}')
                            ccorr_log.info(msg)
                            stat_msgs.append(msg)
                            summary_stats['time_lag'] = lag
                            summary_stats['corr'] = 'not_found'
                        else:
                            fig_msg = (
                                f'{var} has max corr {max_corr:.3f} at '
                                f'lag {lag}')
                            msg = (f'Max correlation {max_corr:.3f} at '
                                   f'lag {lag}')
                            if abs(max_corr) > self.cross_cor_threshold \
                                    and max_corr < 0 or abs(lag) > 0:
                                ccorr_log.warning(msg)
                                stat_msgs.append(msg)
                            else:
                                ccorr_log.info(msg)
                            summary_stats['time_lag'] = lag
                            summary_stats['corr'] = max_corr
                        text.append(fig_msg)
                    else:
                        msg = 'Lag value not found'
                        ccorr_log.info(msg)
                        stat_msgs.append(msg)

                        summary_stats['time_lag'] = 'not_found'
                        if max_corr == -float('inf'):
                            summary_stats['corr'] = 'not_found'
                        else:
                            summary_stats['corr'] = max_corr

                if len(stat_msgs) > 1:
                    stat_msgs[0] = stat_msgs[0]
                    # stat_msgs.insert(0, f'{var}: ')
                    stat_msgs = msg_combiner.join(stat_msgs)
                elif len(stat_msgs) > 0:
                    stat_msgs = stat_msgs[0]
                else:
                    stat_msgs = None

                if stat_msgs == 'None':
                    stat_msgs = None

                stat = self.stat_gen.status_generator(
                    logger=ccorr_log, qaqc_check=ccorr_log.getName(),
                    status_msg=stat_msgs, report_type='single_msg',
                    plots=fig_url)
                stat.add_summary_stats(summary_stats)
                check_statuses[stat.get_qaqc_check()] = stat

                day_stat_log = Logger().getLogger(
                    f'{_log.getName()}-{year}-{var}-daystats')
                night_stat_log = Logger().getLogger(
                    f'{_log.getName()}-{year}-{var}-nightstats')
                daytime_stats = Counter(daytime_comps[var])
                nighttime_stats = Counter(nighttime_comps[var])
                daytime_gt = daytime_stats.get(True, 0)
                nighttime_gt = nighttime_stats.get(True, 0)
                total_daytime = daytime_gt + daytime_stats.get(False, 0)
                total_nighttime = nighttime_gt + nighttime_stats.get(False, 0)

                yr_log.info(f'Stats for variable: {var}')
                percent_threshold_str = '%.3f' % percent_threshold
                yr_log.info(f'Percent threshold: {percent_threshold_str}%')
                threshold_occasion = ((threshold_denom - sunrise_set_pts)
                                      * self.n_col * self.n_row * 0.5
                                      / threshold_denom)
                threshold_occasion_str = '%.1f' % threshold_occasion
                yr_log.info('Threshold equates to roughly '
                            f'{threshold_occasion_str} occasions per window')
                percent_day = None
                percent_night = None
                if total_daytime > 0:
                    percent_day = \
                        round(100 * float(daytime_gt) / total_daytime, 3)
                    percent_day_str = '%.3f' % percent_day
                    day_msg = ('Percent of daytime timestamps exceeding '
                               f'SW_IN_POT {percent_day_str}%')

                if total_nighttime > 0:
                    percent_night = \
                        round(100 * float(nighttime_gt) / total_nighttime, 3)
                    percent_night_str = '%.3f' % percent_night
                    night_msg = ('Percent of nighttime timestamps exceeding '
                                 f'SW_IN_POT {percent_night_str}%')

                # Check if daytime stats were not calculated
                if percent_day is None:
                    day_msg = 'Daytime stats could not be calculated'
                    day_stat_log.warning(day_msg)
                # Otherwise log the appropriate message
                else:
                    if percent_day > percent_threshold:
                        day_stat_log.error(day_msg)
                    elif percent_day > 0:
                        day_stat_log.warning(day_msg)
                    else:
                        day_stat_log.info(day_msg)
                        day_msg = None

                stat = self.stat_gen. \
                    status_generator(logger=day_stat_log,
                                     qaqc_check=day_stat_log.getName(),
                                     status_msg=day_msg,
                                     report_type='single_msg',
                                     plots=fig_url)

                if percent_day is not None:
                    stat.add_summary_stat('day_rad', percent_day)

                check_statuses[stat.get_qaqc_check()] = stat

                # Check if nighttime stats were not calculated
                if percent_night is None:
                    night_msg = 'Nighttime stats could not be calculated'
                    night_stat_log.warning(night_msg)
                else:
                    if percent_night > percent_threshold:
                        night_stat_log.error(night_msg)
                    elif percent_night > 0:
                        night_stat_log.warning(night_msg)
                    else:
                        night_stat_log.info(night_msg)
                        night_msg = None

                stat = self.stat_gen. \
                    status_generator(logger=night_stat_log,
                                     qaqc_check=night_stat_log.getName(),
                                     status_msg=night_msg,
                                     report_type='single_msg',
                                     plots=fig_url)

                if percent_night is not None:
                    stat.add_summary_stat('night_rad', percent_night)

                check_statuses[stat.get_qaqc_check()] = stat

                # Create a summary status for the overall variable
                log_obj = Logger().getLogger(f'{self.qaqc_name}-{year}-{var}')
                var_status = self.stat_gen.composite_status_generator(
                    logger=log_obj,
                    qaqc_check=log_obj.getName(),
                    status_msg=None,
                    plot_paths=fig_url,
                    statuses=check_statuses)

                if year not in yr_sub_dict:
                    yr_sub_dict[year] = {}
                yr_sub_dict[year][var_status.get_src_logger_name()] = \
                    var_status

            decorators = [leg_pot, ]
            labels = [sw_in_pot_label, ]
            for var in radiation_variables:
                decorators += [leg_vars[var], leg_vars_less[var]]
                labels += [var, f'{var} > {sw_in_pot_label}']
            corr_annotation = '\n'.join(text)
            yr_log.info('\n' + corr_annotation)
            figure.text(.975, .975, corr_annotation, ha='right', va='center')
            # TODO: Subtract 3 to stay within figure. Should use a better way.
            figure.legend(
                decorators, labels, loc='lower center',
                ncol=min(len(radiation_variables)*2 + 1,
                         (self.fig_width // 2) - 3))

            if show and has_valid_plt:
                yr_log.info(f'Timestamp shift: showing {year}')
                plt.show()

            if has_valid_plt:
                canvas.print_figure(fig_filename, dpi=self.dpi)
                yr_log.info(f'Timestamp shift: saved {fig_filename}')
            else:
                yr_log.info('No figure to save')

            # Append an overall status object for the year/variable pair
            status_objects.append(self.stat_gen.composite_status_generator(
                logger=yr_log, qaqc_check=yr_log.getName(),
                plot_paths=all_plots[year],
                statuses=yr_sub_dict[year],
                report_type='sub_status_row'))

        return status_objects

    def driver(self, data_reader, rem_sw_in_data, site_id, resolution,
               radiation_vars=['SW_IN', 'PPFD_IN'], output_dir='.',
               ftp_plot_dir=None):
        output_fname_template = f'{site_id}_{self.qaqc_name}'
        base_output_dir = output_dir
        output_dir = self.plot_config.get_plot_dir_for_check(
            output_dir, __name__)
        if 360 // self.doy_interval != (self.n_row * self.n_col):
            msg = 'Number of plot rows and cols do not map to doy interval'
            _log.fatal(msg)
            stats = self.stat_gen.status_generator(
                logger=_log, qaqc_check=_log.getName(),
                status_msg=msg, report_section='high_level')
            self.write_summary(stats, base_output_dir)
            return [stats], output_dir
        radiation_vars = [
            data_reader.base_headers[rv]
            for rv in radiation_vars
            if rv in data_reader.base_headers]
        # flatten list
        radiation_vars = list(itertools.chain(*radiation_vars))
        stats = self.gen_timestamp_shift(
            data_reader=data_reader,
            rem_sw_in_data=rem_sw_in_data,
            radiation_variables=radiation_vars,
            resolution=resolution,
            output_dir=output_dir,
            base_output_dir=base_output_dir,
            output_fname_template=output_fname_template,
            ftp_plot_dir=ftp_plot_dir)

        self.add_result_summary_stat(stats)
        self.write_summary(stats, base_output_dir)
        return stats, output_dir

    def add_result_summary_stat(self, status_objects):
        for yearly_status in status_objects:
            if yearly_status.get_sub_status() is None:
                continue

            # Loop through each var status
            for var_status in yearly_status.get_sub_status().values():
                worst_stat = {
                    'ccorr': StatusCode.OK,
                    'daystats': StatusCode.OK,
                    'nightstats': StatusCode.OK}

                result_code = StatusCode.OK

                # Loop through each check status
                for key, check_stat in var_status.get_sub_status().items():
                    check = key.split('-')[-1]

                    worst_stat[check] = min([worst_stat[check],
                                             check_stat.get_status_code()])

                # Error if
                # Both are true:
                # -- max_corr above threshold and timelag > 0 (ccorr = WARNING)
                # -- 0 < p_day < threshold or 0 < p_night < threshold (WARNING)
                # OR
                # -- p_day > threshold OR p_night > threshold (ERROR)
                if ((worst_stat['ccorr'] == StatusCode.WARNING
                        and (worst_stat['nightstats'] == StatusCode.WARNING
                             or worst_stat['daystats'] == StatusCode.WARNING))
                        or worst_stat['nightstats'] == StatusCode.ERROR
                        or worst_stat['daystats'] == StatusCode.ERROR):
                    result_code = StatusCode.ERROR
                # Warning if
                # -- max_corr above threshold and timelag > 0 (ccorr = WARNING)
                # OR
                # -- 0 < p_day < threshold or 0 < p_night < threshold (WARNING)
                elif any([stat == StatusCode.WARNING
                          for stat in worst_stat.values()]):
                    result_code = StatusCode.WARNING

                var_status.add_summary_stat('result', result_code)

    def write_summary(self, status_objects, base_output_dir):
        # Set the path and filename for where to write csv summary
        summary_dir = os.path.join(base_output_dir, 'summary')
        filename = os.path.join(summary_dir, f'{self.qaqc_name}_summary.csv')

        # Map internal summary_stats keys to desired CSV headers
        csv_headers = {
            'year': 'Period',
            'var': 'Variable',
            'result': 'Result',
            'time_lag': 'Time lag',
            'corr': 'Cross correlation',
            'day_rad': 'Excessive daytime radiation (%)',
            'night_rad': 'Excessive nighttime radiation (%)',
            'figure': 'Figure link'
        }

        table = OutputStats(status_objects, sort_by_header='var')
        table.write_to_csv(filename, csv_headers)


if __name__ == '__main__':
    sys.exit('Not to be run directly')
