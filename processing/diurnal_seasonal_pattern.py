#!/usr/bin/env python

import collections
import csv
import datetime as dt
import math
import matplotlib.lines as mlines
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
import numpy as np
import os
import status
import sys
import warnings

from configparser import ConfigParser
from fp_vars import FPVariables
from logger import Logger
from numpy.ma.core import MaskedConstant
from output_stats import OutputStats
from path_util import PathUtil
from plot_config import PlotConfig
from status import StatusCode
from utils import Decode
from utils import TimestampUtil, StatsUtil, VarUtil


__author__ = 'You-Wei Cheah, Josh Geden'
__email__ = 'ycheah@lbl.gov, joshgeden10@gmail.com'

_log = Logger().getLogger(__name__)


class DiurnalSeasonalPattern():

    def __init__(self, site_id, process_id, resolution,
                 plot_dir=None, day_interval=30, ftp_plot_dir=None):
        """Constructor for initializing various variables"""
        self.ts_util = TimestampUtil()
        self.path_util = PathUtil()
        self.stats_util = StatsUtil()
        self.var_util = VarUtil()
        self.stat_gen = status.StatusGenerator()
        self.decode = Decode()
        self.plot_config = PlotConfig()
        self.fig_name_fmt = '{s}-{p}-{t}-{x}-{yr}.png'
        self.day_interval = day_interval
        self.site_id = site_id
        self.resolution = resolution
        self.process_id = process_id
        self.gray_color = '.75'
        # self.color_palette = ('k', 'r', 'c', 'm')
        self.color_palette = self.plot_config.diurnal_palette

        self.hist_dir_path, self.outer_band_error_threshold, \
            self.outer_band_warning_threshold, \
            self.inner_band_error_threshold, \
            self.inner_band_warning_threshold, self.cross_cor_threshold = \
            self.get_params_from_config()

        self.has_historical = False
        self._calculated_median = 'Calculated Median'
        self.hist_names = ('LOWER1', 'LOWER2', 'MEDIAN', 'UPPER1', 'UPPER2')
        self.hist_names_map = collections.OrderedDict()
        self.hist_names_map['LOWER2'] = '2.5% percentile ({ver})'
        self.hist_names_map['LOWER1'] = '25% percentile ({ver})'
        self.hist_names_map['MEDIAN'] = 'MEDIAN ({ver})'
        self.hist_names_map[self._calculated_median] = 'MEDIAN (current data)'
        self.hist_names_map['UPPER1'] = '75% percentile ({ver})'
        self.hist_names_map['UPPER2'] = '97.5% percentile ({ver})'
        self.doy_var = 'DOY2'
        self.hr_var = 'HR2'

        if plot_dir:
            self.can_plot = True
            self.base_plot_dir = plot_dir
            self.plot_dir = self.plot_config.get_plot_dir_for_check(
                plot_dir, __name__)
            if not os.path.exists(self.plot_dir):
                os.mkdir(self.plot_dir)
        else:
            self.can_plot = False
        self.url_path = ftp_plot_dir
        self.fp_vars = FPVariables().get_fp_vars_dict()
        self.qaqc_name = 'diurnal_seasonal_pattern'

    def find_year_indices(self, ts):
        """Find starting indices for each year"""
        ts_indices = []
        for idx, t in enumerate(ts):
            if self.decode.byte_to_str(t).endswith('01010000') or idx == 0:
                ts_indices.append(idx)

        return ts_indices

    def find_vars(self, hist_vars_with_qualifiers):
        var_ls = []
        for base_candidate_var_name in self.input_data.dtype.names:
            if base_candidate_var_name in hist_vars_with_qualifiers:
                var_ls.append(base_candidate_var_name)
        return var_ls

    def partitioning_by_days(self, ts_vals, day_interval, max_intervals=12):
        """Return indices for each starting 30 day period.
        Max intervals is for limiting the number of intervals and
        clumping the last number of values together"""
        day_delta = dt.timedelta(days=day_interval)

        _c_dt = self.ts_util.cast_as_datetime
        dt_start_vals = [_c_dt(ts) for ts in ts_vals]

        day_idxs = []
        last_dt = None
        for idx, dt_start in enumerate(dt_start_vals):
            if not last_dt:
                last_dt = dt_start
                day_idxs.append(idx)
            else:
                if len(day_idxs) == max_intervals:
                    break
                if (dt_start - last_dt) == day_delta:
                    last_dt = dt_start
                    day_idxs.append(idx)
                else:
                    continue
        return day_idxs

    def _get_start_end_idxs(self, idxs, vals):
        """Simple helper to return a list of the correct start
        and end indices
        """
        result = []
        for idx, v in enumerate(idxs):
            if idx + 1 < len(idxs):
                s = idxs[idx]
                e = idxs[idx + 1]
            else:
                s = idxs[idx]
                e = len(vals)
            result.append((s, e))
        return result

    def processor(self, site_id, var_name):
        """Main processor"""
        var_log = Logger().getLogger(f'{_log.getName()}-{var_name}')
        var_log.resetStats()
        var_sub_dict = {}
        all_plots = []
        ts_vals = self.input_data['TIMESTAMP_START']
        val = self.input_data[var_name]

        ts_idx = self.find_year_indices(ts_vals)
        year_chunks = []

        timestamps_per_month = 24 * 28  # 24 hrs a day, min 28 days a month
        if self.resolution == 'HH':
            timestamps_per_month *= 2

        timestamps_per_year = 24 * 365
        if self.resolution == 'HH':
            timestamps_per_year *= 2

        for s, e in self._get_start_end_idxs(ts_idx, ts_vals):
            if e - s >= timestamps_per_month:
                is_full_year = e - s >= timestamps_per_year
                year_chunks.append(((ts_vals[s:e], val[s:e]), is_full_year))

        if year_chunks == []:
            qaqc_check = f'{self.qaqc_name}-all_data'
            log_obj = Logger().getLogger(qaqc_check)
            status_msg = ('Less than 30 days of data provided; not running '
                          'diurnal seasonal pattern check.')
            log_obj.info(status_msg)

            return self.stat_gen.status_generator(
                logger=log_obj,
                qaqc_check=qaqc_check,
                status_msg=status_msg
            )

        for year_chunk, full_year in year_chunks:
            fig_loc, year, yr_stat = self.process_by_year(
                var_name, site_id, var_log, *year_chunk, full_year=full_year)
            if fig_loc:
                all_plots.append(fig_loc)
            var_sub_dict[year] = yr_stat
        return self.stat_gen.composite_status_generator(
            logger=var_log,
            qaqc_check=var_name, plot_paths=all_plots,
            statuses=var_sub_dict, report_type='sub_status_row')

    def _gen_legend_handles(self, hist_ver=None):
        legend_info = []
        legend_color_map = {self._calculated_median: self.color_palette[0]}
        if hist_ver:
            for n in range(len(self.hist_names)):
                # legend_color_map[self.hist_names[n]] = self.color_palette[
                #     (n % 3) + 1]
                legend_color_map[self.hist_names[n]] = self.color_palette[1]
            for k, v in self.hist_names_map.items():
                v = v.format(ver=hist_ver)
                if k == 'MEDIAN' or k == self._calculated_median:
                    linewidth = 3
                elif k == 'LOWER1' or k == 'UPPER1':
                    linewidth = 2
                else:
                    linewidth = 1
                legend_info.append((legend_color_map.get(k), v, linewidth))

        else:
            legend_info.append(
                (legend_color_map.get(self._calculated_median),
                 self.hist_names_map.get(self._calculated_median), 2))
        handles = [mlines.Line2D(
            [], [], color='None', marker='o', markeredgecolor='None',
            markerfacecolor='0.75', markersize=10, label='data')]
        labels = ['Current Data']
        for c, l, width in legend_info:
            handle = mlines.Line2D([], [], color=c, lw=width)
            handles.append(handle)
        labels += [l for c, l, w in legend_info]
        return handles, labels

    def process_by_year(self, var_name, site_id, var_log,
                        ts, val, full_year=True):
        # Set up annual logger
        _c_dt = self.ts_util.cast_as_datetime
        year = _c_dt(ts[0]).year
        yr_log = Logger().getLogger(f'{self.qaqc_name}-{year}-{var_name}')

        # Need to reset counts after every year
        n_outside_outer_band, n_inside_inner_band = 0, 0
        n_pts = 0
        factor = 0
        processed_ts, processed_val = [], []
        subplot_cols, subplot_rows = 4, 3
        i, j = 0, 0

        fig, axarr = plt.subplots(
            subplot_rows, subplot_cols, sharey=True)

        # Setup plot attributes
        fig.set_size_inches(16, 12)
        # suptitle = ('Diurnal Seasonal Pattern Analysis '
        #             f'of {var_name} for year {year}')
        # fig.suptitle(suptitle, fontsize=self.plot_config.plot_title_fontsize)

        text1 = 'Diurnal Seasonal Pattern Analysis | '
        text2 = f'{var_name}'
        text3 = f' | {year}'

        # Calculate the widths of each text segment
        renderer = fig.canvas.get_renderer()
        bbox1 = fig.text(0, 0, text1, ha='left',
                         va='top', fontsize=16
                         ).get_window_extent(renderer=renderer)
        bbox2 = fig.text(0, 0, text2, ha='left',
                         va='top', fontsize=16, fontweight='bold'
                         ).get_window_extent(renderer=renderer)

        # Starting x position
        x_start = 0.01

        # Set the text positions dynamically based on the calculated widths
        fig.text(x_start, 0.99, text1, ha='left', va='top', fontsize=16)
        fig.text(x_start + bbox1.width / fig.bbox.width,
                 0.99, text2, ha='left', va='top',
                 fontsize=16, fontweight='bold')
        fig.text(x_start + (bbox1.width + bbox2.width) / fig.bbox.width,
                 0.99, text3, ha='left', va='top', fontsize=16)
        plt.subplots_adjust(top=0.80)

        # Set common labels
        base_var_name = self.d.get_base_header(var_name)
        y_axis_label = var_name + ' (' + self.fp_vars.get(base_var_name) + ')'
        fig.text(0.5, 0.85, 'Date Start - Date End', ha='center', va='center')
        fig.text(0.5, 0.05, 'TIMESTAMP_START', ha='center', va='center')
        fig.text(0.09, 0.5, y_axis_label,
                 ha='center', va='center', rotation='vertical')

        # Get the necessary indices to partition data by a certain interval
        day_idx = self.partitioning_by_days(ts, self.day_interval)
        yr_log.info(f'Processing {var_name} for {year}')
        s_e_idxs = self._get_start_end_idxs(day_idx, ts)
        d30_x = []
        d30_y = []
        derived_med = []
        start_time = ts[0]
        x_lab = {}
        new_x_lab = []
        for s, e in s_e_idxs:
            cur_interval_ts = ts[s:e]
            cur_interval_val = val[s:e]
            subplot_x = []
            subplot_y = []
            plot_labels = []
            cur_axarr = axarr[i, j]
            # Process 30 day chunks and get representation for x-axis
            for t, v in zip(cur_interval_ts, cur_interval_val):
                dt_repr = _c_dt(t)
                hour_repr = dt_repr.hour + (dt_repr.minute / 60.0)
                x = hour_repr + (factor * self.ts_util.NUMBER_OF_HOURS_IN_DAY)
                processed_ts.append(x)
                _hr = dt_repr.timetuple().tm_hour
                _minute = dt_repr.timetuple().tm_min
                subplot_x.append(hour_repr)
                if x_lab.get(hour_repr, None) is None:
                    if len(str(_minute)) == 1:
                        _minute = '0' + str(_minute)
                    x_lab[hour_repr] = ':'.join([str(_hr), str(_minute)])
                if isinstance(v, MaskedConstant):
                    subplot_y.append(np.nan)
                else:
                    subplot_y.append(v)
            processed_val.extend(subplot_y)
            # Save for later use
            d30_x.append(subplot_x)
            d30_y.append(subplot_y)

            # Setup subplot and plot values
            start_date = self.ts_util.get_ISO_date_from_datetime(
                _c_dt(cur_interval_ts[0]), self.ts_util.DATE_ONLY_TS_FORMAT)
            end_date = self.ts_util.get_ISO_date_from_datetime(
                _c_dt(cur_interval_ts[-1]), self.ts_util.DATE_ONLY_TS_FORMAT)
            subplot_title = f'{start_date} - {end_date}'

            cur_axarr.set_title('')

            # Place the title inside the plot area
            cur_axarr.text(0.5, 0.95, subplot_title, ha='center', va='center',
                           fontsize=12, transform=cur_axarr.transAxes)
            # Set x-axis limit (24.9 is to prevent axis with overlapping text)
            cur_axarr.set_xlim(-1, 25)
            cur_axarr.plot(
                subplot_x, subplot_y, marker='.', color=self.gray_color, ls='')
            # this locator puts ticks at regular intervals
            loc = plticker.MultipleLocator(base=1.0)
            new_x_lab = []
            label_hr_gaps = 3
            # get_ticklabels returns a range of 0 - 28
            # (this is one more on each side than our xlim of [-1, 25]
            offset = -2  # Ticklabels start at -2, labels should start at 22:00
            xaxis = cur_axarr.xaxis
            xaxis.set_major_locator(loc)
            last_row_idx = subplot_rows - 1
            for idx, label in enumerate(xaxis.get_ticklabels()):
                if i < last_row_idx:
                    label.set_visible(False)
                    continue
                if idx % label_hr_gaps != abs(offset):
                    label.set_visible(False)
                if (idx + offset) < 0:
                    idx += self.ts_util.NUMBER_OF_HOURS_IN_DAY
                if (idx + offset) >= self.ts_util.NUMBER_OF_HOURS_IN_DAY:
                    idx -= self.ts_util.NUMBER_OF_HOURS_IN_DAY
                new_x_lab.append(x_lab.get(idx + offset))
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                cur_axarr.set_xticklabels(new_x_lab, rotation=90)
            for idx, tickline in enumerate(xaxis.get_ticklines()):
                if i < last_row_idx:
                    tickline.set_visible(False)
                    continue
                if idx % label_hr_gaps != 1:
                    tickline.set_visible(False)

            # Calculate median and plot
            x_vals, y_vals = self.calculate_median(subplot_x, subplot_y)
            derived_med.extend(y_vals)
            median_label = self._calculated_median
            cur_axarr.plot(
                x_vals, y_vals, marker=' ', linestyle='-', linewidth=2,
                color=self.color_palette[0], label=median_label)
            plot_labels.append(median_label)

            if j == 0:
                ticks_pos = 'left'
            else:
                ticks_pos = 'none'
            cur_axarr.get_yaxis().set_ticks_position(ticks_pos)
            fig.subplots_adjust(hspace=.1, wspace=0)

            # Move on to next index
            if j < subplot_cols - 1:
                j += 1
            else:
                j = 0
                i += 1
            factor += 1

        data_year = self.ts_util.cast_as_datetime(start_time).year
        fig_name = self.fig_name_fmt.format(
            s=self.site_id, p=self.process_id,
            t='diurnal_seasonal_pattern', x=var_name, yr=data_year)
        fig_loc = os.path.join(self.plot_dir, fig_name)

        # ------------------------------------------------------------------#
        # Plot and handle historical data
        # ------------------------------------------------------------------#
        # median in legend info
        hist_v = {}
        plot_col = 0
        plot_row = 0
        for h in self.hist_names:
            doy, hr, v, ver = self.load_historical_data(
                self.hist_dir_path, site_id, var_name, h)
            hist_v[h] = v
        hist_idx_ls = self.get_historic_data_idx(doy)
        for idx, d in zip(hist_idx_ls, doy):
            s = idx[0]
            e = idx[-1]
            for idx, h in enumerate(self.hist_names):
                if h == 'MEDIAN':
                    linewidth = 3
                elif h == 'LOWER1' or h == 'UPPER1':
                    linewidth = 2
                else:
                    linewidth = 1
                axarr[plot_row, plot_col].plot(
                    hr[s:e], hist_v.get(h)[s:e],
                    marker=' ', linestyle='-', linewidth=linewidth,
                    # color=self.color_palette[(idx % 3) + 1],
                    color=self.color_palette[1],
                    label=h)
            if plot_col < subplot_cols - 1:
                plot_col += 1
            else:
                plot_col = 0
                plot_row += 1
        handles, labels = self._gen_legend_handles(hist_ver=ver)
        leg = plt.figlegend(handles, labels, loc='upper left',
                            ncol=1, bbox_to_anchor=(0.03, 0.97),
                            title='Plot Symbols')
        leg.get_title().set_fontweight('bold')
        bbox = leg.get_window_extent()
        bbox = bbox.transformed(fig.dpi_scale_trans.inverted())

        # Calculate the width of the main legend
        legend_width = bbox.width

        # Analysis
        lo = hist_v.get(self.hist_names[0])
        lo2 = hist_v.get(self.hist_names[1])
        hist_med = hist_v.get(self.hist_names[2])
        hi = hist_v.get(self.hist_names[-2])
        hi2 = hist_v.get(self.hist_names[-1])

        for x, y, idx in zip(d30_x, d30_y, hist_idx_ls):
            cache = {}
            s, e = idx[0], idx[-1]
            for xi, yi in zip(x, y):
                if cache.get(xi, None):
                    cache.get(xi).append(yi)
                else:
                    cache[xi] = [yi]
            for idx in range(s, e):
                h = hr[idx]
                # Handle offsets based on resolution
                if self.resolution == 'HH':
                    h -= .25
                else:
                    h -= .5
                cache_res = cache.get(h)
                if not cache_res:
                    info_msg = ('Data for the full 30 day period is incomplete'
                                ' and cannot be compared with historical.')
                    yr_log.info(info_msg)
                    break
                for v in cache_res:
                    if math.isnan(v):
                        continue
                    n_pts += 1
                    if v > hi2[idx] or v < lo2[idx]:
                        n_outside_outer_band += 1
                    if v <= hi[idx] and v >= lo[idx]:
                        n_inside_inner_band += 1

        # create a dict to hold sub_statuses for each check
        sub_stats = {}

        # Find the lag and max_corr value
        lag = None
        is_valid, n_nan_hist_pts, n_nan_derived_pts = \
            self.stats_util.is_valid_input_for_corr_cond(hist_med, derived_med)
        if is_valid:
            lags, corr = self.stats_util.ccorr(
                hist_med, derived_med, is_HR=(self.resolution == 'HR'))
            lag = lags[0]
            max_corr = corr[0]
            for candidate_lag, c in zip(lags[1:], corr[1:]):
                if abs(float(c)) > abs(float(max_corr)):
                    lag = candidate_lag
                    max_corr = c

        # Perform cross corr check
        corr_log = Logger().getLogger(
            f'{self.qaqc_name}-{year}-{var_name}-ccorr_check')

        if lag is not None and full_year:
            # Log an error if max_corr is negative and
            # abs(max_corr) is significant
            if (abs(float(max_corr)) >= self.cross_cor_threshold and
                    float(max_corr) < 0):
                lag_txt = f'Max corr {max_corr} at lag {lag}'
                corr_log.error(lag_txt)
            # Log a message if no significant cross corr was found
            elif abs(float(max_corr)) < self.cross_cor_threshold:
                lag_txt = 'No significant cross correlation was found'
                corr_log.info(lag_txt)
            # Log a warning if a significant cross corr was found
            else:
                lag_txt = f'Max corr {max_corr} at lag {lag}'
                if (abs(float(max_corr)) >= self.cross_cor_threshold and
                        float(max_corr) > 0 and abs(lag) < 1):
                    corr_log.info(lag_txt)
                else:
                    corr_log.warning(lag_txt)

            time_lag = int(lag)
            corr = round(float(max_corr), 3)
        else:
            if not is_valid:
                lag_txt = 'Insufficient data for calculating cross correlation'
                corr_log.info(lag_txt)
                _log.info('Num of nans in derived var median '
                          f'{var_name}: {n_nan_hist_pts}')
                _log.info('Num of nans in derived var median '
                          f'{var_name}: {n_nan_derived_pts}')
            elif not full_year:
                lag_txt = ('Cannot calculate cross correlation from partial '
                           'year data')
                corr_log.info(lag_txt)
            else:
                lag_txt = 'Cross correlation could not be calculated'
                corr_log.info(lag_txt)

            time_lag = 'not_calculated'
            corr = 'not_calculated'

        handles_stats = [mlines.Line2D(
            [], [], color='None', marker='None', label=lag_txt)]
        labels_stats = [lag_txt]
        new_legend_x = 0.03 + legend_width / fig.get_size_inches()[0]
        leg_stats = plt.figlegend(handles_stats, labels_stats,
                                  loc='upper left', ncol=1,
                                  bbox_to_anchor=(new_legend_x, 0.97),
                                  title='Summary Statistics')
        leg_stats.get_title().set_fontweight('bold')

        plt.savefig(fig_loc, dpi=self.plot_config.plot_default_dpi)
        plt.close()
        fig_url = fig_loc.replace(self.base_plot_dir, self.url_path)

        # Create a Status object for the corr check
        stat = self.stat_gen.status_generator(
                logger=corr_log,
                qaqc_check=corr_log.getName(),
                status_msg=lag_txt,
                plots=[fig_url])
        stat.add_summary_stats({
            'time_lag': time_lag,
            'corr': corr
        })
        sub_stats[corr_log.getName()] = stat

        # stat_msgs = [lag_txt]
        # msg_combiner = '<br>'  # this might need to be a comma

        outer_band_log = Logger().getLogger(
            f'{self.qaqc_name}-{year}-{var_name}-outer_band_check')

        inner_band_log = Logger().getLogger(
            f'{self.qaqc_name}-{year}-{var_name}-inner_band_check')

        # Create an info message that the year had 0 valid points for
        # inner/outer band checks
        if n_pts == 0:
            stat_msg = f'Variable {var_name} has no historical data'
            outer_band_log.info(stat_msg)
            inner_band_log.info(stat_msg)

            # Create an outer_band sub_status
            stat = \
                self.stat_gen.status_generator(
                    logger=outer_band_log,
                    qaqc_check=outer_band_log.getName(),
                    status_msg=stat_msg,
                    plots=[fig_url])
            sub_stats[outer_band_log.getName()] = stat

            # Create an inner band sub_status
            stat = \
                self.stat_gen.status_generator(
                    logger=inner_band_log,
                    qaqc_check=inner_band_log.getName(),
                    status_msg=stat_msg,
                    plots=[fig_url])
            sub_stats[inner_band_log.getName()] = stat

            # Create a composite yearly Status
            stat = self.stat_gen.composite_status_generator(
                logger=yr_log,
                qaqc_check=yr_log.getName(),
                statuses=sub_stats,
                plot_paths=[fig_url])

            return fig_url, year, stat

        # Perform the outer_band check
        percent_outside_outer_band = n_outside_outer_band / n_pts
        percent_inside_inner_band = n_inside_inner_band / n_pts

        percent_str = round(percent_outside_outer_band * 100, 2)
        outer_band_msg = (f'{percent_str}% timestamps outside of 2.5-97.5% '
                          f'percentiles of historical data ({ver})')
        outer_band = round(percent_outside_outer_band * 100, 2)

        if percent_outside_outer_band > self.outer_band_error_threshold:
            outer_band_log.error(outer_band_msg)
        elif percent_outside_outer_band > self.outer_band_warning_threshold:
            outer_band_log.warning(outer_band_msg)
        else:
            outer_band_log.info(outer_band_msg)

        # Create an outer_band Status object
        stat = self.stat_gen.status_generator(
            logger=outer_band_log,
            qaqc_check=outer_band_log.getName(),
            status_msg=outer_band_msg,
            plots=[fig_url])
        stat.add_summary_stat('outer_band', outer_band)
        sub_stats[outer_band_log.getName()] = stat

        # Perform the inner_band check
        percent_str = round(percent_inside_inner_band * 100, 2)
        inner_band_msg = (f'{percent_str}% timestamps inside of 25-75% '
                          f'percentiles of historical data ({ver})')
        inner_band = round(percent_inside_inner_band * 100, 2)

        if percent_inside_inner_band < self.inner_band_error_threshold:
            inner_band_log.error(inner_band_msg)
        elif percent_inside_inner_band < self.inner_band_warning_threshold:
            inner_band_log.warning(inner_band_msg)
        else:
            inner_band_log.info(inner_band_msg)

        # Create an inner_band Status object
        stat = self.stat_gen.status_generator(
            logger=inner_band_log,
            qaqc_check=inner_band_log.getName(),
            status_msg=inner_band_msg,
            plots=[fig_url])
        stat.add_summary_stat('inner_band', inner_band)
        sub_stats[inner_band_log.getName()] = stat

        # Create a yearly status object to bundle the other three statuses
        stat = self.stat_gen.composite_status_generator(
            logger=yr_log,
            qaqc_check=yr_log.getName(),
            plot_paths=[fig_url],
            statuses=sub_stats)

        return fig_url, year, stat

    def format_status_msgs(self, stat_msgs, year, msg_combiner):
        if len(stat_msgs) > 1:
            stat_msgs[0] = f'{year}: {stat_msgs[0]}'
            stats_msgs = msg_combiner.join(stat_msgs)
        elif len(stat_msgs) > 0:
            stats_msgs = f'{year}: {stat_msgs[0]}'
        else:
            stats_msgs = None

        return stats_msgs

    def calculate_median(self, x_vals, y_vals):
        """Given x and y values, we expect multiple values
        with the same x values and different y values. Perform
        the median calculations on the y values and return
        the median as the new y values"""
        store = {}

        for x, y in zip(x_vals, y_vals):
            if store.get(x, False) and not math.isnan(y):
                store.get(x).append(y)
            elif not math.isnan(y):
                store[x] = [y]
        x_vals = []
        y_vals = []

        float_keys = list(map(float, store.keys()))
        float_keys.sort()
        for k in float_keys:
            x_vals.append(k)
            y_vals.append(np.median(store.get(k)))
        return x_vals, y_vals

    def get_params_from_config(self):
        param_vars = [
            'historical_ranges', 'outer_band_warning_threshold',
            'outer_band_error_threshold', 'inner_band_warning_threshold',
            'inner_band_error_threshold', 'cross_cor_threshold']
        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'DIURNAL_SEASONAL_PATTERN'
            hist_dir_path = None
            outer_band_error_threshold = None
            outer_band_warning_threshold = None
            inner_band_error_threshold = None
            inner_band_warning_threshold = None

            if config.has_section(cfg_section):
                historical_ranges = config.get(
                    cfg_section, 'historical_ranges')
                self.hist_dir = historical_ranges
                hist_dir_path = os.path.join(cwd, self.hist_dir)
                outer_band_warning_threshold = config.getfloat(
                    cfg_section, 'outer_band_warning_threshold')
                outer_band_error_threshold = config.getfloat(
                    cfg_section, 'outer_band_error_threshold')
                inner_band_warning_threshold = config.getfloat(
                    cfg_section, 'inner_band_warning_threshold')
                inner_band_error_threshold = config.getfloat(
                    cfg_section, 'inner_band_error_threshold')
                cross_cor_threshold = config.getfloat(
                    cfg_section, 'cross_cor_threshold')
            else:
                warning_msg = 'Cannot find historical ranges dir from config.'
                _log.warning(warning_msg)
                warning_msg = 'Cannot find thresholds from config.'
                _log.warning(warning_msg)

        _log.info('Params from config file')
        for k, v in locals().items():
            try:
                if k in param_vars:
                    _log.info(' '.join([str(k), str(v)]))
            except Exception:
                continue
        return hist_dir_path, outer_band_error_threshold, \
            outer_band_warning_threshold, inner_band_error_threshold, \
            inner_band_warning_threshold, cross_cor_threshold

    def get_available_hist_vars(self):
        var_ls = []
        fname = next((os.path.join(self.hist_dir_path, f)
                      for f in os.listdir(self.hist_dir_path)
                      if self.site_id in f and self.hist_names[0] in f), None)
        if fname is None:
            return var_ls

        with open(fname) as csv_file:
            reader = csv.reader(csv_file)
            for r in reader:
                var_ls.extend(
                    (v for v in r if (v not in (self.doy_var, self.hr_var))))
                break
        return var_ls

    def load_historical_data(self, path, site_id, var_name, data_type):
        """Load data from Housen's diurnal seasonal range that are
        currently CSVs"""
        fnames = os.listdir(path)
        fnames = [os.path.join(path, f) for f in fnames if site_id in f]
        # fnames have format: <path>/<site_id>_<res>_<ver>_<quartile>.csv
        vers = [f.split('_')[-2] for f in fnames]
        latest_ver = self._get_latest_ver(vers)
        for fname in fnames:
            if data_type in fname:
                ver_str = self._format_ver_str(latest_ver)
                data = np.genfromtxt(
                    fname=fname, names=True,
                    delimiter=',', missing_values='-9999',
                    filling_values=np.nan, usemask=True)
        return (data[self.doy_var] - 1, data[self.hr_var],
                data[var_name], ver_str)

    def _get_latest_ver(self, ver_ls):
        data_ver, proc_ver = None, None
        latest_data_ver = None
        latest_proc_ver = None
        latest_ver = None

        for v in ver_ls:
            try:
                vers = v.split('-')
                if len(vers) == 2:
                    data_ver = int(vers[0])
                    proc_ver = int(vers[1])
                elif len(vers) == 1:
                    data_ver = int(vers[0])
                else:
                    _log.fatal(f'Unrecognized version: {v}')
                if latest_data_ver is None or latest_data_ver < data_ver:
                    latest_data_ver = data_ver
                    latest_proc_ver = proc_ver
            except Exception:
                continue
        if latest_proc_ver is not None:
            latest_ver = f'{latest_data_ver}-{latest_proc_ver}'
        return latest_ver

    def _format_ver_str(self, ver):
        if ver is None:
            return 'previous data'
        else:
            return f'BASE {ver}'

    def check_historical_data_avail(self):
        fnames = os.listdir(self.hist_dir_path)
        count = 0
        for fname in fnames:
            if all((attr in fname
                    for attr in (self.site_id, self.resolution))):
                count += 1
        # Must be multiple of hist_names, otherwise we
        # have insufficient historical data files available too
        return (count % len(self.hist_names) == 0) and count > 0

    def get_historic_data_idx(self, doy_list):
        prev_doy = None
        idx_ls = []
        for idx, doy in enumerate(doy_list):
            if prev_doy is None:
                prev_doy = doy
                idx_rng = [idx]
            elif prev_doy != doy:
                prev_doy = doy
                idx_rng.append(idx)
                idx_ls.append(idx_rng)
                idx_rng = [idx]
            else:
                continue
        idx_rng.append(idx)
        idx_ls.append(idx_rng)
        return idx_ls

    def add_result_summary_stat(self, statuses):
        for var_status in statuses:
            if var_status.get_sub_status() is None:
                continue

            for year_status in var_status.get_sub_status().values():
                result = StatusCode.OK
                for check_status in year_status.get_sub_status().values():

                    # Get the worst (minimum) status_code of any check_status
                    status_code = check_status.get_status_code()
                    result = min(result, status_code)

                year_status.add_summary_stat('result', result)

    def _write_summary(self, statuses):
        summary_dir = os.path.join(self.base_plot_dir, 'summary')
        if not os.path.exists(summary_dir):
            os.makedirs(summary_dir)

        csv_headers = {
            'year': 'Period',
            'var': 'Variable',
            'result': 'Result',
            'time_lag': 'Time lag',
            'corr': 'Cross-correlation',
            'inner_band':
                'Timestamps within historical interquartile range (%)',
            'outer_band': 'Timestamps outside historical 95% range (%)',
            'figure': 'Figure link'
        }
        filename = os.path.join(summary_dir, f'{__name__}_summary.csv')
        output_stats = OutputStats(statuses, sort_by_header='var')
        output_stats.write_to_csv(filename, csv_headers)

    def driver(self, data_reader):
        _log.info('Starting diurnal seasonal pattern checks')
        self.d = data_reader
        self.input_data = self.d.get_data()  # Get data object
        self.stat = []

        # Update base vars (Need to get from historical)
        self.has_historical = self.check_historical_data_avail()
        warning_msg_prefix = None
        warning_msg_postfix = ('Skipping diurnal '
                               'seasonal pattern checks.')
        if not self.has_historical:
            warning_msg_prefix = 'No historical data found. '

        if warning_msg_prefix is None:
            hist_var_ls = self.get_available_hist_vars()
            var_ls = self.find_vars(hist_var_ls)

            if not var_ls:
                warning_msg_prefix = (
                    'BASE candidate has no variables that match '
                    'historical data. ')

        if warning_msg_prefix:
            qaqc_check = f'{self.qaqc_name}-historical_data_check'
            log_obj = Logger().getLogger(qaqc_check)

            warning_msg = warning_msg_prefix + warning_msg_postfix
            log_obj.info(warning_msg)

            self.stat.append(
                self.stat_gen.status_generator(logger=log_obj,
                                               qaqc_check=qaqc_check,
                                               status_msg=warning_msg,
                                               report_section='high_level'))
            self._write_summary(self.stat)
            return self.stat, self.plot_dir
        else:
            _log.info('Historical variables available: '
                      f'{", ".join(hist_var_ls)}')
            _log.info('These variables have matching historical variables '
                      'and will be assessed: '
                      f'{", ".join(var_ls)}')
            for var in var_ls:
                self.stat.append(self.processor(self.site_id, var))

        self.add_result_summary_stat(self.stat)
        self._write_summary(self.stat)
        return self.stat, self.plot_dir


if __name__ == '__main__':
    # No use
    sys.exit('ERROR: Do not use this module on its own')
