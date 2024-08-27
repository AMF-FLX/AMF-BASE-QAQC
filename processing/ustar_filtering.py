import copy
import itertools
import matplotlib.dates as dates
import matplotlib.pyplot as plt
import numpy.ma as ma
import os
import matplotlib.lines as mlines

from configparser import ConfigParser
from data_reader import DataReader
from logger import Logger
from output_stats import OutputStats
from plot_config import PlotConfig
from status import Status, StatusCode, StatusGenerator
from typing import List, Tuple
from utils import TimestampUtil, VarUtil

__author__ = 'Norm Beekwilder, Josh Geden'
__email__ = 'norm.beekwilder@gmail.com, joshgeden10@gmail.com'

_log = Logger().getLogger(__name__)


class USTARFiltering:
    def __init__(
          self,
          site_id: str,
          process_id: str,
          plot_dir: str = None,
          ftp_plot_dir: str = None) -> None:
        """ Setup configuration variables for module """

        self.data = None
        self.url_path = ftp_plot_dir
        self.site_id = site_id
        self.process_id = process_id
        self.rad_in_var = None
        self.qaqc_name = 'ustar_filtering'

        # Helper objects
        self.plot_config = PlotConfig()
        self.ts_util = TimestampUtil()
        self.var_util = VarUtil()

        # Determine if plots should be generated and where to save them
        if plot_dir is not None:
            self.can_plot = True
            self.plot_dir = self.plot_config.get_plot_dir_for_check(
                plot_dir, self.qaqc_name,
            )
            self.base_plot_dir = plot_dir
        else:
            self.can_plot = False
            self.plot_dir = None
            self.base_plot_dir = None

        # Populate variables from config file
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            section = 'USTAR_FILTERING'
            if config.has_section(section):
                self.lower_bound_warn = config.getfloat(
                    section, 'lower_bound_warn')
                _log.info(f'lower_bound_warn: {self.lower_bound_warn}')
                self.lower_bound_error = config.getfloat(
                    section, 'lower_bound_error')
                self.difference_warn = config.getfloat(
                    section, 'difference_warn')
                self.difference_error = config.getfloat(
                    section, 'difference_error')
                self.sw_day_night_cutoff = config.getfloat(
                    section, 'sw_day_night_cutoff')
                self.ppfd_day_night_cutoff = config.getfloat(
                    section, 'ppfd_day_night_cutoff')
            else:
                self.lower_bound_warn = .01
                self.lower_bound_error = .05
                self.difference_warn = .01
                self.difference_error = .05
                self.sw_day_night_cutoff = 5
                self.ppfd_day_night_cutoff = 10

                _log.warning('Cannot find USTAR Filtering values; '
                             'using defaults.')

    def _select_rad_var(self, header_map, rad_vars):
        """ Selects which radiation variable to use from rad_vars """

        # Select SW_IN_POT if available
        if 'SW_IN_POT' in rad_vars:
            return 'SW_IN_POT'

        rad_in = None
        for rad_var in rad_vars:
            # Don't select this rad_var if it isn't in the data
            if rad_var not in header_map.keys():
                continue

            lowest_horiz_rad_vars = self.var_util.get_lowest_horiz_variables(
                var_ls=header_map[rad_var], include_filled_vars=True,
                keep_horiz_layer_var_if_h=True, log=_log)
            top_rad_var_ls = self.var_util.get_top_level_variables(
                var_ls=lowest_horiz_rad_vars, include_filled_vars=True,
                log=_log)
            if len(top_rad_var_ls) == 0:
                continue
            elif len(top_rad_var_ls) == 1:
                return top_rad_var_ls[0]
            else:
                rad_in_candidates = \
                    self.var_util.remove_dup_filled_nonfilled_var(
                        top_rad_var_ls, rm_which='gap-filled')
                if len(rad_in_candidates) > 1:
                    horiz_layer_vars = \
                        self.var_util.keep_horiz_layer_vars(
                            var_ls=rad_in_candidates)
                    if horiz_layer_vars:
                        rad_in_candidates = horiz_layer_vars
                    elif any([v.endswith('_A') for v in rad_in_candidates]):
                        replicate_agg_vars = \
                            self.var_util.keep_replicate_agg_vars(
                                var_ls=rad_in_candidates)
                        if replicate_agg_vars:
                            rad_in_candidates = replicate_agg_vars
                    if len(rad_in_candidates) > 1:
                        rad_in = self.var_util.get_lowest_r_variable(
                            top_level_var_ls=rad_in_candidates,
                            include_filled_vars=True)
                if len(rad_in_candidates) == 1:
                    rad_in = rad_in_candidates[0]
            if rad_in:
                break

        return rad_in

    def _check_ustar_vars_present(self, data_reader,
                                  required_vars: List[str]) -> Status:
        """ Checks that the required vars are present for USTAR analysis """

        headers = data_reader.get_base_headers()

        for var in required_vars:
            if var not in headers:
                log_obj = Logger().getLogger(
                    f'{self.qaqc_name}-all_data-{var}-var_present')
                stat_msg = ('Data file does not contain required '
                            f'variable: {var}')
                log_obj.fatal(stat_msg)

                # Return a Status object with fatal status_code
                return StatusGenerator().status_generator(
                    logger=log_obj,
                    qaqc_check=self.qaqc_name,
                    status_msg=stat_msg)

        return None

    def _get_start_end_idx(self, data):
        """Returns start and end indices for years in data"""
        """Start by finding start indices"""
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

    def _calculate_ustar_metrics(self, base_headers) -> Tuple[dict, dict]:
        """ Returns a tuple containing 2 dicts. The first dict stores the
            minimum USTAR value for every USTAR/FC var combination. For the
            absolute USTAR minimum (without respect to an FC var), use
            'base' as the fc_var key.

            It has the form:
            yearly_metrics = {
                year: {
                    ustar_var: {
                        fc_var: {
                            day: min_ustar_val
                            night: min_ustar_val
                        }
                    }
                }
            }

            The second dict contains formatted plotting data. It has the form:
            plot_data = {
                year: {
                    ustar_var: {
                        fc_var: {
                            'day': {
                                timestamp: val,
                                timestamp: val,
                                ...
                            },
                            'night': {
                                timestamp: val,
                                timestamp: val,
                                ...
                            }
                        }
                    }
                }
            }
        """

        annual_idx = self._get_start_end_idx(self.data)
        annual_idx.pop('all_data', None)

        # Keep track of min value for USTAR and each combo of USTAR wrt FC
        ustar_mins = {}
        plot_data = {}

        # Get the day/night cutoff value and radiation data based on selected
        # rad var
        if self.rad_in_var == 'SW_IN' or self.rad_in_var == 'SW_IN_POT':
            day_night_cutoff = self.sw_day_night_cutoff
        elif self.rad_in_var == 'PPFD_IN':
            day_night_cutoff = self.ppfd_day_night_cutoff
        else:
            raise Exception('Unsupported radiation variable '
                            f'{self.rad_in_var} used; valid radiation '
                            'variables are SW_IN_POT, SW_IN, and PPFD_IN.')
        rad_data = self.data[self.rad_in_var]

        # Loop through every year of data
        # for fc_var in ['USTAR'].extend(data_reader.get_base_headers()['FC']):
        ustar_vars = base_headers['USTAR']
        for ustar_var, year in itertools.product(ustar_vars, annual_idx):
            ustar_mins.setdefault(year, {})
            plot_data.setdefault(year, {})

            ustar_mins[year][ustar_var] = {}
            ustar_mins[year][ustar_var]['base'] = {
                'day': float('inf'),
                'night': float('inf'),
            }

            plot_data[year][ustar_var] = {}
            plot_data[year][ustar_var]['base'] = {
                'day': {},
                'night': {},
            }

            start, end = annual_idx[year]['start'], annual_idx[year]['end']

            # Keep track of all FC variable headers
            fc_data = {}
            for fc_var in base_headers['FC']:
                # Ignore gap filled headers
                if 'F' not in fc_var.split('_'):
                    fc_data[fc_var] = self.data[fc_var][start:end + 1]

                    # Keep track of the USTAR min wrt the current FC var
                    ustar_mins[year][ustar_var][fc_var] = {
                        'day': float('inf'),
                        'night': float('inf'),
                    }

                    # Keep track of data to plot
                    plot_data[year][ustar_var][fc_var] = {
                        'day': {},
                        'night': {},
                    }

            timestamps = self.data['TIMESTAMP_START'][start:end + 1]
            ustar_data = self.data[ustar_var][start:end + 1]

            all_fc_data = []
            for fc_header, fc_vals in fc_data.items():
                all_fc_data.append([(fc_header, val) for val in fc_vals])

            # Create a list of tuples with an entry for each timestamp.
            # Each tuple contains more tuples of the form:
            # ('FC_var', FC_val)
            #
            # E.g.: [
            #           (('FC_1_1_1', 1), ('FC_2_1_1', 2)),
            #           (('FC_1_1_1', 3), ('FC_2_1_1', 4)),
            #       ]
            all_fc_data = [e for e in zip(*all_fc_data)]

            # Loop through per timestamp entry
            for timestamp, ustar_val, fc_vals, rad_val in zip(
                    timestamps, ustar_data, all_fc_data, rad_data):

                if not ma.is_masked(ustar_val):
                    # If the amount of radiation is lower than the cutoff
                    # it is nighttime
                    if rad_val < day_night_cutoff:
                        period = 'night'
                    else:
                        period = 'day'

                    # Check if ustar_val is lower than current USTAR min
                    # for day/night
                    ustar_min = ustar_mins[year][ustar_var]['base'][period]
                    if ustar_val < ustar_min:
                        ustar_mins[year][ustar_var]['base'][period] = \
                            ustar_val

                    # Store plot data as {int(timestamp): val}
                    t = dates.date2num(
                        self.ts_util.cast_as_datetime(timestamp))
                    plot_data[year][ustar_var]['base'][period][t] = ustar_val

                    # Loop through all FC vars and check if not masked
                    for fc_var, fc_val in fc_vals:
                        fc_min = ustar_mins[year][ustar_var][fc_var][period]
                        if not ma.is_masked(fc_val):
                            # Check for a new min for the current FC var
                            if (ustar_val < fc_min):
                                ustar_mins[year][ustar_var][fc_var][period] = \
                                    ustar_val

                            plot_data[year][ustar_var][fc_var][period][t] = \
                                ustar_val

        return ustar_mins, plot_data

    def _check_masked_data(self, base_headers) -> Status:
        for base_var in ['USTAR', 'FC']:
            for var in base_headers[base_var]:
                var_data = self.data[var]

                # If all values are masked return a fatal Status
                if all(ma.getmask(var_data)):
                    qaqc_check = \
                        f'{self.qaqc_name}-all_data-{base_var}-masked_check'
                    status_msg = ('Data is masked for the entire record for '
                                  f'required variable {base_var}')

                    log_obj = Logger().getLogger(qaqc_check)
                    log_obj.fatal(status_msg)

                    return StatusGenerator().status_generator(
                        logger=log_obj,
                        qaqc_check=qaqc_check,
                        status_msg=status_msg)

        return None

    def _create_missing_year_status(self, year, ustar_var, fc_var,
                                    period) -> Status:

        qaqc_check = (f'{self.qaqc_name}-{year}-{ustar_var}:{fc_var}-'
                      f'valid_min_{period}')
        log_obj = Logger().getLogger(qaqc_check)
        if fc_var == 'base':
            status_msg = (f'Could not find a valid {period} minimum for '
                          f'{ustar_var} for {year}')
        else:
            status_msg = (f'Could not find a valid {period} time minimum '
                          f'for {ustar_var} with respect to {fc_var} for '
                          f'{year}')

        log_obj.error(status_msg)
        summary_stats = {
            f'min_USTAR_{period}': 'NA',
            f'min_USTAR:{fc_var}_{period}': 'NA',
        }

        stat = StatusGenerator().status_generator(
            logger=log_obj,
            qaqc_check=qaqc_check,
            status_msg=status_msg)
        stat.add_summary_stats(summary_stats)
        return stat

    def _get_ustar_filtering_status(self, year, ustar_var, fc_var,
                                    period, fc_var_min) -> Status:
        """ Returns a Status object indicating if USTAR filtering was
            detected. """

        status_msg = None
        qaqc_check = \
            (f'{self.qaqc_name}-{year}-{ustar_var}:{fc_var}-'
             f'filter_check_{period}')
        log_obj = Logger().getLogger(qaqc_check)

        if fc_var_min != float('inf'):
            if fc_var_min > self.lower_bound_error:
                status_msg = self._get_filtering_msg(
                    fc_var, fc_var_min)
                log_obj.error(status_msg)
            elif fc_var_min > self.lower_bound_warn:
                status_msg = self._get_filtering_msg(
                    fc_var, fc_var_min)
                log_obj.warning(status_msg)
            else:
                status_msg = self._get_filtering_msg(
                    fc_var, fc_var_min, filtering=False)
                log_obj.info(status_msg)

        stat = StatusGenerator().status_generator(
            logger=log_obj,
            qaqc_check=qaqc_check,
            status_msg=status_msg,)
        if fc_var == 'base':
            stat.add_summary_stat(f'min_USTAR_{period}', fc_var_min)
        else:
            stat.add_summary_stat(f'min_USTAR:FC_{period}', fc_var_min)
        return stat

    def _get_filtering_msg(self, fc_var, fc_var_min,
                           filtering=True):
        """ Returns a formatted message to be logged and used in the Status
            object generated in _get_ustar_filtering_status() """

        if filtering:
            if fc_var != 'base':
                msg = (f'Possible USTAR filtering detected '
                       f'with respect to {fc_var}; minimum USTAR '
                       f'value is {fc_var_min}')
            else:
                msg = ('Possible USTAR filtering detected; '
                       'minimum USTAR value is '
                       f'{fc_var_min}')
        else:
            if fc_var != 'base':
                msg = (f'Minimum USTAR value with respect to {fc_var} '
                       f'is {fc_var_min}')
            else:
                msg = f'Minimum USTAR value is {fc_var_min}'
        return msg

    def _get_ustar_diff_status(self, year, ustar_var, fc_var, period,
                               ustar_min, fc_ustar_min, figure_link) -> Status:
        """ Returns a Status object representing the difference between
            the base USTAR min and the USTAR min wrt an FC var """

        qaqc_check = \
            f'{self.qaqc_name}-{year}-{ustar_var}:{fc_var}-diff_check_{period}'

        log_obj = Logger().getLogger(qaqc_check)

        diff = abs(fc_ustar_min - ustar_min)
        status_msg = self._get_ustar_diff_msg(fc_var, period, diff)
        if diff > self.difference_error:
            log_obj.error(status_msg)
        elif diff > self.difference_warn:
            log_obj.warning(status_msg)
        else:
            log_obj.info(status_msg)

        plots = [figure_link] if figure_link is not None else None

        # Create status object to return
        status = StatusGenerator().status_generator(
            logger=log_obj,
            qaqc_check=qaqc_check,
            status_msg=status_msg,
            plots=plots)

        # Add summary stats for output_stats
        status.add_summary_stat('diff', round(diff, 3))

        return status

    def _get_ustar_diff_msg(self, fc_var, period, diff):
        """ Returns a message to be logged and included in the Status object
            generated in _get_ustar_diff_status() """

        diff = round(diff, 4)

        if diff > self.difference_error or diff > self.difference_warn:
            status_msg = (f'{period} time difference in lower bounds exceeds '
                          f'limits with respect to {fc_var}; '
                          f'difference is {diff}')
        else:
            status_msg = (f'{period} time difference in lower bounds with '
                          f'respect to {fc_var} is {diff}')
        return status_msg

    def _make_plot(self, year, ustar_var, fc_var, plot_data) -> str:
        """ Generates a plot illustrating the difference between the
            base USTAR min values and the USTAR mins wrt an FC var. Returns
            the path of the generated plot. """

        if fc_var == 'base':
            return None

        # Used to plot overall USTAR data on top of USTAR wrt FC var data
        ustar_data = plot_data[year][ustar_var]['base']

        day_data = plot_data[year][ustar_var][fc_var]['day']
        night_data = plot_data[year][ustar_var][fc_var]['night']

        # Skip if there's no data to plot
        if len(day_data) == len(night_data) == 0:
            return None

        fig = plt.figure(f'{year}-{ustar_var}:{fc_var}',
                         figsize=(12.8, 12.8),
                         dpi=self.plot_config.plot_default_dpi)

        text1 = 'USTAR Filtering | '
        text2 = f'{ustar_var} - {fc_var}'
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
        x_start = 0.03

        # Set the text positions dynamically based on the calculated widths
        fig.text(x_start, 0.99, text1, ha='left', va='top', fontsize=16)
        fig.text(x_start + bbox1.width / fig.bbox.width, 0.99,
                 text2, ha='left', va='top', fontsize=16, fontweight='bold')
        fig.text(x_start + (bbox1.width + bbox2.width) / fig.bbox.width,
                 0.99, text3, ha='left', va='top', fontsize=16)

        handles = [mlines.Line2D([], [], color='None', marker='o',
                                 markerfacecolor='#9B51E0',
                                 markeredgecolor='#9B51E0',
                                 label=f'{ustar_var} (all)'),
                   mlines.Line2D([], [], color='None', marker='o',
                                 markerfacecolor='0.75',
                                 markeredgecolor='0.75',
                                 label=f'{ustar_var} when '
                                 f'{fc_var} is not missing')]
        labels = [f'{ustar_var} (all)', f'{ustar_var}'
                  f' when {fc_var} is not missing']
        plots = []
        axhlines = []

        # Plot the overall USTAR var day data
        day_handles = []
        day_labels = []
        if len(day_data) > 0:
            p2 = self.plot_config.plot(
                x_vals=list(ustar_data['day'].keys()),
                y_vals=list(ustar_data['day'].values()),
                color='#9B51E0', marker='o', marker_size=2,
                subplot_pos=(2, 1, 1),
                x_label='Time', y_label=ustar_var,
                title=f'{year} daytime',
                # label=f'{ustar_var} (all)',
                reset_all_subplots=True
            )
            plots.extend(p2)
            ustar_min = min(ustar_data['day'].values())
            rounded_ustar_min = round(ustar_min, 2)
            l1 = plt.axhline(
                ustar_min, color='#9B51E0', label=f'{rounded_ustar_min:.2f}'
            )
            axhlines.append(l1)
            day_handles.append(mlines.Line2D([], [], color='#9B51E0',
                                             linestyle='-',
                                             label=f'{rounded_ustar_min:.2f} \
                                               (day)'))
            day_labels.append(f'{rounded_ustar_min:.2f} (day)')
        # Plot USTAR day data wrt the FC var
        if len(day_data) > 0:
            p1 = self.plot_config.plot(
                x_vals=list(day_data.keys()),
                y_vals=list(day_data.values()),
                color='0.75', marker='o', marker_size=1,
                subplot_pos=(2, 1, 1),
                reset_all_subplots=False
            )
            plots.extend(p1)
            ustar_min = min(day_data.values())
            rounded_ustar_min = round(ustar_min, 2)
            l2 = plt.axhline(
                ustar_min, color='0.75', label=f'{rounded_ustar_min:.2f}',
                linestyle='--'
            )
            axhlines.append(l2)
            day_handles.append(mlines.Line2D([], [], color='0.75',
                                             linestyle='--',
                                             label=f'{rounded_ustar_min:.2f} \
                                                (day)'))
            day_labels.append(f'{rounded_ustar_min:.2f} (day)')
        # If axis lines were generated, add them to the figure
        if len(axhlines) < 0:
            day_handles = []
            day_labels = []

        plots = []
        axhlines = []
        night_handles = []
        night_labels = []
        # Plot the overall USTAR var night data
        if len(ustar_data['night']) > 0:
            self.plot_config.plot(
                x_vals=list(ustar_data['night'].keys()),
                y_vals=list(ustar_data['night'].values()),
                color='#9B51E0', marker='o', marker_size=2,
                subplot_pos=(2, 1, 2), x_label='Time',
                y_label=ustar_var, title=f'{year} nighttime',
                reset_all_subplots=False
            )
            ustar_min = min(ustar_data['night'].values())
            rounded_ustar_min = round(ustar_min, 2)
            l1 = plt.axhline(
                ustar_min, color='#9B51E0', label=f'{rounded_ustar_min:.2f}'
            )
            axhlines.append(l1)
            night_handles.append(mlines.Line2D([], [], color='#9B51E0',
                                               linestyle='-',
                                               label=f'{rounded_ustar_min:.2f}\
                                                (night)'))
            night_labels.append(f'{rounded_ustar_min:.2f} (night)')

        # Plot USTAR night data wrt the FC var
        if len(night_data) > 0:
            self.plot_config.plot(
                x_vals=list(night_data.keys()),
                y_vals=list(night_data.values()),
                color='0.75', marker='o', marker_size=1,
                subplot_pos=(2, 1, 2),
                reset_all_subplots=False
            )
            ustar_min = min(night_data.values())
            rounded_ustar_min = round(ustar_min, 2)
            l2 = plt.axhline(
                ustar_min, color='0.75', label=f'{rounded_ustar_min:.2f}',
                linestyle='--'
            )
            axhlines.append(l2)
            night_handles.append(mlines.Line2D([], [], color='0.75',
                                               linestyle='--',
                                               label=f'{rounded_ustar_min:.2f}\
                                                (night)'))
            night_labels.append(f'{rounded_ustar_min:.2f} (night)')

        # If axis lines were generated, add them to the figure
        if len(axhlines) > 0:
            final_handles = handles + day_handles + night_handles
            final_labels = labels + day_labels + night_labels
            leg = fig.legend(final_handles, final_labels,
                             loc='upper left', ncol=1,
                             bbox_to_anchor=(0.01, 0.97),
                             title='Plot Symbols',
                             handletextpad=0.5,
                             columnspacing=1.5,
                             fontsize='small',
                             frameon=True,
                             title_fontproperties={'weight':'bold'})
            leg._legend_box.align = 'left'

        # Adjust the margins and padding
        plt.tight_layout()
        plt.subplots_adjust(top=0.85)

        # Determine where to save the figure
        fig_loc = os.path.join(
            self.plot_dir, f'{self.site_id}-{self.process_id}-'
            f'{self.qaqc_name}-{ustar_var}-{fc_var}-{year}.png'
        )

        # Save the figure to a file
        plt.savefig(fig_loc, dpi=self.plot_config.plot_default_dpi)
        plt.close()

        return fig_loc.replace(self.base_plot_dir, self.url_path)

    def _organize_status_objects(self, yearly_statuses) -> List[Status]:
        """ Returns a list of Status objects that are organized by
            year -> USTAR_VAR:FC_VAR -> individual check.
        """
        statuses = []

        # Collect the base USTAR check statuses
        base_ustar_sub_statuses = {}
        for year in yearly_statuses:
            for var in yearly_statuses[year]:
                ustar_var, fc_var = var.split(':')

                if fc_var == 'base':
                    if year not in base_ustar_sub_statuses:
                        base_ustar_sub_statuses[year] = {}

                    base_ustar_sub_statuses[year][ustar_var] = \
                        yearly_statuses[year][var]

        for year in yearly_statuses:
            var_sub_statuses = {}
            var_plots = []

            for var in yearly_statuses[year]:
                ustar_var, fc_var = var.split(':')
                if fc_var == 'base':
                    continue

                check_sub_statuses = {}
                check_plots = []
                for check in yearly_statuses[year][var]:
                    check_stat = yearly_statuses[year][var][check]
                    key = check_stat.get_qaqc_check()
                    check_sub_statuses[key] = check_stat

                    plot = check_stat.get_plot_paths()
                    if plot:
                        check_plots.extend(plot)

                check_plots = list(set(check_plots))
                if check_plots == []:
                    check_plots = None

                # Include the base USTAR as a sub_status of the USTAR:FC combo
                base_sub_statuses = base_ustar_sub_statuses[year][ustar_var]
                for status in base_sub_statuses.values():
                    copy_status = copy.deepcopy(status)

                    # Rename the copied status' qaqc check
                    check_name = status.get_src_logger_name().split('-')[-1]
                    check_name = f'base_{check_name}'
                    copy_status.set_qaqc_check(
                        f'{self.qaqc_name}-{year}-{var}-{check_name}')

                    check_sub_statuses[copy_status.get_qaqc_check()] = \
                        copy_status

                # Bundle by var
                qaqc_check = f'{self.qaqc_name}-{year}-{var}'
                var_sub_statuses[qaqc_check] = \
                    StatusGenerator().composite_status_generator(
                        logger=Logger().getLogger(qaqc_check),
                        qaqc_check=qaqc_check,
                        statuses=check_sub_statuses,
                        plot_paths=check_plots)

                if check_plots:
                    var_plots.extend(check_plots)

            var_plots = list(set(var_plots))
            if var_plots == []:
                var_plots = None

            # Bundle by year
            qaqc_check = f'{self.qaqc_name}-{year}'
            statuses.append(StatusGenerator().composite_status_generator(
                logger=Logger().getLogger(qaqc_check),
                qaqc_check=qaqc_check,
                statuses=var_sub_statuses,
                plot_paths=var_plots
            ))

        return statuses

    def _add_result_summary_stat(self, statuses: List[Status]):
        for year_status in statuses:
            if year_status.get_sub_status() is None:
                continue

            for var_status in year_status.get_sub_status().values():
                result = StatusCode.OK
                for check_status in var_status.get_sub_status().values():
                    result = min(result, check_status.get_status_code())

                var_status.add_summary_stat('result', result)

    def _write_summary(self, statuses: List[Status]):
        summary_dir = os.path.join(self.base_plot_dir, 'summary')
        if not os.path.exists(summary_dir):
            os.makedirs(summary_dir)

        csv_headers = {
            'year': 'Period',
            'ustar_var': 'USTAR Variable',
            'fc_var': 'FC Variable',
            'result': 'Result',
            'min_USTAR_day': 'Daytime min USTAR',
            'min_USTAR:FC_day': 'Daytime min USTAR with FC',
            'min_USTAR_night': 'Nighttime min USTAR',
            'min_USTAR:FC_night': 'Nighttime min USTAR with FC',
            'figure': 'Figure link'
        }
        filename = os.path.join(summary_dir, f'{self.qaqc_name}_summary.csv')
        output_stats = OutputStats(statuses, sort_by_header='var')
        output_stats.format_variables(['ustar_var', 'fc_var'])
        output_stats.write_to_csv(filename, csv_headers)

    def _get_vars_non_gapfilled(self, base_headers: dict) -> list:
        base_headers['USTAR'] = [i for i in base_headers['USTAR'] if not
                                 self.var_util.
                                 is_var_with_gapfilled_qualifier(i)]
        return base_headers

    def driver(self, data_reader: DataReader) -> List[Status]:
        """ Main entry point for ustar_filter module. """

        self.data = data_reader.get_data()

        # Select which radiation variable to use
        self.rad_in_var = self._select_rad_var(
            header_map=data_reader.get_base_headers(),
            rad_vars=('SW_IN_POT', 'SW_IN', 'PPFD_IN'))

        if self.rad_in_var is None:
            qaqc_check = (f'{self.qaqc_name}-'
                          f'all_data-all_vars-select_rad_var')
            status_msg = 'No valid radiation variable was found'

            log_obj = Logger().getLogger(qaqc_check)
            log_obj.fatal(status_msg)

            status = StatusGenerator().status_generator(
                logger=log_obj,
                qaqc_check=qaqc_check,
                status_msg=status_msg)
            self._write_summary([status])
            return [status]

        # Check that required variables are present, return error Status if not
        status = self._check_ustar_vars_present(
            data_reader,
            required_vars=['USTAR', 'FC', self.rad_in_var])
        if status is not None:
            self._write_summary([status])
            return [status]

        # Check that required variables are not masked for entire record
        status = self._check_masked_data(data_reader.get_base_headers())
        if status is not None:
            self._write_summary([status])
            return [status]

        base_headers_non_gapfilled = \
            self._get_vars_non_gapfilled(data_reader.get_base_headers())
        yearly_metrics, plot_data = \
            self._calculate_ustar_metrics(base_headers_non_gapfilled)

        years = list(yearly_metrics.keys())
        ustar_vars = list(yearly_metrics[years[0]].keys())
        fc_vars = list(yearly_metrics[years[0]][ustar_vars[0]].keys())
        periods = ['day', 'night']

        figure_links = {}
        yearly_statuses = {}

        for year, ustar_var, fc_var, period in itertools.product(
              years, ustar_vars, fc_vars, periods):

            # Get the minimum value of USTAR wrt the current FC var
            fc_var_min = yearly_metrics[year][ustar_var][fc_var][period]

            if year not in yearly_statuses:
                yearly_statuses[year] = {}

            var = f'{ustar_var}:{fc_var}'
            if var not in yearly_statuses[year]:
                yearly_statuses[year][var] = {}

            if fc_var_min == float('inf'):
                key = f'{self.qaqc_name}-{year}-{var}-valid_min_{period}'
                yearly_statuses[year][var][key] = \
                    self._create_missing_year_status(
                        year, ustar_var, fc_var, period)
                continue

            # Get a status for any potential filtering
            stat = self._get_ustar_filtering_status(
                year, ustar_var, fc_var, period, fc_var_min)
            if stat is not None:
                key = stat.get_qaqc_check()
                yearly_statuses[year][var][key] = stat

            if fc_var != 'base':
                # Check if we've already created a plot for the given variables
                # (because day & night share plots)
                if (year, ustar_var, fc_var) in figure_links:
                    figure_link = figure_links[(year, ustar_var, fc_var)]
                # Otherwise generate a plot and return the path to the figure
                elif self.can_plot:
                    figure_link = self._make_plot(
                        year, ustar_var, fc_var, plot_data)
                    figure_links[(year, ustar_var, fc_var)] = figure_link

                # Get a status for USTAR min and diff wrt an FC var
                ustar_min = yearly_metrics[year][ustar_var]['base'][period]
                stat = self._get_ustar_diff_status(
                    year, ustar_var, fc_var, period,
                    ustar_min, fc_var_min, figure_link)
                if stat is not None:
                    key = stat.get_qaqc_check()
                    yearly_statuses[year][var][key] = stat

        statuses = self._organize_status_objects(yearly_statuses)

        # Create a csv summary file of the statuses
        self._add_result_summary_stat(statuses)
        self._write_summary(statuses)

        return statuses
