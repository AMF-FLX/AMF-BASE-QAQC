import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os

from configparser import ConfigParser
from data_reader import DataReader
from datetime import datetime as dt
from logger import Logger
from plot_config import PlotConfig
from status import Status, StatusGenerator
from typing import List
from utils import VarUtil


__author__ = 'Josh Geden'
__email__ = 'joshgeden10@gmail.com'

_log = Logger().getLogger(__name__)


class VariableCoverage:
    def __init__(self, encoding='ascii'):
        self.vars = {}
        self.years = []
        self.required_min = 0.0
        self.encouraged_min = 0.0
        self.encoding = encoding
        self.var_util = VarUtil()
        self.plot_config = PlotConfig()
        config = ConfigParser()

        # Find required, encouraged, & suggested variables from config file
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            if config.has_section('PHASE_II'):
                self.plot_path = config.get('PHASE_II', 'output_dir')
            else:
                self.plot_path = None
                _log.critical('Cannot find data QAQC output '
                              'directory from config.')
            if config.has_section('ONEFLUX_VARIABLES'):
                self.vars['required'] = eval(config.get('ONEFLUX_VARIABLES',
                                                        'required_variables'))
                self.required_min = float(config.get('ONEFLUX_VARIABLES',
                                                     'required_min'))
                self.vars['encouraged'] = eval(config.get(
                    'ONEFLUX_VARIABLES', 'encouraged_variables'))
                self.encouraged_min = float(config.get('ONEFLUX_VARIABLES',
                                                       'encouraged_min'))
                self.vars['suggested'] = eval(config.get(
                    'ONEFLUX_VARIABLES', 'suggested_variables'))
            else:
                self.vars = None
                _log.critical('Cannot find ONEFlux variables from config')

    def _get_start_end_idx(self, data):
        """Returns start and end indecies for years in data"""
        """Start by finding start indicies"""
        ts_indices = []
        for idx, t in enumerate(data['TIMESTAMP_START']):
            if t.decode(self.encoding).endswith('01010000'):
                ts_indices.append(idx)

        # If no start years were found, we start in the middle of year at the
        # 0th index
        if len(ts_indices) == 0:
            ts_indices = [0]

        annual_seg_idxs = {}
        for idx, v in enumerate(ts_indices):
            if idx + 1 < len(ts_indices):
                start = ts_indices[idx]
                end = ts_indices[idx + 1] - 1
            else:
                start = ts_indices[idx]
                end = len(data) - 1
            name = data['TIMESTAMP_START'][start][:4].decode(self.encoding)
            annual_seg_idxs[name] = {'start': start, 'end': end}
        annual_seg_idxs['all_data'] = {'start': 0, 'end': len(data) - 1}
        return annual_seg_idxs

    def _get_days_in_year(self, year):
        """ Get the exact number of days in a given year """
        year = int(year)
        return (dt(year+1, 1, 1) - dt(year, 1, 1)).days

    def calculate_coverage(self, data_reader: DataReader):
        """ Calculates the coverage for each var in self.vars.
            Returns a dict of {base_var: {year: coverage_percent}} for every
            var in self.vars and for every year in self.years. Also returns
            a list of percents for coverage for each year and a list of
            percents for coverage for each period of reported timestamps
        """

        coverage_by_timestamps = np.zeros(shape=(len(self.vars_list),
                                                 len(self.years)))
        coverage_by_year = np.zeros(shape=(len(self.vars_list),
                                           len(self.years)))

        # Dict with coverage percents by variable by year to return
        coverage_dict = {}

        for idx, var in enumerate(self.vars_list):
            cvg_by_timestamps = []
            cvg_by_year = []

            base_var = data_reader.get_base_header(var)
            if self.var_util.is_var_with_gapfilled_qualifier(var):
                base_var = f'{base_var}_F'
            if base_var not in coverage_dict:
                coverage_dict[base_var] = {}

            for year in self.years:
                start, end = (self.annual_idx[year]['start'],
                              self.annual_idx[year]['end'])
                annual_data = self.data[var][start:end+1]

                # Only count the values that aren't masked
                count_vars = len(annual_data[~annual_data.mask])

                # If resolution is HR, there are 24 timestamps each day
                timestamps_per_year = self._get_days_in_year(year) * 24
                # For HH resolution double the number of timestamps
                if self.resolution == 'HH':
                    timestamps_per_year *= 2

                cvg_by_timestamps.append(count_vars / len(annual_data))
                cvg_by_year.append(count_vars / timestamps_per_year)

                # Store the highest coverage for a base_var
                # E.g. if G_1_1_1 has coverage 0.99 and G_2_1_1 has
                # coverage 0.50 then coverage_dict['G']['year'] = 0.99

                if year not in coverage_dict[base_var]:
                    coverage_dict[base_var][year] = \
                        count_vars / timestamps_per_year
                else:
                    coverage_dict[base_var][year] = max(
                        count_vars / timestamps_per_year,
                        coverage_dict[base_var][year]
                    )

            coverage_by_timestamps[idx] = cvg_by_timestamps
            coverage_by_year[idx] = cvg_by_year

        return coverage_dict, coverage_by_year, \
            coverage_by_timestamps

    def heatmap(self, data_reader, data, row_labels, col_labels,
                cbar_kw={}, cbarlabel='', **kwargs):
        """
        Create a heatmap from a numpy array and two lists of labels.

        Adapted from:
        https://matplotlib.org/stable/gallery/images_contours_and_fields/image_annotated_heatmap.html

        Parameters
        ----------
        data
            A 2D numpy array of shape (N, M).
        row_labels
            A list or array of length N with the labels for the rows.
        col_labels
            A list or array of length M with the labels for the columns.
        cbar_kw
            A dictionary with arguments to `matplotlib.Figure.colorbar`.
            Optional.
        cbarlabel
            The label for the colorbar.  Optional.
        **kwargs
            All other arguments are forwarded to `imshow`.
        """

        # Plot the heatmap
        ax = plt.gca()
        im = ax.imshow(data, vmax=1, **kwargs)

        # Create colorbar
        cbar = None
        if cbarlabel != '':
            cbar = ax.figure.colorbar(im, ax=ax, orientation='vertical',
                                      fraction=0.046, pad=0.04,
                                      **cbar_kw)
            cbar.ax.set_ylabel(cbarlabel, rotation=-90, va='bottom')
            cbar.set_ticks([0, 0.2, 0.4, 0.6, 0.8, 1])
            cbar.set_ticklabels(["0%", "20%", "40%", "60%", "80%", "100%"])

        # We want to show all ticks...
        ax.set_xticks(np.arange(data.shape[1]))
        ax.set_yticks(np.arange(data.shape[0]))
        # ... and label them with the respective list entries.
        ax.set_xticklabels(col_labels)
        ax.set_yticklabels(row_labels)

        # Flatten the list of required vars to contain only str entries
        flat_list = []
        for var in self.vars['required']:
            if isinstance(var, tuple):
                for v in var:
                    flat_list.append(v)
            else:
                flat_list.append(var)

        for label in ax.get_yticklabels():
            base = data_reader.get_base_header(label._text)
            # Ignore qualifier variables with _F or _PI component
            if not self.var_util.is_var_with_general_qualifiers(label._text) \
               and base in flat_list:
                label.set_fontweight('bold')

        # Let the horizontal axes labeling appear on top.
        ax.tick_params(top=True, bottom=False,
                       labeltop=True, labelbottom=False)

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=-30, ha='right',
                 rotation_mode='anchor')

        ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
        ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
        ax.grid(which='minor', color='w', linestyle='-', linewidth=5)
        ax.tick_params(which='minor', bottom=False, left=False)

        return im, cbar

    def annotate_heatmap(self, im, data=None, valfmt='{x:.2f}',
                         textcolors=('black', 'white'),
                         empty_cell_fmt='', **textkw):
        """
        A function to annotate a heatmap.

        Adapted from:
        https://matplotlib.org/stable/gallery/images_contours_and_fields/image_annotated_heatmap.html

        Parameters
        ----------
        im
            The AxesImage to be labeled.
        data
            Data used to annotate.  If None, the image's data is used.
            Optional.
        valfmt
            The format of the annotations inside the heatmap.  This should
            either use the string format method, e.g. '$ {x:.2f}', or be a
            `matplotlib.ticker.Formatter`.  Optional.
        textcolors
            A pair of colors.  The first is used for values below a threshold,
            the second for those above.  Optional.
        **kwargs
            All other arguments are forwarded to each call to `text` used to
            create the text labels.
        """

        if not isinstance(data, (list, np.ndarray)):
            data = im.get_array()

        # Set threshold to midpoint of the range [0, 1.0]
        threshold = 0.5

        # Set default alignment to center, but allow it to be
        # overwritten by textkw.
        kw = dict(horizontalalignment='center',
                  verticalalignment='center')
        kw.update(textkw)

        # Get the formatter in case a string is supplied
        if isinstance(valfmt, str):
            valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

        # Loop over the data and create a `Text` for each 'pixel'.
        # Change the text's color depending on the data.
        texts = []
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                kw.update(color=textcolors[
                    int(im.norm(data[i, j]) > threshold)])
                if data[i, j] == 0:
                    text = im.axes.text(j, i, empty_cell_fmt, **kw)
                else:
                    text = im.axes.text(j, i, int(data[i, j]*100), **kw)
                texts.append(text)

        return texts

    def make_plots(self, data_reader: DataReader, site_id: str,
                   process_id: str, vars: List[str], years: List[str],
                   coverage_by_year: List[float],
                   coverage_by_timestamps: List[float]):

        """ Creates two plots for variable coverage by year and variable
            coverage by reported timestamps. Generates the figures, makes
            calls to heatmap() and annotate_heatmap() to handle the plot
            generation and axes labeling, and then saves them """

        # Get the output dir for the plot and ensure it exists
        output_dir = os.path.join(self.plot_path, site_id, process_id,
                                  'output', 'variable_coverage')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Create the path to save the yearly plot
        plt_name = f'{site_id}-Variable_Coverage-by_Year'
        fig_loc = os.path.join(output_dir, plt_name)

        # Make the width grow with the number of years
        # Have a minimum figure width of 12 inches
        fig_width = max(12, int(0.8 * len(years)))

        # Make the height grow with the number of vars
        # Have a minimum figure height of 13 inches
        fig_height = max(13, int(0.3 * len(vars)))

        # Generate the coverage by year figure
        plt.figure('Variable_Coverage-1', figsize=(fig_width, fig_height))
        plt.title(f'{site_id}: variable coverage by year\n',
                  fontsize=self.plot_config.plot_suptitle_fontsize,
                  ha='center')

        im, _ = self.heatmap(data_reader,
                             coverage_by_year,
                             vars,
                             years,
                             interpolation='nearest', aspect=0.5,
                             cmap='Blues', cbarlabel='Percent coverage')

        self.annotate_heatmap(im, valfmt='{x:.2f}')

        plt.savefig(fig_loc, bbox_inches='tight', pad_inches=1,
                    dpi=self.plot_config.plot_default_dpi)
        plt.close()

        # Create the path to save the timestamps plot
        plt_name = f'{site_id}-Variable_Coverage-by_Reported_Timestamps'
        fig_loc = os.path.join(output_dir, plt_name)

        fig_width = max(12, int(0.8 * len(years)))
        fig_height = max(13, int(0.3 * len(vars)))

        # Generate the coverage by reported timestamps figure
        plt.figure('Variable_Coverage-2', figsize=(fig_width, fig_height))
        plt.title(f'{site_id}: variable coverage by reported timestamps\n',
                  fontsize=self.plot_config.plot_suptitle_fontsize,
                  ha='center')

        im, _ = self.heatmap(data_reader,
                             coverage_by_timestamps,
                             vars,
                             years,
                             interpolation='nearest', aspect=0.5,
                             cmap='Blues', cbarlabel='Percent coverage')

        self.annotate_heatmap(im, valfmt='{x:.2f}')

        plt.savefig(fig_loc, bbox_inches='tight', pad_inches=1,
                    dpi=self.plot_config.plot_default_dpi)
        plt.close()

    def get_status(self, qaqc_check: str, status_msg: str,
                   log_obj: Logger) -> Status:
        """ Returns a status object to represent checking a variable
            for coverage """
        stat_gen = StatusGenerator()

        return stat_gen.status_generator(logger=log_obj,
                                         qaqc_check=qaqc_check,
                                         status_msg=status_msg)

    def below_threshold(self, coverage_dict: dict, var: str,
                        var_type: str, year: str) -> bool:
        """ Returns whether a variable is missing or below the coverage
            threshold for a year. Logs a warning if either of these is true"""

        below_threshold = False

        # If the variable is an or option (like ('SW_IN', 'PPFD_IN')), then
        # only one must be present. Uses the coverage of the first option
        # found
        if isinstance(var, tuple):
            for v in var:
                if v in coverage_dict:
                    if not self.below_threshold(
                            coverage_dict, v, var_type, year):

                        return False
            else:
                return True

        if (var_type == 'required' and
                coverage_dict[var][year] < self.required_min or
                var_type == 'encouraged' and
                coverage_dict[var][year] < self.encouraged_min):

            below_threshold = True

        return below_threshold

    def is_missing(self, var, coverage_dict) -> bool:
        """ Returns if a variable or one variable in a tuple is completely
            missing from the coverage_dict for every year """

        if isinstance(var, tuple):
            for v in var:
                if v in coverage_dict:
                    return False
            return True
        return var not in coverage_dict

    def driver(self, data_reader: DataReader, site_id: str,
               process_id: str, resolution: str) -> List[Status]:
        """ Main entrypoint for VariableCoverage module. Will calculate
            variable coverage by year and by reported timestamps for all
            required, encouraged, suggested (set in config) and included
            variables. Then generates heatmaps of this information and logs
            warnings if any variable does not meet the expected threshold.
            Returns a list of Status objects representing missing and below
            thershold variables for each variable type."""

        self.data = data_reader.get_data()
        self.resolution = resolution

        # Get all vars except for TIMESTAMP_START and TIMESTAMP_END at front
        self.vars_list = data_reader.header_as_is[2:]
        self.vars_list.sort()

        # Get start and end indexes for each year
        self.annual_idx = self._get_start_end_idx(data_reader.get_data())

        # Get all the years except 'all_data' key at the end
        self.years = list(self.annual_idx.keys())[:-1]

        coverage_dict, coverage_by_year, coverage_by_timestamps = \
            self.calculate_coverage(data_reader)

        self.make_plots(data_reader, site_id, process_id, self.vars_list,
                        self.years, coverage_by_year,
                        coverage_by_timestamps)

        qaqc_checks = {
            'required_missing':
                'variable_coverage-required_variables_missing',
            'required_below_threshold':
                'variable_coverage-required_below_threshold',
            'encouraged_missing':
                'variable_coverage-encouraged_variables_missing',
            'encouraged_below_threshold':
                'variable_coverage-encouraged_below_threshold',
            'suggested_missing':
                'variable_coverage-suggested_variables_missing'
        }

        # Logger objects to keep track of number of missing var for each type
        loggers = {
            'required_missing': Logger().getLogger(
                'variable_coverage-required_variables_missing'),
            'encouraged_missing': Logger().getLogger(
                'variable_coverage-encouraged_variables_missing'),
            'suggested_missing': Logger().getLogger(
                'variable_coverage-suggested_variables_missing'),
        }

        # Dict of status objects to be returned
        # Each category holds a dict with entries
        # like: {'var': <Status obj about var>}
        sub_statuses = {
            'required_below_threshold': {},
            'encouraged_below_threshold': {},
        }

        # Dict to keep track of missing vars per type
        missing_vars = {
            'required': [],
            'encouraged': [],
            'suggested': []
        }

        # Loop through ['required', 'encouraged', and 'suggested'] with their
        # required thresholds (suggested doesn't have a set threshold, only
        # has to be present)
        for var_type, limit in zip(self.vars.keys(),
                                   [self.required_min,
                                   self.encouraged_min,
                                   'present']):
            _log.info(f'Checking coverage for {var_type} variables is at '
                      f'least {limit}')

            for var in self.vars[var_type]:
                # Check if variable is missing
                if self.is_missing(var, coverage_dict):
                    log_obj = loggers[f'{var_type}_missing']
                    if isinstance(var, tuple):
                        log_obj.warning('/'.join(var))
                    else:
                        log_obj.warning(var)

                    missing_vars[var_type].append(var)

                # Check if the variable is below the minimum threshold
                else:
                    if isinstance(var, tuple):
                        var_log = Logger().getLogger(
                            f'variable_coverage-{var_type}_below_threshold'
                            + f'-{"/".join(var)}')
                    else:
                        var_log = Logger().getLogger(
                            f'variable_coverage-{var_type}_below_threshold-'
                            f'{var}')

                    # Keep track of years where a variable is below
                    # the coverage threshold
                    years_below_threshold = []
                    for year in self.years:
                        # Figure out if a variable is below the coverage
                        # threshold for a given year
                        below_threshold = self.below_threshold(
                            coverage_dict, var, var_type, year
                        )

                        if below_threshold:
                            years_below_threshold.append(year)

                    status_msg = None
                    if len(years_below_threshold) > 0:
                        status_msg = ', '.join(years_below_threshold)

                    if status_msg is not None:
                        var_log.warning(status_msg)
                        var_log.warning_count = len(years_below_threshold)

                    if isinstance(var, tuple):
                        qaqc_check = '/'.join(var)
                    else:
                        qaqc_check = var

                    if var_type in ['required', 'encouraged']:
                        sub_statuses[f'{var_type}_below_threshold'][var] = \
                            self.get_status(qaqc_check, status_msg, var_log)

        # Create an overall status object for each category of sub_statuses
        statuses = []
        for key in qaqc_checks:
            # For missing variables, return a single non-recursive Status obj
            if 'missing' in key:
                var_type = key.split('_')[0]

                # Formats the status_msg to be like:
                # "required variables missing: A, B, (C, D)"
                msg_list = list(map(
                            lambda x: '(' + ', '.join(x) + ')'
                            if isinstance(x, tuple) else x,
                            missing_vars[var_type]))
                if len(msg_list) == 0:
                    status_msg = None
                else:
                    status_msg = ', '.join(msg_list)

                log_obj = loggers[key]
                statuses.append(
                    StatusGenerator().composite_status_generator(
                        logger=log_obj, qaqc_check=qaqc_checks[key],
                        status_msg=status_msg
                    )
                )
            # For variables below threshold, return a Status object with a
            # sub-status object for each variable
            else:
                log_obj = Logger().getLogger(qaqc_checks[key])

                if sub_statuses[key] == {}:
                    sub_statuses[key] = None

                statuses.append(
                    StatusGenerator().composite_status_generator(
                        logger=log_obj, qaqc_check=qaqc_checks[key],
                        statuses=sub_statuses[key],
                    )
                )

        return statuses
