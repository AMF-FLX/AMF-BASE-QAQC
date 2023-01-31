import os

from configparser import ConfigParser
from pathlib import Path
from status import Status, StatusCode
from typing import List

__author__ = 'Josh Geden'
__email__ = 'joshgeden10@gmail.com'


class OutputStats:
    """ Utility class that can be used to collect and report summary statistics
        from a list of Status objects """

    def __init__(self, statuses: List[Status], sort_by_header='year',
                 rename_result_column=None):
        self.lookup_table = {}

        for status in statuses:
            self._add_status_to_table(status)

        self._sort_by(sort_by_header)

        # Check the config file to see if the result codes should be changed
        if rename_result_column is None:
            config = ConfigParser()
            with open(os.path.join(os.getcwd(), 'qaqc.cfg')) as cfg:
                config.read_file(cfg)
            if config.has_section('OUTPUT_STATS'):
                rename_result_column = config.getboolean(
                    'OUTPUT_STATS', 'rename_result_column')

        if rename_result_column:
            self._modify_result_column()

    def _add_status_to_table(self, status: Status):
        if status is None:
            return

        try:
            qaqc_check = status.get_qaqc_check()
            _, year, var, *_ = qaqc_check.split('-')

            if (year, var) not in self.lookup_table:
                self.lookup_table[(year, var)] = {}

            entry = {
                'var': var,
                'year': year,
            }

            summary_stats = status.get_summary_stats()
            if summary_stats is not None:
                entry.update(summary_stats)

            self.lookup_table[(year, var)].update(entry)

            # Only overwrite the figure link if it has not been set or
            # if the current status object has a valid figure link
            plot_paths = status.get_plot_paths()
            if 'figure' in self.lookup_table[(year, var)]:
                if plot_paths is not None:
                    # Obtain all previously found plots for this year/var in
                    # a list
                    all_figures = self.lookup_table[(year, var)]['figure']
                    all_figures = all_figures.split(';')

                    # Add the new plots and resave them as a string seperated
                    # by a semicolon
                    all_figures.extend(plot_paths)
                    all_figures = ';'.join(list(set(all_figures)))

                    # Reassign the figure stat for the given year/var
                    self.lookup_table[(year, var)]['figure'] = all_figures
            else:
                path_str = ';'.join(plot_paths)
                self.lookup_table[(year, var)].update({'figure': path_str})
        except Exception:
            pass

        if status.get_sub_status() is not None:
            for sub_status in status.get_sub_status().values():
                self._add_status_to_table(sub_status)

    def format_variables(self, var_headers: List[str]):
        # Loop through lookup table
        # Split the variable name on ':'
        # Create a new dict for the individual variables

        for summary_stats in self.lookup_table.values():
            var = summary_stats['var']
            vals = var.split(':')

            if len(vals) != len(var_headers):
                continue

            for header, header_val in zip(var_headers, vals):
                summary_stats[header] = header_val

    def split_plot_column(self, n_columns: int):
        """ Will attempt to split multiple figure paths into separate columns.
            The single figure link (stored in the lookup table with key
            'figure') will be split on ';'.
            The new columns will be 0-indexed (starting at figure_0, figure_1,
            etc.). Any number of figures beyond the n_columns specified will
            be dropped. If there are not enough plots to split into n_columns,
            the extra figure columns will have None for their value.
        """

        for entry in self.lookup_table.values():
            if 'figure' not in entry:
                continue

            figures = entry['figure'].split(';')
            for i in range(n_columns):
                if i < len(figures):
                    entry[f'figure_{i}'] = figures[i]
                else:
                    entry[f'figure_{i}'] = None

    def _sort_by(self, sort_by_header):
        """ Sorts self.summary_stats_lookup by the given header. """

        lookup_table_list = sorted(self.lookup_table.items(),
                                   key=lambda x: x[1][sort_by_header])
        self.lookup_table = dict(lookup_table_list)

    def _modify_result_column(self):
        """ Replaces the StatusCode values in the result summary stat with
            values set in the config file. Will update any column that has
            the term 'result' in it.
        """
        config = ConfigParser()
        with open(os.path.join(os.getcwd(), 'qaqc.cfg')) as cfg:
            config.read_file(cfg)

        if config.has_section('OUTPUT_STATS'):
            ok_result = config.get('OUTPUT_STATS', 'ok_result')
            warning_result = config.get('OUTPUT_STATS', 'warning_result')
            error_result = config.get('OUTPUT_STATS', 'error_result')
            fatal_result = config.get('OUTPUT_STATS', 'fatal_result')

            display_code_map = {
                StatusCode.OK: ok_result,
                StatusCode.WARNING: warning_result,
                StatusCode.ERROR: error_result,
                StatusCode.FATAL: fatal_result
            }

            for entry in self.lookup_table.values():
                for stat_name in entry:
                    if 'result' in stat_name:
                        entry[stat_name] = \
                            display_code_map[entry[stat_name]]

    def write_to_csv(self, filepath: str, headers: dict) -> None:
        """ Writes self.summary_stats_lookup as a csv file with the given
            filename. The headers argument maps internal summary field keys to
            desired csv header name.
            E.g.:
            headers = {
                'var': 'Variable',
                'year': 'Period',
                'figure': 'Figure link'
            }
            Only entries in the lookup_table that have values for all provided
            headers will be written to the file.
        """
        filepath = Path(filepath)

        # Create the filepaths parent directory if it doesn't exist
        if not os.path.exists(filepath.parent):
            os.makedirs(filepath.parent)

        with open(filepath, 'w') as f:
            f.write(','.join(headers.values()) + '\n')

            for entry in self.lookup_table.values():
                if set(headers.keys()).issubset(entry.keys()):
                    terms = [self._format_term(entry[header])
                             for header in headers]
                    f.write(','.join(terms) + '\n')

    def _format_term(self, term):
        """ Returns a string for a value to be written to the csv file """
        if isinstance(term, float) and term == 0:
            return str(0)

        return str(term)
