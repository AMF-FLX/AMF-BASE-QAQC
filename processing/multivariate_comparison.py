import math
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import numpy as np
import os
import statistics
import sys

from configparser import ConfigParser
from logger import Logger
from output_stats import OutputStats
from plot_config import PlotConfig
from scipy.odr import ODR
from scipy.odr import Model
from scipy.odr import RealData
from scipy import stats
from status import StatusCode, StatusGenerator
from utils import Decode
from utils import TimestampUtil, VarUtil


__author__ = 'You-Wei Cheah, Housen Chu, Fianna O''Brien, Josh Geden'
__email__ = ('ycheah@lbl.gov, hchu@berkeley.edu, flobrien@lbl.gov'
             'joshgeden10@gmail.com')

_log = Logger().getLogger(__name__)


class MultivariateComparison():
    """This is a class that handles multivariate comparison checks.
    Checks that are implemented here are biological and meteorological
    inter-relationship (e.g., PPFD_IN & SW_IN, TA & TS, WS & USTAR),
    check for short term mismatch, long term consistency, modeled data.
    """

    def __init__(self, site_id, process_id, plot_dir=None, ftp_plot_dir=None,
                 character_encoding='utf-8'):
        """Initialize variables on loading of class here"""
        self._analysis = {}
        self.decode = Decode()
        self.ts_util = TimestampUtil()
        self.var_util = VarUtil()
        self.plot_config = PlotConfig()
        self.plot = self.plot_config.plot
        self.site_id = site_id
        self.process_id = process_id
        self.fig_name_fmt = '{s}-{p}-{t}-{x}-{y}-{yr}.png'
        self._c_dt = self.ts_util.cast_as_datetime
        self.character_encoding = character_encoding
        self.color_palette = self.plot_config.multivariate_palette
        self.tol = 10 ** -(sys.float_info.dig - 1)
        self.statistics_rounding = 3

        if plot_dir:
            self.can_plot = True
            self.plot_dir = self.plot_config.get_plot_dir_for_check(
                plot_dir, __name__)
            self.base_plot_dir = plot_dir
        else:
            self.can_plot = False
            self.plot_dir = None
        if self.plot_dir and not os.path.exists(self.plot_dir):
            os.mkdir(self.plot_dir)
        self.url_path = ftp_plot_dir

        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            if config.has_section('PHASE_II'):
                self.plot_path = config.get('PHASE_II', 'output_dir')
            else:
                self.plot_path = None
                critical_msg = ('Cannot find data QAQC output directory '
                                'from config.')
                _log.critical(critical_msg)

        self.qaqc_name = 'multivariate_comparison'
        self.read_thresholds()
        self.stat_gen = StatusGenerator()
        self.statuses = []

    def read_thresholds(self):
        """ Read thresholds from qaqc config file """
        config = ConfigParser()
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            cfg_section = 'MULTIVARIATE_COMPARISON'
            if config.has_section(cfg_section):
                self.ta_t_sonic_threshold = config.getfloat(
                    cfg_section, 'ta_t_sonic_threshold')
                self.ta_t_sonic_lo_threshold = config.getfloat(
                    cfg_section, 'ta_t_sonic_lo_threshold')
                self.ta_t_sonic_up_threshold = config.getfloat(
                    cfg_section, 'ta_t_sonic_up_threshold')
                self.ta_t_sonic_delta_s_warning = config.getfloat(
                    cfg_section, 'ta_t_sonic_delta_s_warning')
                self.ta_t_sonic_delta_s_error = config.getfloat(
                    cfg_section, 'ta_t_sonic_delta_s_error')

                self.ppfd_in_sw_in_threshold = config.getfloat(
                    cfg_section, 'ppfd_in_sw_in_threshold')
                self.ppfd_in_sw_in_lo_threshold = config.getfloat(
                    cfg_section, 'ppfd_in_sw_in_lo_threshold')
                self.ppfd_in_sw_in_up_threshold = config.getfloat(
                    cfg_section, 'ppfd_in_sw_in_up_threshold')
                self.ppfd_in_sw_in_delta_s_warning = config.getfloat(
                    cfg_section, 'ppfd_in_sw_in_delta_s_warning')
                self.ppfd_in_sw_in_delta_s_error = config.getfloat(
                    cfg_section, 'ppfd_in_sw_in_delta_s_error')

                self.ws_ustar_threshold = config.getfloat(
                    cfg_section, 'ws_ustar_threshold')
                self.ws_ustar_lo_threshold = config.getfloat(
                    cfg_section, 'ws_ustar_lo_threshold')
                self.ws_ustar_up_threshold = config.getfloat(
                    cfg_section, 'ws_ustar_up_threshold')
                self.ws_ustar_delta_s_warning = config.getfloat(
                    cfg_section, 'ws_ustar_delta_s_warning')
                self.ws_ustar_delta_s_error = config.getfloat(
                    cfg_section, 'ws_ustar_delta_s_error')

                self.ta_rep_threshold = config.getfloat(
                    cfg_section, 'ta_rep_threshold')
                self.ta_level_threshold = config.getfloat(
                    cfg_section, 'ta_level_threshold')

                self.slope_deviation_warning = config.getfloat(
                    cfg_section, 'slope_deviation_warning')
                self.slope_deviation_error = config.getfloat(
                    cfg_section, 'slope_deviation_error')

                self.outlier_warning = config.getfloat(
                    cfg_section, 'outlier_warning')

                # How many years with R^2 values between lo & up threshold are
                # required before running a slope deviation check
                self.years_required_for_deviation_check = config.getint(
                    cfg_section, 'years_required_for_deviation_check')

                info_msg = ('Thresholds read from config:\n'
                            '\tTA TSONIC threshold: {t1}\n'
                            '\tPPFD_IN SW_IN threshold: {t3}\n'
                            '\tWS USTAR threshold: {t4}\n'
                            '\tTA replicate threshold: {t5}\n'
                            '\tTA level threshold: {t6}\n')
                _log.info(info_msg.format(
                    t1=self.ta_t_sonic_threshold,
                    t3=self.ppfd_in_sw_in_threshold,
                    t4=self.ws_ustar_threshold,
                    t5=self.ta_rep_threshold,
                    t6=self.ta_level_threshold))
            else:
                _log.info('No threshold specified in config, using defaults.')

    def store_analysis(self, fit, r2, x_lab, y_lab, ts_start, ts_end):
        """Stores annual regression fits"""
        key = (x_lab, y_lab)
        if not self._analysis.get(key, None):
            self._analysis[key] = {(ts_start, ts_end): (fit, r2)}
        else:
            self._analysis.get(key)[
                (ts_start, ts_end)] = (fit, r2)

    def find_vars_using_base_var(self, base_var):
        var_ls = []
        for var_name in self.input_data.dtype.names:
            if self.d.get_base_header(var_name) == base_var and \
               not self.var_util.is_var_with_general_qualifiers(var_name):
                var_ls.append(var_name)
        return var_ls

    def _get_start_end_idxs(self, idxs, vals):
        """Simple helper to return a list of the correct start
        and end indices
        """
        result = []
        for idx, v in enumerate(idxs):
            if idx + 1 < len(idxs):
                s = idxs[idx]
                e = idxs[idx+1]
            else:
                s = idxs[idx]
                e = len(vals)
            result.append((s, e))
        return result

    def gen_warning_status(self, log_obj, qaqc_check, status_msg):
        """ Generate a single message status object for a missing variable """
        self.statuses.append(self.stat_gen.generate_single_msg_warning_status(
            log_obj=log_obj, qaqc_check=qaqc_check, status_msg=status_msg))

    def gen_missing_variable_status(self, log_obj, var_1_ls, var_1,
                                    var_2_ls, var_2):
        """ Generate a single message status object for a missing variable """
        if not var_1_ls and not var_2_ls:
            status_msg = f'{var_1} and {var_2} variables are'
        elif not var_1_ls:
            status_msg = f'{var_1} variable is'
        else:
            status_msg = f'{var_2} variable is'
        status_msg += (f' not present. {var_1}-{var_2} '
                       'comparison not performed.')

        log_obj.info(status_msg)
        self.statuses.append(self.stat_gen.status_generator(
            logger=log_obj,
            qaqc_check=log_obj.getName(),
            status_msg=status_msg))

    def ta_tsonic_cross_check(self):
        init_statuses_len = len(self.statuses)
        # starting a sub logger to handle all info form this test
        logger_name = f'{self.qaqc_name}-TA:T_SONIC-cross_check'
        check_log = Logger().getLogger(logger_name)
        check_log.resetStats()
        check_log.info('Analyzing TA, T_SONIC')
        ta_base_var = 'TA'
        t_sonic_base_var = 'T_SONIC'
        ta_ls = self.find_vars_using_base_var(ta_base_var)
        t_sonic_ls = self.find_vars_using_base_var(t_sonic_base_var)

        # Check if either TA or T_SONIC are missing, don't run check if so
        if not ta_ls or not t_sonic_ls:
            self.gen_missing_variable_status(
                check_log, ta_ls, ta_base_var, t_sonic_ls, t_sonic_base_var)
            return

        self._vars_cross_check_helper(
            ta_ls, t_sonic_ls, ta_base_var, t_sonic_base_var,
            self.ta_t_sonic_threshold, check_log,
            lo_threshold=self.ta_t_sonic_lo_threshold,
            up_threshold=self.ta_t_sonic_up_threshold,
            delta_s_warning=self.ta_t_sonic_delta_s_warning,
            delta_s_error=self.ta_t_sonic_delta_s_error)

        self._check_empty_status(init_status_len=init_statuses_len,
                                 current_log=check_log)

    def ppfd_in_sw_in_cross_check(self):
        init_statuses_len = len(self.statuses)
        logger_name = f'{self.qaqc_name}-PPFD_IN:SW_IN-cross_check'
        check_log = Logger().getLogger(logger_name)
        check_log.resetStats()
        _log.info('Analyzing PPFD_IN, SW_IN')
        ppfd_in_base_var = 'PPFD_IN'
        sw_in_base_var = 'SW_IN'
        ppfd_in_ls = self.find_vars_using_base_var(ppfd_in_base_var)
        sw_in_ls = self.find_vars_using_base_var(sw_in_base_var)
        n_ppfd = len(ppfd_in_ls)
        n_sw_in = len(sw_in_ls)

        # Check if SW_IN or PPFD_IN is missing, don't run check if so
        if not sw_in_ls or not ppfd_in_ls:
            self.gen_missing_variable_status(
                check_log, sw_in_ls, sw_in_base_var,
                ppfd_in_ls, ppfd_in_base_var)
            return

        if n_ppfd != n_sw_in:
            msg = (
                f'Number of PPFD_IN {n_ppfd} and SW_IN {n_sw_in} '
                'variables differ. Only the top-level (lowest indices) '
                'will be assessed.')
            check_log.info(msg)
        elif n_ppfd > 1:
            msg = (f'{n_ppfd} PPFD_IN variables found. Only the top-level '
                   '(lowest indices) variables will be assessed.')
            check_log.info(msg)
        elif n_sw_in > 1:
            msg = (f'{n_sw_in} SW_IN variables found. Only the top level '
                   '(lowest indices) variables will be assessed.')
            check_log.info(msg)

        self._vars_cross_check_helper(
            ppfd_in_ls, sw_in_ls, ppfd_in_base_var, sw_in_base_var,
            self.ppfd_in_sw_in_threshold, check_log,
            lo_threshold=self.ppfd_in_sw_in_lo_threshold,
            up_threshold=self.ppfd_in_sw_in_up_threshold,
            delta_s_warning=self.ppfd_in_sw_in_delta_s_warning,
            delta_s_error=self.ppfd_in_sw_in_delta_s_error)

        self._check_empty_status(init_status_len=init_statuses_len,
                                 current_log=check_log)

    def ws_ustar_cross_check(self):
        init_statuses_len = len(self.statuses)
        logger_name = f'{self.qaqc_name}-WC:USTAR-cross_check'
        check_log = Logger().getLogger(logger_name)
        check_log.resetStats()
        _log.info('Analyzing WS, USTAR')
        ustar_base_var = 'USTAR'
        ws_base_var = 'WS'
        ws_ls = self.find_vars_using_base_var('WS')
        ustar_ls = self.find_vars_using_base_var(ustar_base_var)

        # Check if USTAR or WS is missing, don't run check if so
        if not ustar_ls or not ws_ls:
            self.gen_missing_variable_status(
                check_log, ustar_ls, ustar_base_var, ws_ls, ws_base_var)
            return

        ustar_comp_ls = []
        # Get the highest level USTAR if more than one found
        ustar_comp_ls = self.var_util.get_top_level_variables(
            ustar_ls, log=check_log)
        if ustar_base_var not in ustar_comp_ls and ustar_base_var in ustar_ls:
            ustar_comp_ls.append(ustar_base_var)
        for u in ustar_comp_ls:
            for w in ws_ls:
                self.analysis_handler(
                    w, u, self.ws_ustar_threshold,
                    lo_threshold=self.ws_ustar_lo_threshold,
                    up_threshold=self.ws_ustar_up_threshold,
                    delta_s_warning=self.ws_ustar_delta_s_warning,
                    delta_s_error=self.ws_ustar_delta_s_error)
        self._check_empty_status(init_status_len=init_statuses_len,
                                 current_log=check_log)

    def ta_cross_replicate_check(self):
        self._cross_replicate_check('TA', self.ta_rep_threshold)

    def ta_cross_level_check(self):
        self._cross_level_check('TA', self.ta_level_threshold)

    def _vars_cross_check_helper(self, var1_ls, var2_ls, base_var1, base_var2,
                                 threshold, log=None,
                                 lo_threshold=0.7, up_threshold=1.0,
                                 delta_s_warning=0.1, delta_s_error=0.2):

        has_base_var1 = self.var_util.fill_base_var_with_idx(
            var1_ls, base_var1)
        has_base_var2 = self.var_util.fill_base_var_with_idx(
            var2_ls, base_var2)

        grouped_var1_cache = self.var_util.group_h_profile_var(var1_ls)
        grouped_var2_cache = self.var_util.group_h_profile_var(var2_ls)

        # Merge caches for both variables
        grouped_var_cache = grouped_var1_cache.copy()
        for k, v in grouped_var2_cache.items():
            cache = grouped_var_cache.get(k)
            if cache:
                grouped_var_cache[k] = [cache, v]
            else:
                grouped_var_cache[k] = [[], v]

        for idx in grouped_var_cache.keys():
            grouped_vars = grouped_var_cache.get(idx)
            top_lvl_vars = [self.var_util.get_top_level_variables(v, log=log)
                            for v in grouped_vars]
            lowest_r_vars = [self.var_util.get_lowest_r_variable(v, log=log)
                             for v in top_lvl_vars]
            if any(v is None for v in lowest_r_vars):
                log.info('Cannot perform comparison')
                continue
            self.var_util.strip_base_vars_with_idx(
                lowest_r_vars, has_base_var1, has_base_var2)
            args = lowest_r_vars
            args.append(threshold)
            self.analysis_handler(*args, lo_threshold=lo_threshold,
                                  up_threshold=up_threshold,
                                  delta_s_warning=delta_s_warning,
                                  delta_s_error=delta_s_error)

    def _cross_level_check(self, base_var, threshold):
        init_statuses_len = len(self.statuses)
        logger_name = f'{self.qaqc_name}-{base_var}-cross_level'
        check_log = Logger().getLogger(logger_name)
        check_log.resetStats()
        _log.info(f'Cross-level check for {base_var}')

        var_ls = self.find_vars_using_base_var(base_var)
        if len(var_ls) == 0 or len(var_ls) == 1:
            self.create_missing_var_status(
                base_var, var_ls, 'Cross level', check_log)
            return

        grouped_var_cache = self.var_util.group_h_profile_var(var_ls)
        for idx in grouped_var_cache.keys():
            grouped_var_ls = grouped_var_cache.get(idx)
            check_log.info(f'Iterating over H position {idx} with '
                           f'variables {grouped_var_ls}.')
            if len(grouped_var_ls) < 2:
                check_log.info(f'Not enough variables at H position {idx} '
                               'to perform cross-level analysis.')
                continue
            while grouped_var_ls:
                self._v_level_iterate_helper(
                    grouped_var_ls, threshold)
        self._check_empty_status(init_status_len=init_statuses_len,
                                 current_log=check_log)

    def _v_level_iterate_helper(self, var_ls, threshold):
        top_lvl_vars = self.var_util.get_top_level_variables(var_ls)
        lowest_r_top_lvl_var = self.var_util.get_lowest_r_variable(
            top_lvl_vars)
        _, _, top_v, _ = self.var_util.parse_h_v_r(lowest_r_top_lvl_var)
        next_lvl_vars = self.var_util.get_nearest_lower_level_variables(
            var_ls, top_v)
        lowest_r_next_lvl_var = self.var_util.get_lowest_r_variable(
            next_lvl_vars)
        if lowest_r_next_lvl_var is not None:
            self.analysis_handler(
                lowest_r_top_lvl_var, lowest_r_next_lvl_var, threshold)
        for var in top_lvl_vars:
            var_ls.remove(var)

    def _cross_replicate_check(self, base_var, threshold):
        init_statuses_len = len(self.statuses)
        logger_name = f'{self.qaqc_name}-{base_var}-cross_replicate'
        check_log = Logger().getLogger(logger_name)
        check_log.resetStats()
        _log.info(f'Cross-replicate check for {base_var}')

        var_ls = self.find_vars_using_base_var(base_var)
        if len(var_ls) == 0 or len(var_ls) == 1:
            self.create_missing_var_status(
                base_var, var_ls, 'Cross replicate', check_log)
            return

        grouped_var_cache = self.var_util.group_h_v_profile_var(var_ls)
        for idx in grouped_var_cache.keys():
            grouped_var_ls = grouped_var_cache.get(idx)
            check_log.info(f'Iterating over H_V position {idx} with '
                           f'variables {grouped_var_ls}.')
            if len(grouped_var_ls) < 2:
                check_log.info(f'Not enough variables at H_V position {idx} '
                               'to perform cross-replicate analysis.')
                continue
            self._h_v_iterate_helper(grouped_var_ls, threshold)
        self._check_empty_status(init_status_len=init_statuses_len,
                                 current_log=check_log)

    def create_missing_var_status(self, base_var, var_ls, test_name, log_obj):
        if len(var_ls) == 0:
            status_msg = (f'No {base_var} variables are present. '
                          f'{test_name} variable comparison not performed.')
        else:
            status_msg = (f'Multiple {base_var} variables are not '
                          f'present. {test_name} variable comparison '
                          'not performed.')
        log_obj.info(status_msg)

        self.statuses.append(StatusGenerator().status_generator(
            logger=log_obj,
            qaqc_check=log_obj.getName(),
            status_msg=status_msg
        ))

    def _h_v_iterate_helper(self, var_ls, threshold):
        while var_ls:
            cur_ptr = var_ls[0]
            for var in var_ls[1:]:
                self.analysis_handler(cur_ptr, var, threshold)
            var_ls.remove(cur_ptr)

    def fit_odr(self, x, y):
        data = RealData(x, y)
        linear = Model(self.odr_linear_function)
        res = ODR(data, linear, beta0=[0, 0])
        fit = res.run()
        return fit

    def fit_lin_regression(self, x, y):
        fit = stats.linregress(x, y)
        return fit

    def get_vertical_dist_from_regres_ln(self, x, y, linreg_output):
        """Get y distance from fitted regression line.
        To be used with fit_lin_regression"""
        y1 = self.lin_reg_linear_function(linreg_output, x)
        dist = abs(y1 - y)
        return dist

    def get_ortho_dist_from_regres_ln(self, x, y, linreg_output):
        """Get shortest distance from fitted regression line.
        To be used with fit_odr

        :param x
        :param y
        :param linreg_output
        TODO: This is not correctly implemented
        """
        y1 = self.odr_linear_function(linreg_output, x)
        x1 = (y - linreg_output[1]) / linreg_output[0]
        dist = math.sqrt(((x1 - x)**2) + ((y - y1)**2))
        # If the distance is exactly 0, change it to a really small number
        # to avoid dividing by 0 below
        if dist == 0:
            dist = sys.float_info.epsilon
        ortho_dist = (abs(x1 - x) * abs(y1 - y)) / dist
        return ortho_dist

    def find_initial_year_indices(self, ts):
        ts_indices = []
        for idx, t in enumerate(ts):
            decoded_str = self.decode.byte_to_str(t)
            cond = []
            cond.append(len(t) == self.ts_util.YYYYMMDD_LEN and
                        decoded_str.endswith('0101'))
            cond.append(len(t) == self.ts_util.YYYYMMDDHH_LEN and
                        decoded_str.endswith('010100'))
            cond.append(len(t) == self.ts_util.YYYYMMDDHHMM_LEN and
                        decoded_str.endswith('01010000'))
            if any(cond):
                ts_indices.append(idx)
        return ts_indices

    def build_plot_inputs(
            self, var_analysis_results, x, y,
            outlier_analysis_results, check_label, log_obj=_log, yr='all_yrs'):
        inputs = list(var_analysis_results)
        inputs.extend(
            (x, y, outlier_analysis_results, check_label, log_obj, yr))
        return inputs

    def analysis_handler(self, x, y, outlier_threshold=None,
                         lo_threshold=0.7, up_threshold=1,
                         delta_s_warning=0.1, delta_s_error=0.2):
        """ Calls var_analysis on every year of data and for all_data.
            Performs outlier analysis, generates outlier plots, and appends
            status objects to self.statuses. """

        sub_log_name = f'multivariate_comparison-{x}:{y}'
        sub_log = Logger().getLogger(sub_log_name)
        sub_log.resetStats()

        self._analysis = {}
        x_vals = self.input_data[x]
        y_vals = self.input_data[y]
        ts_start_vals = self.input_data['TIMESTAMP_START']
        ts_end_vals = self.input_data['TIMESTAMP_END']
        check_label = '{x} and {y}'.format(x=x, y=y)

        # Track statuses and slopes for each year
        yearly_sub_statuses = {}
        yearly_slopes = {}

        yr_log_name = f'{self.qaqc_name}-all_data-{x}:{y}'
        yr_log = Logger().getLogger(yr_log_name)
        yr_log.resetStats()

        # Keep track of all generated plots
        plot_paths = {}

        # Full record analysis
        results, status_msg = self.var_analysis(
            x_vals, y_vals, x, y, ts_start_vals, ts_end_vals, all_data=True)
        if results:
            check_stats = {}
            year = 'all_data'

            _, _, _, fit, _, ss_total_y = results

            # Get Status for outlier analysis
            stat, outlier_count, fig_loc = self.create_outlier_status(
                x, y, year, results, outlier_threshold, check_label, x_vals,
                y_vals, ts_start_vals)

            total_count = len(ts_start_vals)
            outlier_percent = round((outlier_count / total_count) * 100, 2)

            stat.add_summary_stat('outlier', outlier_percent)
            check_stats[stat.get_qaqc_check()] = stat

            # Store the figure generated from outlier analysis because
            # it also applys to the following statuses
            plot_paths[year] = {}
            plot_paths[year]['outlier'] = fig_loc

            # Get Status for r2 analysis
            stat, r2 = self.create_r2_status(
                x, y, year, ss_total_y, fit, lo_threshold, up_threshold,
                fig_loc)
            stat.add_summary_stat('r2', r2)
            check_stats[stat.get_qaqc_check()] = stat

            # Get slope
            slope = round(fit.beta[0], self.statistics_rounding)
            yearly_slopes[year] = slope

            yearly_sub_statuses[year] = check_stats
        else:
            plot_paths['all_data'] = {}
            plot_paths['all_data']['outlier'] = None

            # Store the reason the var_analysis failed
            yearly_sub_statuses['all_data'] = status_msg

            self.create_overall_status_object(
                yearly_sub_statuses, plot_paths, x, y, 0,
                0, yearly_slopes, lo_threshold, up_threshold,
                delta_s_warning, delta_s_error, sub_log)

            yr_log.info('Skipping individual year analysis.')
            return

        # Keep track of slope values to calculate mean and deviation later on
        overall_slope = 0
        overall_slope_count = 0

        # Find outliers, r2 values, and slopes for every year
        ts_start_idx_ls = self.find_initial_year_indices(ts_start_vals)
        for s, e in self._get_start_end_idxs(ts_start_idx_ls, ts_start_vals):
            data_year = self.ts_util.cast_as_datetime(ts_start_vals[s]).year
            year = str(data_year)
            results, status_msg = self.var_analysis(
                x_vals[s:e], y_vals[s:e], x, y,
                ts_start_vals[s:e], ts_end_vals[s:e])
            if results:
                yearly_sub_statuses[year] = {}

                _, _, _, fit, _, ss_total_y = results

                # Get Status for outlier analysis
                stat, outlier_count, fig_loc = self.create_outlier_status(
                    x, y, year, results, outlier_threshold, check_label,
                    x_vals[s:e], y_vals[s:e], ts_start_vals[s:e])
                yearly_sub_statuses[year][stat.get_qaqc_check()] = stat

                total_count = len(ts_start_vals[s:e])
                outlier_percent = round((outlier_count / total_count) * 100, 2)
                stat.add_summary_stat('outlier', outlier_percent)

                plot_paths[year] = {}
                plot_paths[year]['outlier'] = fig_loc

                # Get Status for r2 analysis
                stat, r2 = self.create_r2_status(
                    x, y, year, ss_total_y, fit, lo_threshold, up_threshold,
                    fig_loc)
                stat.add_summary_stat('r2', r2)
                yearly_sub_statuses[year][stat.get_qaqc_check()] = stat

                # Get the slope
                slope = round(fit.beta[0], self.statistics_rounding)
                yearly_slopes[year] = slope

                overall_slope += fit.beta[0]
                overall_slope_count += 1

                self.store_analysis(
                    fit, r2, x, y, ts_start_vals[s], ts_end_vals[e-1])
            else:
                plot_paths[year] = None
                # Store the reason the var_analysis failed
                yearly_sub_statuses[year] = status_msg

        self.create_overall_status_object(
            yearly_sub_statuses, plot_paths, x, y, overall_slope,
            overall_slope_count, yearly_slopes, lo_threshold, up_threshold,
            delta_s_warning, delta_s_error, sub_log)

    def create_overall_status_object(self, yearly_sub_statuses, plot_paths, x,
                                     y, overall_slope, overall_slope_count,
                                     yearly_slopes, lo_threshold, up_threshold,
                                     delta_s_warning, delta_s_error, sub_log):
        # Create yearly fit plots if possible
        ts_start, slope, r2, x_label, y_label = \
            self.build_yearly_fit_plots()
        if ts_start and self.can_plot:
            fig_loc = self.fit_plotter(ts_start, r2, slope, x_label, y_label)

            # Replace the local file path with the url path
            fig_loc = fig_loc.replace(self.base_plot_dir, self.url_path)
            plot_paths['all_data']['slope'] = fig_loc

        # Count how many years have valid r2 values
        valid_years = 0
        for year, sub_statuses in yearly_sub_statuses.items():
            if not isinstance(sub_statuses, dict):
                continue

            for key, status in sub_statuses.items():
                if 'all_data' in key or 'r2' not in key:
                    continue

                r2 = status.get_summary_stat('r2')
                if r2 != 'not_found' and lo_threshold < r2 < up_threshold:
                    valid_years += 1

        # Calculate the deviation from mean slope if there is enough data
        log_name = f'{self.qaqc_name}-all_data-{x}:{y}-slope_check'
        log_obj = Logger().getLogger(log_name)
        log_obj.resetStats()

        if valid_years < self.years_required_for_deviation_check:
            log_obj.info('Not enough years with valid r2 values to '
                         'calculate slope deviation.')
        else:
            log_obj.info('Performing slope deviation analysis.')

        mean_slope = None
        if overall_slope_count:
            mean_slope = overall_slope / overall_slope_count

        # Perform a retroactive slope deviation analysis
        for year in yearly_sub_statuses:
            if year not in yearly_slopes:
                continue

            slope = yearly_slopes[year]
            stat, delta_s_percent = self.create_slope_status(
                x, y, year, slope, mean_slope, plot_paths['all_data']['slope'],
                delta_s_error, delta_s_warning, valid_years)

            stat.add_summary_stat('slope', slope)
            stat.add_summary_stat('delta_s', delta_s_percent)
            yearly_sub_statuses[year][stat.get_qaqc_check()] = stat

        # Assemble a composite status object for each year
        statuses = {}
        for year, sub_statuses in yearly_sub_statuses.items():

            yr_log_name = f'{self.qaqc_name}-{year}-{x}:{y}'
            yr_log = Logger().getLogger(yr_log_name)
            yr_log.resetStats()
            status_msg = None

            # Determine whether to add not_calculated for all summary_stat
            # values
            add_not_calculated_summary_stats = False

            # This means an error message was stored instead of actual
            # sub_status objects
            if isinstance(sub_statuses, str):
                status_msg = sub_statuses
                sub_statuses = None

                # Create an empty status for the missing data for all_data only
                if year != 'all_data':
                    status_msg = status_msg[:-1] + \
                        '; individual year analysis not performed.'
                    yr_log.info(status_msg)
                    continue
                else:
                    yr_log.info(status_msg)
                    add_not_calculated_summary_stats = True

            # Collect all plots for the given year
            if sub_statuses:
                plots = []
                for status in sub_statuses.values():
                    plots.extend(status.get_plot_paths())
                plots = list(set(plots))
            else:
                plots = None

            # Create a composite yearly Status object
            stat = self.stat_gen.composite_status_generator(
                    logger=yr_log,
                    qaqc_check=yr_log.getName(),
                    plot_paths=plots,
                    status_msg=status_msg,
                    statuses=sub_statuses
                )

            # Add not_calculated for all summary_stat values if they weren't
            # calculated due to an error
            if add_not_calculated_summary_stats:
                stat.add_summary_stats({
                    k: 'not_calculated' for k in
                    ('outlier', 'r2', 'slope', 'delta_s')
                })

            statuses[stat.get_qaqc_check()] = stat

        # Collect all plots generated
        all_plots = []
        for year in plot_paths:
            # Check that plot_paths isn't None for that year
            if plot_paths[year]:
                for p in plot_paths[year].values():
                    # Check that the path generated isn't None
                    if p:
                        all_plots.append(p)
        all_plots = list(set(all_plots))

        if all_plots == []:
            all_plots = None

        # Create an overall variable Status object with the yearly statuses as
        # sub_status objects
        stat = StatusGenerator().composite_status_generator(
                logger=sub_log,
                qaqc_check=sub_log.getName(),
                statuses=statuses,
                plot_paths=all_plots,
                report_type='sub_status_row')
        self.statuses.append(stat)

    def create_outlier_status(self, x, y, year, results, outlier_threshold,
                              check_label, x_vals, y_vals, ts_start_vals):
        """ Analyses whether the amount of outliers is above threshold and
            logs a message. Returns (Status, outlier_count, fig_loc) """

        # Create logger for outlier analysis
        log_name = f'{self.qaqc_name}-{year}-{x}:{y}-outlier_check'
        log_obj = Logger().getLogger(log_name)
        log_obj.resetStats()

        # Find what points are outliers
        outlier_analysis_args = list(results[:-2])  # Leave out ss_totals
        outlier_analysis_args.extend((outlier_threshold, log_name))
        outlier_ls = self.outlier_analysis(*outlier_analysis_args)

        # Make a composite plot based on outliers list
        plot_inputs = self.build_plot_inputs(
            results[:-2], x, y, outlier_ls, check_label, log_obj, year)
        plot_inputs = list(plot_inputs)
        plot_inputs.extend([x_vals, y_vals, ts_start_vals])

        # Replace the local file path with the url path
        fig_loc = self.composite_plotter(*plot_inputs)
        fig_loc = fig_loc.replace(self.base_plot_dir, self.url_path)

        if fig_loc is not None:
            plot_paths = [fig_loc]
        else:
            plot_paths = None

        # Find how many points are outliers
        outlier_count = len(outlier_ls)
        total_count = len(ts_start_vals)

        # Log a warning if there are too many outliers
        outlier_percent = round((outlier_count / total_count) * 100, 2)
        status_msg = (f'{outlier_count} / {total_count} ({outlier_percent}%) '
                      'are outliers')
        if (outlier_count / total_count > self.outlier_warning):
            log_obj.warning(status_msg)
        else:
            log_obj.info(status_msg)

        # Create an outliers Status object
        return self.stat_gen.status_generator(
            logger=log_obj,
            qaqc_check=log_obj.getName(),
            plots=plot_paths,
            status_msg=status_msg), outlier_count, fig_loc

    def create_r2_status(self, x, y, year, ss_total_y, fit, lo_threshold,
                         up_threshold, fig_loc):
        """ Calculates the r2 values from the var_analysis results and
            logs a message if there are any issues with it. Returns
            (Status, r2) """

        # Calculate R2
        log_name = f'{self.qaqc_name}-{year}-{x}:{y}-r2_check'
        log_obj = Logger().getLogger(log_name)
        log_obj.resetStats()

        status_msg = None
        if ss_total_y < 0:
            r2 = 'not_found'
            log_obj.error('Could not calculate r2 because '
                          'compute_sum_of_squares failed.')
        else:
            r2 = 1 - (fit.sum_square / ss_total_y)

            # Round the r2 value. Only display 1.00 if it is actually 1.00
            if round(r2, self.statistics_rounding) == 1.00 and r2 != 1.00:
                r2 = 0.999
            else:
                r2 = round(r2, self.statistics_rounding)

            # Check r2 thresholds
            if r2 == 1:
                status_msg = f'Calculated R2 {r2} has perfect fit of 1.'
                log_obj.error(status_msg)
            elif r2 < lo_threshold:
                status_msg = (f'Calculated R2 {r2} is less than '
                              f'{lo_threshold}')
                log_obj.warning(status_msg)
            elif r2 >= up_threshold:
                status_msg = (f'Calculated R2 {r2} is greater or equal '
                              f'to {up_threshold}')
                log_obj.warning(status_msg)
            else:
                status_msg = f'Calculated R2 is {r2}'
                log_obj.info(status_msg)

        if fig_loc is None:
            plots = None
        else:
            plots = [fig_loc]

        # Create an R2 Status object
        return self.stat_gen.status_generator(
            logger=log_obj,
            qaqc_check=log_obj.getName(),
            plots=plots,
            status_msg=status_msg), r2

    def create_slope_status(self, x, y, year, slope, mean_slope, fig_loc,
                            delta_s_error, delta_s_warning,
                            valid_years):
        """ Analyses if there are any issues with the slope. Determines
            whether or not to calculate slope deviation (delta_s). Logs
            a message about any issues and returns (Status, delta_s (in %))
        """

        # Create a slope logger
        log_name = f'{self.qaqc_name}-{year}-{x}:{y}-slope_check'
        log_obj = Logger().getLogger(log_name)
        log_obj.resetStats()

        # Don't calculate slope deviation for all_data
        if year == 'all_data':
            delta_s = 'NA'
            status_msg = (f'Slope: {slope}; slope deviation: NA - slope for '
                          'all_data is the slope mean')
            log_obj.info(status_msg)
        # Check if there's enough years with valid R2 values to do a deviation
        # check
        elif valid_years < self.years_required_for_deviation_check:
            delta_s = 'not_calculated'
            status_msg = (f'Slope: {slope}; slope deviation: not calculated - '
                          'insufficient data.')
            log_obj.info(status_msg)
        # Check that mean_slope was correctly calculated
        elif not mean_slope:
            delta_s = 'not_found'
            status_msg = (f'Slope: {slope}; cannot find slope '
                          'deviation - error calculating mean_slope')
            log_obj.error(status_msg)
        else:
            delta_s = (slope - mean_slope) / mean_slope
            delta_s = round(delta_s * 100, 2)

            delta_s_error_percent = round(delta_s_error * 100, 2)
            delta_s_warning_percent = round(delta_s_warning * 100, 2)

            # Log a message about the delta_s
            if abs(delta_s) > delta_s_error_percent:
                status_msg = (f'Slope: {slope}; slope deviation '
                              f'{delta_s}% is greater than error '
                              f'threshold {delta_s_error_percent}%')
                log_obj.error(status_msg)
            elif abs(delta_s) > delta_s_warning_percent:
                status_msg = (f'Slope: {slope}; slope deviation '
                              f'{delta_s}% is greater than warning '
                              f'threshold {delta_s_warning_percent}%')
                log_obj.warning(status_msg)
            else:
                status_msg = (f'Slope: {slope}; slope deviation: '
                              f'{delta_s}%')
                log_obj.info(status_msg)

        if fig_loc is None:
            plots = None
        else:
            plots = [fig_loc]

        return self.stat_gen.status_generator(
            logger=log_obj,
            qaqc_check=log_obj.getName(),
            plots=plots,
            status_msg=status_msg), delta_s

    def compute_sum_of_squares(self, x_ls, y_ls, log=_log):
        ss_total_x = 0
        ss_total_y = 0
        try:
            mean_x = statistics.mean(x_ls)
            mean_y = statistics.mean(y_ls)
            for x in x_ls:
                ss_total_x += (x - mean_x)**2
            for y in y_ls:
                ss_total_y += (y - mean_y)**2
        except Exception as e:
            log.error(
                'Unable to compute sum of squares with error {}'.format(e))
            return -1, -1
        return ss_total_x, ss_total_y

    def build_yearly_fit_plots(self):
        ts_start = []
        slope = []
        r2 = []

        x_label = ''
        y_label = ''
        for var_pairs, annual_stats in self._analysis.items():
            for time_range, attrs in annual_stats.items():
                fit = attrs[0]
                ts_start.append(time_range[0])
                slope.append(fit.beta[0])
                r2.append(attrs[1])

        if len(self._analysis) > 0:
            x_label = var_pairs[0]
            y_label = var_pairs[-1]
            ts_start, slope = (list(x) for x in zip(
                *sorted(zip(ts_start, slope))))
        return ts_start, slope, r2, x_label, y_label

    def var_analysis(self, x_vals, y_vals, x_label, y_label,
                     ts_start_vals, ts_end_vals, all_data=False):
        """ Returns the masked_x values, masked_y values, overall fit, sum of
            squares for x values, and sum of squares for y values """

        year_start = str(self.ts_util.cast_as_datetime(ts_start_vals[0]).year)
        year_end = str(self.ts_util.cast_as_datetime(ts_start_vals[-1]).year)

        if all_data:
            year = 'all_data'
        elif year_start != year_end:
            year = f'{year_start} - {year_end}'
        else:
            year = year_start

        s = ts_start_vals[0].decode(self.character_encoding)
        e = ts_end_vals[-1].decode(self.character_encoding)

        all_x_masked = all(x_vals.mask)
        all_y_masked = all(y_vals.mask)

        if all_x_masked and all_y_masked:
            status_msg = (f'All {x_label} and {y_label} values are missing '
                          f'for {year}: (timestamps: {s} to {e}).')
            return False, status_msg
        elif all_x_masked:
            status_msg = (f'All {x_label} values are missing for {year} '
                          f'(timestamps: {s} to {e}).')
            return False, status_msg
        elif all_y_masked:
            status_msg = (f'All {y_label} values are missing for {year} '
                          f'(timestamps: {s} to {e}).')
            return False, status_msg

        mask = ~x_vals.mask & ~y_vals.mask
        masked_x = []
        masked_y = []
        masked_ts = []
        for m, x_val, y_val, s, e in zip(
                mask, x_vals, y_vals, ts_start_vals, ts_end_vals):
            if m:
                masked_x.append(x_val)
                masked_y.append(y_val)
                masked_ts.append((s, e))

        if masked_x == [] and masked_y == []:
            status_msg = (f'No timestamps found where {x_label} and {y_label} '
                          f'both have unmasked values.')
            return False, status_msg

        masked_x = np.asarray(
            masked_x, dtype=x_vals.dtype)
        masked_y = np.asarray(
            masked_y, dtype=y_vals.dtype)
        fit = self.fit_odr(masked_x, masked_y)
        # fit = self.fit_lin_regression(masked_x, masked_y)
        ss_total_x, ss_total_y = self.compute_sum_of_squares(
            masked_x, masked_y)
        return (masked_x, masked_y, masked_ts,
                fit, ss_total_x, ss_total_y), None

    def outlier_analysis(self, masked_x, masked_y, masked_ts, fit,
                         outlier_threshold=None, test_log=_log):
        outlier_ls = []
        if fit.res_var < 0:
            _log.debug('Negative fit.res_var {}'.format(fit.res_var))
        rse = math.sqrt(abs(fit.res_var))
        for x0, y0, ts in zip(masked_x, masked_y, masked_ts):
            if outlier_threshold:
                is_outlier = self.classify_outliers(
                    x0, y0, fit, rse, outlier_threshold, test_log=test_log)
            else:
                is_outlier = self.classify_outliers(
                    x0, y0, fit, rse, test_log=test_log)
            if is_outlier:
                outlier_ls.append((x0, y0, ts))
                # start_time = ts[0]
                # end_time = ts[1]
                # warning_msg = (
                #     'Outlier classified for '
                #     '{x} and {y} '.format(x=x0, y=y0) +
                #     'with timestamp range {s} and {e}'.format(
                #         s=start_time, e=end_time))
                # test_log.warning(warning_msg)
        return outlier_ls

    def fit_plotter(self, ts_start, r2, slope, x_label, y_label):
        dt_num = [self.ts_util.timestamp_str_to_num(t) for t in ts_start]

        plt.cla()
        plt.clf()
        plt.close()

        fig, ax1 = plt.subplots()
        fig.subplots_adjust(top=0.9)
        std_plt_args = {}
        # std_plt_args['linestyle'] = '--'
        std_plt_args['linewidth'] = 2

        # Formatting title
        text1 = 'Multivariate Comparison | '
        text2 = f'Slope of fit for {x_label} and {y_label}'

        renderer = fig.canvas.get_renderer()
        bbox1 = fig.text(0, 0, text1, ha='left', va='top', fontsize=10
                         ).get_window_extent(renderer=renderer)

        fig.text(0.01, 0.99, text1, ha='left', va='top', fontsize=10)
        fig.text(0.01 + bbox1.width / fig.bbox.width, 0.99, text2,
                 ha='left', va='top', fontsize=10, fontweight='bold')

        std_plt_args['linestyle'] = '-'
        std_plt_args['fmt'] = 'o'
        std_plt_args['label'] = 'r2'
        std_plt_args['color'] = self.color_palette[0]
        ax1.plot_date(dt_num, r2, **std_plt_args)

        # This is a hack for putting together legends for data on two axes
        std_plt_args['linestyle'] = '--'
        std_plt_args['fmt'] = 'D'
        std_plt_args['label'] = 'Slope'
        std_plt_args['color'] = self.color_palette[0]
        # ax1.plot_date(dt_num[0], np.nan, **std_plt_args)

        # Set labels on plot
        ax1.set_ylabel('R2')
        ax1.set_xlabel('Year')
        # Force x-axis to plot the same values as data points
        ax1.xaxis.set_ticks(dt_num)

        # Plot 2nd axis
        ax2 = ax1.twinx()
        ax2.plot_date(dt_num, slope, **std_plt_args)
        ax2.set_ylabel('Slope')

        fig.autofmt_xdate()
        plt.tight_layout()
        box = ax1.get_position()
        ax1.set_position([box.x0, box.y0,
                          box.width, box.height * 0.8])
        ax2.set_position([box.x0, box.y0,
                          box.width, box.height * 0.8])

        leg = fig.legend(loc='upper left', ncol=1, bbox_to_anchor=(0.01, 0.97),
                         title='Plot Symbols', handletextpad=0.5,
                         columnspacing=1.5, fontsize=7, frameon=True,
                         title_fontproperties={'weight':'bold'})
        leg._legend_box.align = "left"

        bbox = leg.get_window_extent()
        bbox = bbox.transformed(fig.dpi_scale_trans.inverted())

        # Calculate the width of the main legend
        legend_width = bbox.width
        new_legend_x = 0.01 + legend_width / fig.get_size_inches()[0]

        # Summary Statistics Legend
        # deviated_years_label = f'Years {self.deviated_years} ' \
        #     f'had relatively deviated regression slopes'
        # deviated_slopes_label = f'Diff ({len(self.deviated_slopes)}): ' \
        #     f'{self.deviated_slopes}'

        # handles_analysis = [mlines.Line2D([], [], linestyle='',
        #                                   label=deviated_years_label),
        #                     mlines.Line2D([], [], linestyle='',
        #                                   label=deviated_slopes_label)]
        # labels_analysis = [deviated_years_label, deviated_slopes_label]
        # leg_stats = plt.figlegend(handles_analysis, labels_analysis,
        #                           loc='upper left', ncol=1,
        #                           bbox_to_anchor=(new_legend_x, 0.97),
        #                           title='Summary Statistics', fontsize=5)
        # leg_stats.get_title().set_fontweight('bold')

        # rename x and y to variable name
        fig_name = self.fig_name_fmt.format(
            s=self.site_id, p=self.process_id,
            t=f'{self.qaqc_name}_SlopeOfFit',
            x=x_label, y=y_label, yr='all_data')
        fig_loc = os.path.join(self.plot_dir, fig_name)
        plt.savefig(fig_loc, dpi=self.plot_config.plot_default_dpi)
        plt.close()
        return fig_loc

    def composite_plotter(
            self, masked_x, masked_y, masked_ts, fit, x_label, y_label,
            outlier_ls, check_label, plot_log, yr, x_val, y_val, ts_start):
        if not self.can_plot:
            return

        info_msg = ('Plotting output between {s} and {e}'.format(
            s=masked_ts[0][0], e=masked_ts[-1][-1]))
        plot_log.info(info_msg)

        text1 = 'Multivariate Comparison | '
        text2 = f'{x_label} and {y_label}'
        text3 = f' | {yr}'

        # Create a figure to calculate text widths
        fig = plt.figure(figsize=(12, 18))

        # Calculate the widths of each text segment
        renderer = fig.canvas.get_renderer()
        bbox1 = fig.text(0, 0, text1, ha='left', va='top',
                         fontsize=16).get_window_extent(renderer=renderer)
        bbox2 = fig.text(0, 0, text2, ha='left', va='top',
                         fontsize=16, fontweight='bold'
                         ).get_window_extent(renderer=renderer)

        # Starting x position
        x_start = 0.03

        # Set the text positions dynamically based on the calculated widths
        fig.text(x_start, 0.99, text1, ha='left', va='top', fontsize=16)
        fig.text(x_start + bbox1.width / fig.bbox.width, 0.99,
                 text2, ha='left', va='top', fontsize=16, fontweight='bold')
        fig.text(x_start + (bbox1.width + bbox2.width) / fig.bbox.width,
                 0.99, text3, ha='left', va='top', fontsize=16)

        # First plot
        self.plot(
            masked_x, masked_y, x_label, y_label, subplot_pos=(3, 1, 1),
            is_plot_date=False, reset_all_subplots=True)

        # Calculate start and end points for fitted regression line
        xx = np.array([np.amin(masked_x), np.amax(masked_x)])
        yy = self.odr_linear_function(fit.beta, xx)
        # yy = self.lin_reg_linear_function(fit, xx)

        # Plot fitted regression line
        self.plot(xx, yy, color=self.color_palette[0], marker='', subplot_pos=(3, 1, 1),
                  linestyle='-', linewidth=1.5, is_plot_date=False)

        # Process outliers
        o_x_ls = []
        o_y_ls = []
        o_ts_ls = []

        for o in outlier_ls:
            o_x, o_y, o_ts = o
            o_x_ls.append(o_x)
            o_y_ls.append(o_y)
            o_ts_ls.append(self.ts_util.timestamp_str_to_num(o_ts[0]))

        # Plot outliers
        self.plot(o_x_ls, o_y_ls, color=self.color_palette[1], marker_size=10,
                  marker_fill=False, is_plot_date=False, subplot_pos=(3, 1, 1))

        # Create timestamp map for non-masked values
        ts_label = 'TIMESTAMP_START'
        dt_num = [self.ts_util.timestamp_str_to_num(t) for t in ts_start]

        # Plot second graph
        self.plot(dt_num, x_val, ts_label, x_label, subplot_pos=(3, 1, 2))

        # Plot outliers for 2nd graph
        self.plot(
            o_ts_ls, o_x_ls, color=self.color_palette[1], marker_size=10,
            marker_fill=False, subplot_pos=(3, 1, 2))

        # Plot third graph
        self.plot(dt_num, y_val, ts_label, y_label, subplot_pos=(3, 1, 3))

        # Plot outliers for third graph
        self.plot(
            o_ts_ls, o_y_ls, color=self.color_palette[1], marker_size=10,
            marker_fill=False, subplot_pos=(3, 1, 3))

        # Set labels and colors for points
        # (label, facecolor, edgecolor)
        legendInfo = [('data', '0.75', '0.75'),
                      ('potential outliers', 'None', self.color_palette[1])]

        # convert legend info to Line Objs
        handles = [mlines.Line2D(
            [], [], color='None', marker='o', markersize=10, label=l,
            markerfacecolor=fc, markeredgecolor=ec)
            for (l, fc, ec) in legendInfo]
        labels = [l for (l, fc, ec) in legendInfo]

        # Add labels and colors for point styling
        reg_label = 'Linear Regression between {x} and {y}'.format(
            x=x_label, y=y_label)
        handles += [mlines.Line2D([], [], color=self.color_palette[0], label=reg_label)]
        labels += [reg_label]

        # Calculating analysis details for legend
        _, ss_total_y = self.compute_sum_of_squares(
            masked_x, masked_y)
        r2 = 1 - (fit.sum_square / ss_total_y)
        num_flagged = len(outlier_ls)
        total_points = len(masked_x)
        percent_flagged = round((num_flagged / total_points) * 100, 2)

        flagged_label = f'Flagged data points = ' \
            f'{num_flagged}/{total_points} ({percent_flagged}%)'
        analysis_label = ("Slope = {slope}, "
        "intercept = {intercept}, R2 = {r2}").format(
            slope=round(fit.beta[0], self.statistics_rounding),
            intercept=round(fit.beta[1], self.statistics_rounding),
            r2=round(r2, self.statistics_rounding))

        handles_analysis = [mlines.Line2D([], [],
                                          linestyle='', label=flagged_label),
                            mlines.Line2D([], [], linestyle='',
                                          label=analysis_label)]
        labels_analysis = [flagged_label, analysis_label]

        leg = plt.figlegend(handles, labels, loc='upper left',
                            ncol=1, bbox_to_anchor=(0.01, 0.97),
                            title='Plot Symbols')
        leg.get_title().set_fontweight('bold')
        bbox = leg.get_window_extent()
        bbox = bbox.transformed(fig.dpi_scale_trans.inverted())

        #  Calculate the width of the main legend
        legend_width = bbox.width
        new_legend_x = 0.01 + legend_width / fig.get_size_inches()[0]
        leg_stats = plt.figlegend(handles_analysis,
                                  labels_analysis, loc='upper left',
                                  ncol=1,
                                  bbox_to_anchor=(new_legend_x, 0.97),
                                  title='Summary Statistics')
        leg_stats.get_title().set_fontweight('bold')

        fig_name = self.fig_name_fmt.format(
            s=self.site_id, p=self.process_id, t=self.qaqc_name,
            x=x_label, y=y_label, yr=yr)
        fig_loc = os.path.join(self.plot_dir, fig_name)
        plt.savefig(fig_loc, dpi=self.plot_config.plot_default_dpi)
        plt.close()
        return fig_loc

    def classify_outliers(self, x, y, fit, rse, threshold=4.5, test_log=_log):
        """Classify whether a point is an outlier based on the residual
        standard deviation from the regression line

        """
        dist = self.get_ortho_dist_from_regres_ln(x, y, fit.beta)
        # dist = self.get_vertical_dist_from_regres_ln(x, y, output)
        # test_log.info("Dist: {d}, std residual {r}".format(d=dist, r=rse))
        return dist > (rse * threshold) + self.tol

    def odr_linear_function(self, B, x):
        return B[0]*x + B[1]

    def lin_reg_linear_function(self, fit, x):
        return fit.slope*x + fit.intercept

    def _check_empty_status(self, init_status_len, current_log):
        """
        check for empty status for a multivariate comparison test. if
            there is no status, return an OK return with some general message
            that the check was not run (as expected)
        :param init_status_len:
        :param current_log:
        :return: none (write directly to the self.statuses variable)
        """
        empty_status, stat_obj = \
            self.stat_gen.check_for_empty_status(
                init_status_len=init_status_len,
                current_status_len=len(self.statuses),
                warning_msg='Multivariate Comparison check for {c} '
                'was not performed. This may be expected if variables '
                'at multiple levels or with multiple replicates '
                'are not present in the data.'
                .format(c=current_log.getName()),
                current_log=current_log)
        if not empty_status:
            return
        elif stat_obj:
            self.statuses.append(stat_obj)
        else:
            _log.warning('Expected warning message not generated in '
                         '_check_empty_status. Check this out.')

    def write_summary(self):
        summary_dir = os.path.join(self.base_plot_dir, 'summary')
        if not os.path.exists(summary_dir):
            os.makedirs(summary_dir)

        csv_headers = {
            'year': 'Period',
            'var1': 'Variable 1',
            'var2': 'Variable 2',
            'result': 'Result',
            'slope': 'Regression slope',
            'r2': 'Regression R2',
            'delta_s': 'Slope deviation (%)',
            'outlier': 'Outliers (%)',
            'figure_0': 'Figure 1',
            'figure_1': 'Figure 2'
        }
        filename = os.path.join(summary_dir, f'{self.qaqc_name}_summary.csv')
        output_stats = OutputStats(self.statuses, sort_by_header='var')

        # Turns the 'var' header into 'var1' and 'var2'
        output_stats.format_variables(['var1', 'var2'])

        # Turns the 'figure' header into 'figure_0' and 'figure_1'
        output_stats.split_plot_column(n_columns=2)
        output_stats.write_to_csv(filename, csv_headers)

    def add_result_summary_stat(self):
        for variable_status in self.statuses:
            if variable_status.get_sub_status() is None:
                continue

            for year_status in variable_status.get_sub_status().values():
                result_code = StatusCode.OK
                for check_status in year_status.get_sub_status().values():

                    # Get the worst (minimum) status_code of all check_stats
                    status_code = check_status.get_status_code()
                    result_code = min(result_code, status_code)

                year_status.add_summary_stat('result', result_code)

    def driver(self, data_reader):
        """ This is a driver to test and run QAQC Algorithm
        specific to this class
        """
        _log.info('Starting multivariate comparison')
        self.d = data_reader
        self.input_data = self.d.get_data()  # Get data object

        # Cross variable checks
        self.ta_tsonic_cross_check()
        self.ws_ustar_cross_check()
        self.ppfd_in_sw_in_cross_check()

        # Cross level checks
        self.ta_cross_level_check()

        # Cross replicate checks
        self.ta_cross_replicate_check()

        # Save results to csv summary file
        self.add_result_summary_stat()
        self.write_summary()

        return self.statuses, self.plot_dir


if __name__ == '__main__':
    # No use
    sys.exit('ERROR: Do not use this module on its own')
