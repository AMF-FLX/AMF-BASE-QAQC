#!/usr/bin/env python

import status
from math import exp
from math import log
import numpy as np
from scipy import stats
from logger import Logger

__author__ = "You-Wei Cheah, Housen Chu"
__email__ = "ycheah@lbl.gov, hchu@berkeley.edu"

_log = Logger().getLogger(__name__)


class SpikeDetection():

    def __init__(self):
        self.const = 0.6745

    def get_threshold_const(self, kurtosis):
        numerator = exp(-.2775258 - (.1720364 * log(kurtosis)))
        denominator = exp(-.2775258 - (.1720364 * log(3)))
        return numerator / denominator

    def run_algorithm(self, time, var, threshold=4):
        """ Calculate x(t) - 1/2*[x(t-1) - x(t+1)]"""
        y_data = []
        z_data = []
        for lag, c, r in zip(var[:-2], var[1:-1], var[2:]):
            temp_y = c - ((lag - r) / 2)
            y_data.append(temp_y)

        kurtosis = stats.kurtosis(y_data, fisher=False)
        md = np.median(y_data)
        mad = np.median([abs(y - md) for y in y_data])
        threshold = self.get_threshold_const(kurtosis) * threshold
        for s in y_data:

            comparator = (threshold * mad) / self.const
            diff = s - md
            z_value = None

            if diff > comparator:
                z_value = 1
            elif diff < -comparator:
                z_value = -1
            else:
                z_value = 0

            z_data.append(z_value)

        # Classify outliers
        outliers = []
        for l_rng, c_rng, r_rng in zip(z_data[:-2], z_data[1:-1], z_data[2:]):
            if (c_rng == 1 and l_rng == -1 and r_rng == -1) or (
                    c_rng == -1 and l_rng == 1 and r_rng == 1):
                outliers.append(True)
            else:
                outliers.append(False)

        return outliers

    def driver(self, data_reader):
        _log.info('Starting spike detection checks')
        self.d = data_reader
        self.input_data = self.d.get_data()  # Get data object

        # Testing for now
        ts = self.input_data['TIMESTAMP_START']
        v = self.input_data[self.d.base_headers['TA'][0]]
        mask = ~ts.mask & ~v.mask

        ts_no_mask = []
        v_no_mask = []

        for t, v, m in zip(ts, v, mask):
            if m:
                ts_no_mask.append(t)
                v_no_mask.append(v)

        self.run_algorithm(ts_no_mask, v_no_mask, 1)
        return [status.StatusGenerator().composite_status_generator(
            _log, 'spike detection')]
