import sys
import numpy
import matplotlib
import status

from logger import Logger
from numpy.ma.core import MaskedConstant
from datetime import datetime
from matplotlib import pyplot, gridspec


__author__ = 'Gilberto Z. Pastorello, Sigrid Dengel'
__email__ = 'gzpastorello@lbl.gov, sdengel@lbl.gov'

# removes smoothing, force plotting all points
matplotlib.rcParams['path.simplify'] = False

log = Logger().getLogger(__name__)


class FPPError(Exception):
    """
    Base error/exception class for FPP
    """
    pass


MONTH_LABELS = {
    1: 'Jan',
    2: 'Feb',
    3: 'Mar',
    4: 'Apr',
    5: 'May',
    6: 'Jun',
    7: 'Jul',
    8: 'Aug',
    9: 'Sep',
    10: 'Oct',
    11: 'Nov',
    12: 'Dec'
}


def get_headers(filename):
    """
    Parse headers from FPFileV2 format and returns list
    of string with header labels.
    Must have at least two columns.

    :param filename: name of the FPFileV2 to be loaded
    :type filename: str
    :rtype: list
    """
    with open(filename, 'r') as f:
        line = f.readline()
    headers = line.strip().split(',')
    if len(headers) < 2:
        raise FPPError("Headers too short: '{h}'".format(h=line))
    headers = [i.strip() for i in headers]
    log.debug("headers: {h}".format(h=headers))
    return headers


STRTEST_STANDARD = ['TIMESTAMP', 'TIMESTAMP_START', 'TIMESTAMP_END']


def get_dtype(variable, resolution):
    for s in STRTEST_STANDARD:
        if s.lower() in variable.lower():
            return 'a25'
    return 'f8'


def get_fill_value(dtype):
    if dtype == 'a25':
        return ''
    elif dtype == 'i8':
        return -9999
    else:
        return numpy.NaN


def load_data_file(filename, resolution):
    log.debug("Loading: {f}".format(f=filename))
    headers = get_headers(filename=filename)
    dtype = [(h, get_dtype(h, resolution)) for h in headers]
    fill_values = [get_fill_value(dtype=d[1]) for d in dtype]
    data = numpy.genfromtxt(fname=filename, dtype=dtype, names=True,
                            delimiter=",", skip_header=0,
                            missing_values='-9999,-9999.0,-6999,-6999.0, ',
                            usemask=True)
    data = numpy.atleast_1d(data)
    data = numpy.ma.filled(data, fill_values)

    timestamp_start = [datetime.strptime(i, "%Y%m%d%H%M")
                       for i in data['TIMESTAMP_START']]
    timestamp_end = [datetime.strptime(i, "%Y%m%d%H%M")
                     for i in data['TIMESTAMP_START']]

    return data, timestamp_start, timestamp_end


def derivative(data):
    result = numpy.zeros(len(data) - 1, dtype='f8')
    for i in range(len(data) - 1):
        if isinstance(data[i + 1], MaskedConstant) or \
           isinstance(data[i], MaskedConstant):
            result[i] = 0
        else:
            result[i] = data[i + 1] - data[i]
    return result


# threshold of points flagged as too steep change
STEEP_COUNT = 9

# for each hour of day, number of points flagged steep is counted,
# this is the threshold ratio between 2nd largest to 1st largest counts
RATIO_2ND_1ST = 0.75


def mytest_slope(data, timestamps, years, months, days, hours, minutes,
                 resolution='hh', label='', show=False):
    data_1deriv = derivative(data / numpy.nanmax(data))
    data_2deriv = derivative(data_1deriv)

    dmask = (data != data)  # all false
    dmask[:-2] = numpy.absolute(data_2deriv) > 0.3  # find steep slope changes
    data_masked = numpy.copy(data)
    data_masked[~dmask] = numpy.NaN

    if resolution == 'hh':
        keys = ['0000', '0030', '0100', '0130', '0200', '0230',
                '0300', '0330', '0400', '0430', '0500', '0530',
                '0600', '0630', '0700', '0730', '0800', '0830',
                '0900', '0930', '1000', '1030', '1100', '1130',
                '1200', '1230', '1300', '1330', '1400', '1430',
                '1500', '1530', '1600', '1630', '1700', '1730',
                '1800', '1830', '1900', '1930', '2000', '2030',
                '2100', '2130', '2200', '2230', '2300', '2330', ]
        times = {k: 0 for k in keys}
    else:
        keys = ['0000', '0100', '0200', '0300', '0400', '0500',
                '0600', '0700', '0800', '0900', '1000', '1100',
                '1200', '1300', '1400', '1500', '1600', '1700',
                '1800', '1900', '2000', '2100', '2200', '2300', ]
        times = {k: 0 for k in keys}

    idx_flagged = numpy.where(dmask)[0]
    for idx in idx_flagged:
        h, m = hours[idx], minutes[idx]
        hm = str(h).zfill(2) + str(m).zfill(2)
        times[hm] += 1

    times_keys = sorted(keys)
    times_values = [times[k] for k in times_keys]

    idx_max = numpy.argmax(times_values)
    times_keys2nd = times_keys[:]
    times_values2nd = times_values[:]
    times_keys2nd.pop(idx_max)
    times_values2nd.pop(idx_max)
    idx_max2nd = numpy.argmax(times_values2nd)
    if times_values[idx_max] == 0:
        ratio_2nd_1st = 1
    else:
        ratio_2nd_1st = (float(times_values2nd[idx_max2nd]) /
                         float(times_values[idx_max]))
    steep_count = STEEP_COUNT
    if (times_values[idx_max] >= steep_count) and \
       (ratio_2nd_1st <= RATIO_2ND_1ST):
        msg = 'Var {v}: potential shadow at {h} '
        msg += '({n} occurrences at {h}, 2nd-to-1st ratio: {r}, '
        msg += 'steep count: {c})'
        log.error(msg.format(h=times_keys[idx_max],
                             n=times_values[idx_max],
                             r=ratio_2nd_1st,
                             v=label,
                             c=steep_count))
    else:
        msg = 'Var {v}: no potential shadow detected '
        msg += '({n} occurrences at {h}, 2nd-to-1st ratio: '
        msg += '{r}, steep count: {c})'
        log.info(msg.format(h=times_keys[idx_max],
                            n=times_values[idx_max],
                            r=ratio_2nd_1st,
                            v=label,
                            c=steep_count))

    if show:
        pyplot.close('all')
        figure = pyplot.figure()
        figure.set_figwidth(20)
        figure.set_figheight(10)

        gs = gridspec.GridSpec(1, 1)
        gs.update(left=0.06, right=0.96, hspace=0.08, top=0.98, bottom=0.08)

        ax = pyplot.subplot(gs[0, 0])
        ax.plot_date(timestamps, data, linewidth=1.0, linestyle='-',
                     marker='', markersize=3, color='#8080ff',
                     markeredgecolor='#8080ff', alpha=1.0,
                     label=label)
        ax.plot_date(timestamps, data_masked, linewidth=1.0, linestyle='',
                     marker='o', markersize=6, color='red',
                     markeredgecolor='red', alpha=1.0, label=label)

        log.info("plot: showing {s}".format(s=label))
        pyplot.show()


RAD_VARS = ("PPFD_IN", "SW_IN")


class Shadows(object):
    """
    Find potential shadowed radiation sensors
    """

    def driver(self, data, resolution):
        data = data.get_data()
        data_o = data
        timestamp_start_o = [datetime.strptime(i.decode('ascii'), "%Y%m%d%H%M")
                             for i in data['TIMESTAMP_START']]

        years_o = numpy.array([t.year for t in timestamp_start_o])
        months_o = numpy.array([t.month for t in timestamp_start_o])
        days_o = numpy.array([t.day for t in timestamp_start_o])
        hours_o = numpy.array([t.hour for t in timestamp_start_o])
        minutes_o = numpy.array([t.minute for t in timestamp_start_o])

        first_year, last_year = years_o[0], years_o[-1]
        log.debug("first year: {fy}, last year: {ly}".format(fy=first_year,
                                                             ly=last_year))
        for y in range(first_year, last_year + 1):
            for m in range(1, 12 + 1):
                msg = "radiation: checking {m} {y}"
                log.info(msg.format(m=MONTH_LABELS[m], y=y))
                mask = (years_o == y) & (months_o == m)
                idx_first = numpy.where(mask)[0][0]
                idx_last = numpy.where(mask)[0][-1]

                data = data_o[mask]
                timestamp_start = timestamp_start_o[idx_first:idx_last + 1]
                years = years_o[mask]
                months = months_o[mask]
                days = days_o[mask]
                hours = hours_o[mask]
                minutes = minutes_o[mask]

                for d in data.dtype.names[2:]:
                    if d in RAD_VARS:
                        mytest_slope(data[d],
                                     timestamps=timestamp_start,
                                     years=years,
                                     months=months,
                                     days=days,
                                     hours=hours,
                                     minutes=minutes,
                                     resolution=resolution,
                                     label=d,
                                     show=False)
        return [status.StatusGenerator().composite_status_generator(log,
                                                                    'shadows')]


if __name__ == '__main__':
    sys.exit("Not to be run directly")
