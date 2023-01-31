import json
import numpy as np
import numpy.lib.recfunctions as nlr
import os
import shutil
import subprocess

from configparser import ConfigParser
from data_reader import DataReader
from datetime import datetime as dt
from logger import Logger
from path_util import PathUtil
from utils import Decode, SysUtil, TimestampUtil, WSUtil


__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'

_log = Logger().getLogger(__name__)


class SW_IN_POT_Generator():

    HOURLY_OPTION = '1'
    SW_IN_POT = 'SW_IN_POT'

    def __init__(self):
        """ Initialize class and variables on loading of class """
        self.decoder = Decode()
        self.path_util = PathUtil()
        self.sys_util = SysUtil()
        self.ts_util = TimestampUtil()
        self._platform = self.sys_util.get_platform()
        self.init_status = self._get_params_from_config()
        self.ws_util = WSUtil(_log)

        self.sw_in_pot_data = None
        self.sw_in_pot_dtype = None
        self.file_sw_in_pot_data = None

    def _get_params_from_config(self):
        """ Get required parameters from config files """
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            try:
                self._read_config(cfg)
                return True
            except Exception:
                return False

    def _read_config(self, cfg):
        """ Do the actual reading in of parameters from the config file
        :param cfg: opened config file
        :type cfg: file (Python 2.7x) or io.TextIOWrapper (Python 3)

        :rtype: None.
        :return: None
        """
        config = ConfigParser()
        config.read_file(cfg)
        ws_cfg = 'WEBSERVICES'
        phase_II_cfg = 'PHASE_II'
        if config.has_section(ws_cfg):
            self.site_info_ws = config.get(ws_cfg, 'site_info')
        else:
            err_msg = 'Unable to read properties for Site Info WS'
            _log.error(err_msg)
            raise Exception(err_msg)
        if config.has_section(phase_II_cfg):
            self.orig_base_dir = config.get(phase_II_cfg, 'output_dir')
            self.intermediate_dirname = config.get(
                phase_II_cfg, 'intermediate_dirname')
        else:
            err_msg = 'Unable to read properties for output_dir'
            _log.error(err_msg)
            raise Exception(err_msg)

        sw_in_pot_cfg = self.SW_IN_POT
        if config.has_section(sw_in_pot_cfg):
            linux_exe = config.get(sw_in_pot_cfg, 'linux_path')
            win_exe = config.get(sw_in_pot_cfg, 'win_path')
            mac_exe = config.get(sw_in_pot_cfg, 'mac_path')
        else:
            err_msg = 'Unable to read properites for {}'.format(self.SW_IN_POT)
            _log.error(err_msg)
            raise Exception(err_msg)

        if self._platform == self.sys_util.LINUX:
            self.sw_in_pot_exe = linux_exe
        elif self._platform == self.sys_util.WIN:
            self.sw_in_pot_exe = win_exe
        elif self._platform == self.sys_util.OS_X:
            self.sw_in_pot_exe = mac_exe
        else:
            err_msg = "Unrecognized platform {p} has no executable"
            _log.error(err_msg.format(p=self._platform))
            raise Exception(err_msg)
        if not os.path.exists(self.sw_in_pot_exe):
            err_msg = "Executable for platform has invalid path: {p}"
            _log.error(err_msg.format(p=self.sw_in_pot_exe))
            raise Exception(err_msg)

    def _get_BADM_data(self, json_data, field_name, ts_field_name):
        """ Function to parse out BADM data with date_start construct.
            If multiple values exist for field, try to parse out the
            latest one that is denoted in the timestamp field.

        :param json_data: JSON formatted BADM str
        :type json_data: str

        :param field_name: Name of field to look up from BADM JSON
        :type field_name: str

        :param ts_field_name: Name of timestamp field to look up in BADM JSON
        :type ts_field_name: str

        :rtype: value
        :return Value for field_name
        """
        value = None
        if not json_data:
            return value
        keys = json_data.keys()
        if not keys:
            return value
        if len(keys) == 1:
            value = json_data.get(next(iter(keys))).get(field_name)
            return value
        latest_data_ts = None
        latest_data = None
        for k in keys:
            cur_data = json_data.get(k)
            cur_data_ts = None
            try:
                ts = cur_data.get(ts_field_name)
                ISO_ts = self.ts_util.get_ISO_str_timestamp(ts)
                cur_data_ts = self.ts_util.cast_as_datetime(ISO_ts)
            except Exception:
                warning_msg = ("Has no timestamp to differentiate most "
                               "current value")
                _log.warning(warning_msg)
            if not cur_data_ts:
                continue
            if not latest_data_ts:
                latest_data_ts = cur_data_ts
                latest_data = cur_data
            else:
                if latest_data_ts < cur_data_ts:
                    latest_data_ts = cur_data_ts
                    latest_data = cur_data
        if latest_data:
            value = latest_data.get(field_name)
        return value

    def cleanup_sw_in_file(self, sw_in_pot_fname, site_id, process_id):
        # Move generated sw_in_pot file
        process_path = self.path_util.create_dir_for_run(
            site_id, process_id, self.orig_base_dir)
        sw_in_pot_path = self.path_util.create_valid_path(
            process_path, self.intermediate_dirname)

        _log.debug('Target move dir: {}'.format(sw_in_pot_path))
        _log.debug('Filename to move: {}'.format(sw_in_pot_fname))

        shutil.move(sw_in_pot_fname, sw_in_pot_path)

    def get_UTC_offset(self, utc_data):
        """ Look up UTC offset from BADM data. If not available, assume
            no offset.
        """
        date_start_field = 'UTC_OFFSET_DATE_START'
        offset = self._get_BADM_data(utc_data, 'UTC_OFFSET', date_start_field)
        if not offset:
            _log.warning("No UTC offset specified in BADM")
            offset = str(0)
        return offset

    def get_location(self, loc_data):
        """ Look up latitude and longitude from BADM data.
        """
        date_start_field = 'LOCATION_DATE_START'
        lat = self._get_BADM_data(loc_data, 'LOCATION_LAT', date_start_field)
        lon = self._get_BADM_data(loc_data, 'LOCATION_LONG', date_start_field)
        return lat, lon

    def get_site_attrs(self, site_id):
        """ Get the lat, lon, and UTC_offset for a given site ID
            from BADM information.

        :param site_id: AmeriFlux site ID string
        :type site_id: str

        :rtype: tuple.
        :return tuples of lat, lon and UTC_offset
        """
        BADM_ws = self.site_info_ws + '{}'
        url = BADM_ws.format(site_id)
        content = self.ws_util.get_content(url)
        data = json.loads(content)
        # TODO: Assumes simple case of one lat, lon and utc per site
        #       This is not always true and we need to account for that
        data_values = data.get('values')
        location_data = data_values.get('GRP_LOCATION')
        utc_offset_data = data_values.get('GRP_UTC_OFFSET')
        lat, lon = self.get_location(location_data)
        utc_offset = self.get_UTC_offset(utc_offset_data)
        return lat, lon, utc_offset

    def gen_sw_in_pot_file(self, site_id, data, rez, lat, lon, utc_offset):
        _log.info("Generating SW_IN_POT file for site {}".format(site_id))
        output_fname = "{s}_{rez}".format(s=site_id, rez=rez)
        ts_start = self.input_data['TIMESTAMP_START']
        year_start = self.ts_util.cast_as_datetime(ts_start[0]).year
        year_end = self.ts_util.cast_as_datetime(ts_start[-1]).year
        year_range = '{ys}-{ye}'.format(ys=year_start, ye=year_end)
        cmd = [output_fname, year_range, lat, lon, utc_offset]
        cmd.insert(0, self.sw_in_pot_exe)

        if rez == 'HR':
            cmd.append(self.HOURLY_OPTION)
        _log.info(cmd)

        try:
            output = subprocess.check_output(cmd)
            fname_output_clause = self.decoder.byte_to_str(
                output.strip()).split('\n')[-1]
            output_fname = fname_output_clause.split()[1]
        except subprocess.CalledProcessError as cmd_exc:
            _log.error("Error code: ", cmd_exc.returncode)
            _log.error("Error output:\n", cmd_exc.output)
        except Exception as gen_exc:
            _log.error(gen_exc)

        return output_fname

    def get_sw_in_pot_data_for_timerange(
            self, data, ts_start, ts_end):
        """ Get SW_IN_POT data for corresponding start and end time.

        :param data: Data from DataReader() containing SW_IN_POT data
        :type: str

        :param ts_start: First timestamp in TIMESTAMP_START of original data
        :type: byte str
        :param ts_end: Last timestamp in TIMESTAMP_END of original data
        :type: byte str

        :rtype tuple
        :return sw_in_pot_data, sw_in_pot_dtype
        """
        start_idx = end_idx = None
        for idx, ts in enumerate(
                zip(data['TIMESTAMP_START'], data['TIMESTAMP_END'])):
            _ts_start = self.decoder.byte_to_str(ts[0])
            _ts_end = self.decoder.byte_to_str(ts[-1])
            if _ts_end == ts_end:
                end_idx = idx
                break
            elif _ts_start == ts_start:
                start_idx = idx
        if start_idx is None or end_idx is None:
            return None, None
        sw_in_pot_data = data[
            self.SW_IN_POT][start_idx:end_idx+1]  # +1 for len
        dt_descr = data.dtype.descr
        sw_in_pot_dtype = [dt for dt in dt_descr if dt[0] == self.SW_IN_POT]
        return sw_in_pot_data, sw_in_pot_dtype

    def gen_file_sw_in_pot_data(self, data_reader, site_id,
                                resolution, process_id):

        """ Uses the driver function to read data from a file, and then
            calculates that file's sw_in_pot_data """

        sw_in_pot_fname = self.driver(
                            site_id, data_reader, resolution)

        _temp_d = DataReader()
        _temp_d.driver(sw_in_pot_fname, run_type='o')
        _sw_in_pot_data = _temp_d.get_data()

        # Cleanup steps
        del _temp_d  # Deallocate to free mem
        try:
            self.cleanup_sw_in_file(sw_in_pot_fname, site_id, process_id)
        except Exception:
            _log.error('Unable to cleanup temporary file')

        self.file_sw_in_pot_data = _sw_in_pot_data
        return self.file_sw_in_pot_data

    def gen_rem_sw_in_pot_data(self, data_reader, process_id,
                               resolution, site_id, ts_start, ts_end):

        """ Uses the sw_in_pot_data from the file and calculates the
            remaining sw_in_pot_data for that time period
        """

        if not self.file_sw_in_pot_data:
            self.gen_file_sw_in_pot_data(data_reader, site_id,
                                         resolution, process_id)

        ts_util = TimestampUtil()
        sw_in_gen_end_year = ts_util.cast_as_datetime(
            data_reader.get_data()['TIMESTAMP_START'][-1]).year
        ts_end_dt = ts_util.cast_as_datetime(ts_end)

        rem_sw_in_pot_data = None
        if ts_end_dt.year <= sw_in_gen_end_year:
            rem_ts_start = dt(ts_end_dt.year, ts_end_dt.month, ts_end_dt.day)
            # Get year end date
            rem_ts_end = dt(rem_ts_start.year, dt.max.month, dt.max.day)
            rem_ts_start = rem_ts_start.strftime(ts_util.PREFERRED_TS_FORMAT)
            rem_ts_end = rem_ts_end.strftime(ts_util.PREFERRED_TS_FORMAT)
            rem_sw_in_pot_data, _ = \
                self.get_sw_in_pot_data_for_timerange(
                    self.file_sw_in_pot_data, rem_ts_start, rem_ts_end)

        return rem_sw_in_pot_data

    def merge_data(self, data_reader, site_id, resolution,
                   process_id, ts_start, ts_end):
        # If the sw_in_pot_data from the file hasn't been generated, do so
        if self.file_sw_in_pot_data is None:
            self.gen_file_sw_in_pot_data(data_reader, site_id,
                                         resolution, process_id)

        # If the sw_in_pot_data for the given time range hasn't been generated,
        # do so
        if self.sw_in_pot_data is None or self.sw_in_pot_dtype is None:
            self.sw_in_pot_data, self.sw_in_pot_dtype = \
                self.get_sw_in_pot_data_for_timerange(
                    self.file_sw_in_pot_data, ts_start, ts_end)

        # Merge the given datareader's data with the sw_in_pot_data generated
        return nlr.merge_arrays(
            [data_reader.get_data(), np.ma.asarray(
                self.sw_in_pot_data, dtype=self.sw_in_pot_dtype)],
            asrecarray=True, flatten=True, usemask=True)

    def driver(self, site_id, d, resolution):
        lat, lon, utc_offset = self.get_site_attrs(site_id)
        self.input_data = d.get_data()
        output_fname = self.gen_sw_in_pot_file(
            site_id, self.input_data, resolution, lat, lon, utc_offset)
        return output_fname


if __name__ == "__main__":
    # Testing only
    sw_in_pot_gen = SW_IN_POT_Generator()
    if sw_in_pot_gen.init_status:
        # Do something here
        pass
