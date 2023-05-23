import ast
from configparser import ConfigParser
import csv
import datetime
from db_handler import DBConfig, DBHandler, NewDBHandler
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from logger import Logger
from pathlib import Path
from process_actions import ProcessActions
from process_states import ProcessStates
from report_status import ReportStatus
from utils import FileUtil
from utils import TimestampUtil
from amf_utils.flux_vars.utils import VarUtils

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'

_log_name_prefix = 'GenBASEBADM'
_log = Logger(True, None, None, _log_name_prefix).getLogger(_log_name_prefix)


class BASECreator():
    def __init__(self):
        self._cwd = Path.cwd()
        self.db_conn_pool = {}
        self.new_db_handler = NewDBHandler()
        self.init_status = self._get_params_from_config()
        self.BASE_fname_fmt = 'AMF_{sid}_BASE_{res}_{ver}.csv'
        self.file_util = FileUtil()
        self.ts_util = TimestampUtil()
        self.fnv = FileNameVerifier()
        self.report_status = ReportStatus()
        self.process_actions = ProcessActions()
        self.process_states = ProcessStates()

        fp_var_names = FPVariables().get_fp_vars_dict().keys()
        self.var_util = VarUtils(fp_var_names)

        # Initialize logger
        _log.info('Initialized')

    def __del__(self):
        for conn in self.db_conn_pool.values():
            conn.close()

    def _get_params_from_config(self):
        with open(self._cwd / 'qaqc.cfg') as cfg:
            code_ver, code_major_ver, combined_files_loc, path, \
                flux_hostname, flux_user, flux_auth, flux_db_name, PI_vars, \
                new_db_config = self._read_config(cfg)
        if not code_ver:
            print('No code version specified in config file')
            return False
        else:
            self.code_ver = code_ver
            _log.info(f'QAQC code version = {code_ver}')
        if not code_major_ver:
            print('No code major version specified in config file')
            return False
        else:
            self.code_major_ver = code_major_ver
        if not combined_files_loc:
            print('No path for Phase II combined files in config file')
            return False
        else:
            self.combined_files_loc = combined_files_loc
        if not path:
            print('No path for Phase III specified in config file')
            return False
        else:
            path.mkdir(parents=True, exist_ok=True)
            self.path = path

        if not all((flux_user, flux_auth, flux_db_name)):
            print('FLUX DB configurations not assigned')
            return False
        else:
            self.flux_db_handler = DBHandler(
                flux_hostname, flux_user, flux_auth, flux_db_name)
        if not new_db_config:
            _log.error('New Postgres DB configurations not assigned')
            return False
        else:
            self.db_conn_pool['psql_conn'] = self.new_db_handler.init_db_conn(
                new_db_config)

        if not PI_vars:
            print('No _PI variables specified in config file')
        self.PI_vars = tuple(PI_vars)  # PI_vars is [] if unspecified in config
        return True

    def _read_config(self, cfg):
        code_ver = code_major_ver = combined_files_loc = path = None
        flux_hostname = flux_user = flux_auth = flux_db_name = None
        new_hostname = new_user = new_auth = new_db_name = None
        PI_vars = []
        config = ConfigParser()
        config.read_file(cfg)
        cfg_section = 'VERSION'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'code_version'):
                code_ver = config.get(
                    cfg_section, 'code_version')
            if config.has_option(cfg_section, 'code_major_version'):
                code_major_ver = config.get(
                    cfg_section, 'code_major_version')

        cfg_section = 'PHASE_II'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'combined_file_dir'):
                combined_files_loc = config.get(
                    cfg_section, 'combined_file_dir')

        cfg_section = 'PHASE_III'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'output_dir'):
                phase_III_output = config.get(cfg_section, 'output_dir')
                phase_III_output_path = Path(phase_III_output)
                if phase_III_output_path.is_absolute():
                    path = phase_III_output_path
                else:
                    path = self._cwd / phase_III_output
            if config.has_option(cfg_section, 'PI_vars'):
                try:
                    PI_vars = ast.literal_eval(
                        config.get(cfg_section, 'PI_vars'))
                except Exception:
                    PI_vars = []

        cfg_section = 'DB'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'flux_hostname'):
                flux_hostname = config.get(cfg_section, 'flux_hostname')
            if config.has_option(cfg_section, 'flux_user'):
                flux_user = config.get(cfg_section, 'flux_user')
            if config.has_option(cfg_section, 'flux_auth'):
                flux_auth = config.get(cfg_section, 'flux_auth')
            if config.has_option(cfg_section, 'flux_db_name'):
                flux_db_name = config.get(cfg_section, 'flux_db_name')
            if config.has_option(cfg_section, 'new_hostname'):
                new_hostname = config.get(cfg_section, 'new_hostname')
            if config.has_option(cfg_section, 'new_user'):
                new_user = config.get(cfg_section, 'new_user')
            if config.has_option(cfg_section, 'new_auth'):
                new_auth = config.get(cfg_section, 'new_auth')
            if config.has_option(cfg_section, 'new_db_name'):
                new_db_name = config.get(cfg_section, 'new_db_name')
        new_db_config = DBConfig(new_hostname, new_user, new_auth, new_db_name)

        return (code_ver, code_major_ver, combined_files_loc, path,
                flux_hostname, flux_user, flux_auth, flux_db_name, PI_vars,
                new_db_config)

    def create_BASE(self, fp_in, base, site_id, base_version):
        write_as_is = False
        r = csv.reader(fp_in)
        w = csv.writer(base)
        w.writerow([f'# Site: {site_id}'])
        w.writerow([f'# Version: {base_version}'])
        for row in r:
            if write_as_is:
                w.writerow(row)
                continue
            new_row = []
            for fp_in_var in row:
                fp_var = self.var_util.tag_PI_for_BASE_var(
                    fp_in_var, PI_vars=self.PI_vars)
                if fp_var is None:
                    raise Exception(f'{site_id} has invalid FP-In '
                                    f'variable {fp_in_var}')
                new_row.append(fp_var)
            w.writerow(new_row)
            write_as_is = True

    def get_BASE_attrs(self, file_path):
        self.fnv.driver(file_path)
        site_id = self.fnv.fname_attrs.get('site_id')
        resolution = self.fnv.fname_attrs.get('resolution')
        start_time = self.fnv.fname_attrs.get('ts_start')
        end_time = self.fnv.fname_attrs.get('ts_end')
        start_dt = self.ts_util.cast_as_datetime(start_time)
        end_dt = self.ts_util.cast_as_datetime(end_time)
        start_year = start_dt.year
        end_year = end_dt.year
        if end_dt.day == 1 and end_dt.month == 1:
            end_year -= 1
        base_path = self.path / self.BASE_fname_fmt.format(
            sid=site_id, res=resolution,
            ver='version')

        return site_id, base_path, start_year, end_year, resolution

    def get_last_base_version(self, resolution, last_base_version,
                              last_cdiac_HH, last_cdiac_HR):
        is_last_ver_cdiac = False
        if not last_base_version:
            if resolution == 'HH':
                last_base_version = last_cdiac_HH
            elif resolution == 'HR':
                last_base_version = last_cdiac_HR
            if last_base_version:
                is_last_ver_cdiac = True
            elif last_cdiac_HR or last_cdiac_HH:
                last_ver = last_cdiac_HH
                if not last_ver:
                    last_ver = last_cdiac_HR
                error_msg = f'CDIAC version {last_ver} did not have the ' \
                            f'corresponding resolution. Check it out.'
                _log.error(error_msg)
                raise Exception(error_msg)
        return last_base_version, is_last_ver_cdiac

    def assign_new_data_version(self, resolution, last_base_version,
                                last_processID, is_last_ver_cdiac,
                                site_id, md5sum, processID=None):
        if last_base_version:
            if not is_last_ver_cdiac:
                last_pid_inputs = self.flux_db_handler.get_input_files(
                    last_processID)
                pid_inputs = self.flux_db_handler.get_input_files(processID)
                _log.debug(f'{processID}: {pid_inputs}')
                _log.debug(f'{last_processID}: {last_pid_inputs}')
                if pid_inputs ^ last_pid_inputs:
                    _log.info(f'Last version inputs {last_pid_inputs} are not '
                              f'the same as new inputs {pid_inputs}')
                else:
                    _log.info(f'Last version inputs {last_pid_inputs} are '
                              f'the same as new inputs {pid_inputs}')
            _ver_chunks = last_base_version.split('-')
            last_base_filename = self.BASE_fname_fmt.format(
                sid=site_id, res=resolution, ver=last_base_version)
            last_checksum = self.filename_checksum_lookup.get(
                last_base_filename)
            if last_checksum == md5sum:
                new_ver_chunks = (_ver_chunks[0], self.code_major_ver)
            else:
                new_ver_chunks = (
                    str(int(_ver_chunks[0]) + 1), self.code_major_ver)
            new_version = '-'.join(new_ver_chunks)
        else:
            new_version = '1-' + str(self.code_major_ver)
        _log.info(f'Proposed new BASE version is {new_version}')
        if last_base_version == new_version:
            error_msg = f'Last BASE version ({last_base_version}) is the ' \
                        f'same as the new version ({new_version}).'
            _log.error(error_msg)
            raise Exception(error_msg)
        return new_version

    def candidate_iterator(self, site_id, attr):
        f_path = attr.get('full_path')
        base_path = attr.get('base_path')
        res = attr.get('res')
        file_attr = []
        process_id = self.preBASE_files.get(str(f_path))
        try:
            _log.info(f'Creating BASE files for candidate: {f_path.name}')
            last_base_version, last_processID = \
                self.site_list.get(site_id, (None, None))
            last_cdiac_HH, last_cdiac_HR = self.historic_site_list.get(
                site_id, (None, None))
            last_base_version, is_last_ver_cdiac = self.get_last_base_version(
                resolution=res, last_base_version=last_base_version,
                last_cdiac_HH=last_cdiac_HH, last_cdiac_HR=last_cdiac_HR)
            with open(f_path, 'r') as fp_in, open(base_path, 'w') as base:
                self.create_BASE(fp_in, base, site_id, last_base_version)
            md5sum = self.file_util.get_md5(base_path)
            base_path.unlink()
            new_base_version = self.assign_new_data_version(
                resolution=res, last_base_version=last_base_version,
                last_processID=last_processID,
                is_last_ver_cdiac=is_last_ver_cdiac, site_id=site_id,
                md5sum=md5sum, processID=process_id)
            new_base_path = Path(str(base_path)
                                 .replace('version', new_base_version))
            with open(f_path, 'r') as fp_in, open(new_base_path, 'w') as base:
                self.create_BASE(fp_in=fp_in, base=base, site_id=site_id,
                                 base_version=new_base_version)
            new_md5sum = self.file_util.get_md5(new_base_path)
            size = new_base_path.stat().st_size
            base_fname = new_base_path.name
            timestamp = datetime.datetime.now()
            file_attr = [size, new_md5sum, base_fname, timestamp,
                         new_base_path, new_base_version]
            self.report_status.enter_new_state(
                process_id=process_id,
                action=self.process_actions.GeneratedBASE,
                status=self.process_states.GeneratedBASE)
        except Exception as e:
            error_msg = f'Failed to create base file with error {e}'
            _log.error(error_msg)
            self.report_status.enter_new_state(
                process_id=process_id,
                action=self.process_actions.BASEGenFailed,
                status=self.process_states.BASEGenFailed)
        return file_attr

    def driver(self):
        _log.info(f'Pipeline has version: {self.code_ver}')
        entry_ls = []
        new_entry_ls = []
        base_attrs = {}
        psql_conn = self.db_conn_pool.get('psql_conn')
        self.site_list = self.flux_db_handler.get_sites_with_updates()
        self.historic_site_list = self.new_db_handler.get_sites_with_updates(
            psql_conn, is_historic=True)
        self.preBASE_files = self.flux_db_handler.get_BASE_candidates()

        attr_keys = ['base_path', 'start_year', 'end_year', 'res']
        file_attr_keys = ['size', 'md5sum', 'base_fname', 'timestamp',
                          'base_path', 'ver']

        self.filename_checksum_lookup = \
            self.new_db_handler.get_filename_checksum_lookup(psql_conn)

        for f, pid in self.preBASE_files.items():
            f_path = Path(f)
            base_candidate_name = f_path.name
            _log.info(f'BASE candidate: {base_candidate_name}')
            file_attrs = self.get_BASE_attrs(f)
            site_id = file_attrs[0]
            cur_file_res = file_attrs[-1]
            site_id_attrs = base_attrs.get(site_id)

            if site_id_attrs:
                info_msg = f'Site {site_id} was processed earlier.'
                _log.info(info_msg)
                has_same_res = False
                for attr in site_id_attrs:
                    res = attr.get('res')
                    if res == cur_file_res:
                        has_same_res = True
                        break
                if has_same_res:
                    error_msg = (f'More than one candidate file for {site_id}'
                                 f'has same {res} resolution')
                    _log.error(error_msg)
                    return

            _tmp = {}
            for k, v in zip(attr_keys, file_attrs[1:]):
                _tmp[k] = v
            _tmp['base_candidate_name'] = base_candidate_name
            _tmp['full_path'] = f_path
            if site_id_attrs:
                site_id_attrs.append(_tmp)
                base_attrs[site_id] = site_id_attrs
            else:
                base_attrs[site_id] = [_tmp]

        for site_id, attr_ls in base_attrs.items():
            for attr in attr_ls:
                file_attr_vals = self.candidate_iterator(site_id, attr)
                for k, v in zip(file_attr_keys, file_attr_vals):
                    attr[k] = v
                entry = (attr.get('base_fname'),
                         attr.get('start_year'),
                         attr.get('end_year'),
                         attr.get('size'),
                         attr.get('md5sum'),
                         attr.get('base_path'),
                         attr.get('timestamp'),
                         None, None)
                entry_ls.append(entry)
                new_entry_ls.append(entry[:-2])

        status = None
        try:
            self.new_db_handler.insert_BASE_entries(
                self.db_conn_pool.get('psql_conn'), new_entry_ls)
        except Exception as e:
            _log.warning(f'New Postgres logging has exception {e}')

        if status:
            _log.error('DB inserts with errors')
        else:
            _log.info('DB inserts successful')
        _log.debug(base_attrs)

        return base_attrs


if __name__ == '__main__':
    b = BASECreator()
    if not b.init_status:
        print('Unsuccessful initialization. Terminating.')
    else:
        b.driver()
