import os
import datetime
import subprocess

from configparser import ConfigParser
from db_handler import DBConfig, DBHandler, NewDBHandler
from file_name_verifier import FileNameVerifier
from logger import Logger
from process_states import ProcessStates, ProcessStateHandler
from report_status import ReportStatus
from utils import FileUtil
from utils import RemoteSSHUtil
from utils import TimestampUtil
from utils import ZipUtil

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'

_log = Logger(True, None, None, 'GenBASEBADM').getLogger(
    'GenBASEBADM')

NETWORK = 'AMF'


class UpdateBASEBADM():
    def __init__(self):
        self._cwd = os.getcwd()
        self.db_conn_pool = {}
        self.new_db_handler = NewDBHandler()
        self.init_status = self._get_params_from_config()
        self.ts_util = TimestampUtil()
        self.fnv = FileNameVerifier()
        self.file_util = FileUtil()
        self.zip_util = ZipUtil()
        self.report_status = ReportStatus()
        self.process_states = ProcessStateHandler()
        self.remote_ssh_util = RemoteSSHUtil(_log)

        # Initialize logger
        _log.info('Initialized')

    def __del__(self):
        for conn in self.db_conn_pool.values():
            conn.close()

    def _read_config(self, cfg):
        combined_files_loc = None
        BASE_BADM_path = None
        BADM_mnt = None
        OLD_BASE_mnt = None
        BADM_exe_dir = None

        new_db_config = None
        config = ConfigParser()
        config.read_file(cfg)

        cfg_section = 'PHASE_III'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'output_dir'):
                output = config.get(cfg_section, 'output_dir')
                if os.path.abspath(output):
                    BASE_BADM_path = output
                else:
                    BASE_BADM_path = os.path.join(self._cwd, output)
            if config.has_option(cfg_section, 'badm_mnt'):
                BADM_mnt = config.get(cfg_section, 'badm_mnt')
            if config.has_option(cfg_section, 'old_base_mnt'):
                OLD_BASE_mnt = config.get(cfg_section, 'old_base_mnt')
            if config.has_option(cfg_section, 'badm_exe_dir'):
                BADM_exe_dir = config.get(cfg_section, 'badm_exe_dir')

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

        return (combined_files_loc, BASE_BADM_path, BADM_mnt, OLD_BASE_mnt,
                BADM_exe_dir, flux_hostname, flux_user, flux_auth,
                flux_db_name, new_db_config)

    def _get_params_from_config(self):
        with open(os.path.join(self._cwd, 'qaqc.cfg')) as cfg:
            combined_files_loc, BASE_BADM_path, BADM_mnt, OLD_BASE_mnt, \
                BADM_exe_dir, flux_hostname, flux_user, flux_auth, \
                flux_db_name, new_db_config = self._read_config(cfg)
        if not BASE_BADM_path:
            _log.error('No path for Phase III specified in config file')
            return False
        elif not os.path.exists(BASE_BADM_path):
            os.makedirs(BASE_BADM_path)
            self.BASE_BADM_path = BASE_BADM_path
        else:
            self.BASE_BADM_path = BASE_BADM_path
        if not BADM_mnt:
            _log.error('BADM mount directory not specified in config file')
            return False
        else:
            self.BADM_mnt = BADM_mnt
        if not OLD_BASE_mnt:
            _log.error('OLD BADM mount directory not specified in config file')
            return False
        else:
            self.OLD_BASE_mnt = OLD_BASE_mnt
        if not BADM_exe_dir:
            _log.error('BADM exe directory not specified in config file')
        else:
            self.BADM_exe_dir = BADM_exe_dir

        if not new_db_config:
            _log.error('New Postgres DB configurations not assigned')
            return False
        else:
            self.db_conn_pool['psql_conn'] = self.new_db_handler.init_db_conn(
                new_db_config)

        return True

    def lookup_prev_BASE_ver(self, site_id, ver):
        prev_BASE = set()

        new_base_files = [f for f in os.listdir(self.BASE_BADM_path)
                          if not f.endswith(self.zip_util.ZIP_EXT)]

        for f in new_base_files:
            HH_ver = HR_ver = None
            base_name = base_path = None

            if ver in f and site_id in f:
                base_name = os.path.basename(f)
                base_path = os.path.join(self.BASE_BADM_path, f)
                if 'HH' in base_name:
                    HH_ver = ver
                elif 'HR' in base_name:
                    HR_ver = ver
                else:
                    _log.error('No HH or HR version detected.')
                prev_BASE.add((HH_ver, HR_ver, base_name, base_path))

        if prev_BASE:
            return prev_BASE

        old_base_files = [f for f in os.listdir(self.OLD_BASE_mnt)
                          if not f.endswith(self.zip_util.ZIP_EXT)]

        for f in old_base_files:
            HH_ver = HR_ver = None
            base_name = base_path = None

            if ver in f and site_id in f:
                base_name = os.path.basename(f)
                base_path = os.path.join(self.OLD_BASE_mnt, f)
                if 'HH' in base_name:
                    HH_ver = ver
                elif 'HR' in base_name:
                    HR_ver = ver
                else:
                    _log.error('No HH or HR version detected.')
                prev_BASE.add((HH_ver, HR_ver, base_name, base_path))

        return prev_BASE

    def _get_newer_ver(self, ver1, ver2):
        ver1_split = ver1.split('-')
        ver2_split = ver2.split('-')
        ver1_data_ver = int(ver1_split[0])
        ver2_data_ver = int(ver2_split[0])
        if ver1_data_ver > ver2_data_ver:
            return ver1
        elif ver1_data_ver == ver2_data_ver:
            ver1_code_ver = int(ver1_split[-1])
            ver2_code_ver = int(ver2_split[-1])
            if ver1_code_ver > ver2_code_ver:
                return ver1
            else:
                return ver2
        else:
            return ver2

    def driver(self, base_attrs, post_base_only=None):
        psql_conn = self.db_conn_pool.get('psql_conn')
        if post_base_only:
            status = self.remote_ssh_util.update_base_badm('post_base')
            if not status:
                _log.error(
                    'Update postBASE failed with errors, '
                    'cannot proceed with update, exiting.')
                return
            return

        if not self.remote_ssh_util.update_base_badm('badm_update'):
            _log.error(
                'Error updating BADM update tables, cannot proceed, exiting.')
            return

        try:
            _log.info('Generating BIFs')
            bif_gen_path = os.path.join(
                self.BADM_exe_dir, 'BIF_generator.py')
            _log.debug(f'BIF gen path: {bif_gen_path}')
            status = subprocess.call(
                ['python', bif_gen_path, '-amf', '-s', '-p'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        except Exception as e:
            _log.error('Error triggering BIF generation.')
            _log.info(f'{e}')
        if status != 0:
            _log.error('BIF generation failed, exiting.')
            return

        if not self.remote_ssh_util.update_base_badm('post_badm'):
            return

        expected_zips = self.new_db_handler.get_base_badm_update_zip_count(
            psql_conn)
        badm_map = self.new_db_handler.get_badm_map(psql_conn)
        sites_needing_updates = self.new_db_handler.get_sites_with_updates(
            psql_conn)
        base_candidate_map = self.flux_db_handler.get_BASE_candidates(
            state_ids=self.process_states.base_candidate_states)

        # Create zip file
        sites_processed = set()
        n_created_zips = 0
        entry_ls = []
        new_entry_ls = []
        base_name_ls = []
        process_ids_ls = []

        for site_id in sites_needing_updates.keys():
            HH_ver = None
            HR_ver = None
            files_to_zip = []
            base_candidates_ls = []
            process_id_ls = []
            process_id_fail = False

            # Handle BASE files
            site_attrs_ls = base_attrs.get(site_id)
            has_new_BASE = False
            if site_attrs_ls:
                has_new_BASE = True
                for site_attrs in site_attrs_ls:
                    base_name = site_attrs.get('base_fname')
                    base_candidate_name = site_attrs.get('base_candidate_name')
                    files_to_zip.append(site_attrs.get('base_path'))
                    base_candidates_ls.append(base_candidate_name)
                    full_path_base_name = site_attrs.get('full_path')
                    # get processID for updated site-res flux data combo.
                    # NOTE: only updated flux data will look for processID.
                    #       So report_status updates will only occur for these
                    #       updated site-res flux data. I.e., if the flux data
                    #       is not being update, no processID and no
                    #       report_status update
                    if not base_candidate_map.get(full_path_base_name):
                        _log.error('Cannot find corresponding process ID '
                                   f'for full file name {full_path_base_name}')
                        process_id_fail = True
                    process_id_ls.append(
                        base_candidate_map.get(full_path_base_name))
                    _ver = site_attrs.get('ver')
                    if 'HH' in base_name:
                        HH_ver = _ver
                    elif 'HR' in base_name:
                        HR_ver = _ver
                    else:
                        _log.error('No HH or HR version detected.')

            if process_id_fail:
                _log.error('Problem finding corresponding process id for '
                           f'{site_id}. Skipping site.')
                continue

            _HH_ver, _HR_ver = sites_needing_updates.get(site_id)
            prev_BASE = set()
            if _HH_ver:
                prev_BASE.update(self.lookup_prev_BASE_ver(site_id, _HH_ver))
            if _HR_ver:
                prev_BASE.update(self.lookup_prev_BASE_ver(site_id, _HR_ver))

            for f in prev_BASE:
                prev_HH_ver, prev_HR_ver, base_name, base_path = f
                has_candidate = False
                if has_new_BASE:
                    # YWC: For ones with new BASE, check if historical
                    # 'other' version exists: HR/HH
                    # (US-UMB with both HH and HR comes to mind)

                    if None not in (HH_ver, prev_HR_ver):
                        HR_ver = prev_HR_ver
                        has_candidate = True
                    elif None not in (HR_ver, prev_HH_ver):
                        HH_ver = prev_HH_ver
                        has_candidate = True
                    else:
                        pass
                else:
                    if HH_ver is None and prev_HH_ver is not None:
                        HH_ver = prev_HH_ver
                        has_candidate = True
                    elif HR_ver is None and prev_HR_ver is not None:
                        HR_ver = prev_HR_ver
                        has_candidate = True
                    elif HH_ver is not None and prev_HH_ver is not None:
                        new_HH_ver = self._get_newer_ver(HH_ver, prev_HH_ver)
                        if new_HH_ver == HH_ver:
                            continue
                        del_ver = '_HH_' + HH_ver
                        HH_ver = new_HH_ver
                        files_to_zip.remove(
                            [f for f in files_to_zip if del_ver in f][0])
                        has_candidate = True
                    elif HR_ver is not None and prev_HR_ver is not None:
                        new_HR_ver = self._get_newer_ver(HR_ver, prev_HR_ver)
                        if new_HR_ver == HR_ver:
                            continue
                        del_ver = '_HR_' + HR_ver
                        HR_ver = new_HR_ver
                        files_to_zip.remove(
                            [f for f in files_to_zip if del_ver in f][0])
                        has_candidate = True

                if has_candidate:
                    files_to_zip.append(base_path)
                    base_candidates_ls.append(base_name)
                    process_id_ls.append(None)
                    if has_new_BASE:
                        break

            if HH_ver and HR_ver:
                zip_ver = self._get_newer_ver(HH_ver, HR_ver)
            else:
                zip_ver = HR_ver if HR_ver else HH_ver

            # Handle BADM files
            badm_filename, badm_version = badm_map.get(site_id)
            if not badm_filename:
                _log.warning(f'No BADM for site {site_id}')
                for process_id, filename in zip(process_id_ls,
                                                base_candidates_ls):
                    if not process_id:
                        _log.info(f'No processID for file: {filename}. '
                                  'Skipping report_status.')
                        continue
                    try:
                        self.report_status.enter_new_state(
                            process_id=process_id,
                            state_id=self.process_states.get_process_state(
                                ProcessStates.BADMUpdateFailed))
                        info_msg = ('Wrote report_status BADMUpdateFailed'
                                    f'for processID {process_id} '
                                    f'(file: {filename}).')
                        _log.info(info_msg)
                    except Exception as e:
                        info_msg = ('Failed report_status BADMUpdateFailed '
                                    f'for processID {process_id} '
                                    f'(file: {filename}) with error {e}')
                        _log.warning(info_msg)
                continue
            else:
                badm_filepath = f'{self.BADM_mnt}/{badm_filename}'
                is_BADM_exist = os.path.exists(badm_filepath)
                _log.debug(f'BADM file path: {badm_filepath}')
                _log.debug(f'BADM file path exists?: {is_BADM_exist}')
                files_to_zip.append(badm_filepath)

            if files_to_zip:
                if len(files_to_zip) <= 1:
                    _log.warning('Number of files to zip seems insufficient.')
                    _log.warning(files_to_zip)
                    continue
                product_name = 'BASE-BADM'
                ext = self.zip_util.ZIP_EXT
                zip_name = f'{NETWORK}_{site_id}_{product_name}_{zip_ver}{ext}'
                zip_path = os.path.join(self.BASE_BADM_path, zip_name)
                _log.info(files_to_zip)
                # Zip files
                # We can safely assume that we have more than one file
                self.zip_util.zip_file(files_to_zip[0], zip_path)
                for f in files_to_zip[1:]:
                    self.zip_util.zip_file(f, zip_path, 'a')
                createDate = datetime.datetime.now()
                size = os.path.getsize(zip_path)
                md5sum = self.file_util.get_md5(zip_path)
                sites_processed.add(site_id)
                n_created_zips += 1

                # hardcode for now
                entry = (zip_name, HH_ver, HR_ver, badm_version, 'BIF*',
                         size, md5sum, zip_path, createDate, None, None)
                entry_ls.append(entry)
                new_entry = (zip_name, HH_ver, HR_ver, badm_version,
                             size, md5sum, zip_path, createDate)
                new_entry_ls.append(new_entry)
                base_name_ls.append(base_candidates_ls)
                process_ids_ls.append(process_id_ls)
                for process_id, filename in zip(process_id_ls,
                                                base_candidates_ls):
                    if not process_id:
                        _log.info(f'No processID for file: {filename}. '
                                  'Skipping report_status.')
                        continue
                    try:
                        self.report_status.enter_new_state(
                            process_id=process_id,
                            state_id=self.process_states.base_candidate_states(
                                ProcessStates.UpdatedBASEBADM))
                        info_msg = ('Wrote report_status UpdatedBASEBADM '
                                    f'for processID {process_id} '
                                    f'(file: {filename}).')
                        _log.info(info_msg)
                    except Exception as e:
                        info_msg = ('Failed report_status UpdatedBASEBADM '
                                    f'for processID {process_id} '
                                    f'(file: {filename}) with error {e}')
                        _log.warning(info_msg)

        number_of_sites = len(sites_processed)
        _log.info(f'Number of sites processed {number_of_sites}')
        if n_created_zips != expected_zips:
            _log.warning(f'Expected {expected_zips} zips, '
                         f'but {n_created_zips} created.')

        # Insert information about zip into log table and mark filelist
        # as processed.
        try:
            self.new_db_handler.insert_BASE_BADM_entries(
                self.db_conn_pool.get('psql_conn'), new_entry_ls)
        except Exception as e:
            _log.warning(f'New Postgres logging has exception {e}')

        # Create filelist
        file_ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        filelist_name = f'base_badm_zips_{file_ts}.flist'
        with open(os.path.join(self.BASE_BADM_path, filelist_name), 'w') as fl:
            for e, b, p in zip(entry_ls, base_name_ls, process_ids_ls):
                timestamp = e[-3].strftime('%Y%m%d%H%M%S%f')
                zip_path = e[-4]
                ver = []
                for base_file in b:
                    if '_HH_' in base_file:
                        ver.append(e[1])
                    elif '_HR_' in base_file:
                        ver.append(e[2])
                    else:
                        ver.append('unk')
                        _log.warning(f'BASE file {base_file} name does not '
                                     'have recognizable resolution. Expect '
                                     'errors in database.')
                file_entry = f'{timestamp}\t{zip_path}\t{b}\t{p}\t{ver}\n'
                fl.write(file_entry)

        # Update BADM DB after generation of BASE-BADM zips
        if not self.remote_ssh_util.update_base_badm('base_badm_update'):
            _log.error(
                'Updating BADM DB after BASE-BADM zip generation '
                'contains errors, cannot proceed with update, exiting.')
            return


if __name__ == '__main__':
    u = UpdateBASEBADM()
    if not u.init_status:
        print('Unsuccessful initialization. Terminating.')
    else:
        u.driver(None)
