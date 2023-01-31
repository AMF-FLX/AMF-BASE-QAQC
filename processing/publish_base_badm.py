import argparse
import ast
import collections
import os
from configparser import ConfigParser
from db_handler import DBHandler
from logger import Logger
from process_actions import ProcessActions
from process_states import ProcessStates
from publish import Publish
from report_status import ReportStatus
from utils import RemoteSSHUtil

__author__ = "You-Wei Cheah"
__email__ = "ycheah@lbl.gov"

_log = Logger(True, None, None, "GenBASEBADM").getLogger(
    "GenBASEBADM")


class PublishBASEBADM():
    def __init__(self):
        self._cwd = os.getcwd()
        self.init_status = self._get_params_from_config()
        self.publisher = Publish()
        self.report_status = ReportStatus()
        self.process_actions = ProcessActions()
        self.process_states = ProcessStates()

        _log.info("Initialized")
        self.remote_ssh_util = RemoteSSHUtil(_log)

    def _read_config(self, cfg):
        BASE_BADM_path = None
        db_hostname = None
        db_user = None
        db_auth = None
        db_name = None
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

        cfg_section = 'DB'
        if config.has_section(cfg_section):
            if config.has_option(cfg_section, 'hostname'):
                db_hostname = config.get(cfg_section, 'hostname')
            if config.has_option(cfg_section, 'user'):
                db_user = config.get(cfg_section, 'user')
            if config.has_option(cfg_section, 'auth'):
                db_auth = config.get(cfg_section, 'auth')
            if config.has_option(cfg_section, 'db_name'):
                db_name = config.get(cfg_section, 'db_name')
        return BASE_BADM_path, BADM_mnt, db_hostname, db_user, db_auth, db_name

    def _get_params_from_config(self):
        with open(os.path.join(self._cwd, 'qaqc.cfg')) as cfg:
            path, BADM_mnt, db_hostname, db_user, db_auth, \
                db_name = self._read_config(cfg)
        if not path:
            _log.error("No path for Phase III specified in config file")
            return False
        elif not os.path.exists(path):
            os.makedirs(path)
            self.path = path
        else:
            self.path = path
        if not BADM_mnt:
            _log.error("BADM mount directory not specified in config file")
            return False
        else:
            self.BADM_mnt = BADM_mnt
        if not all((db_user, db_auth, db_name)):
            _log.error("DB configurations not assigned")
            return False
        else:
            self.db_handler = DBHandler(db_hostname, db_user, db_auth, db_name)
        return True

    def driver(self, args):
        flist_ls = [f for f in os.listdir(self.path) if f.endswith('.flist')]
        if len(flist_ls) > 1:
            _log.warning("More than one flist file found")
            return
        elif not flist_ls:
            _log.info("No flist files found")
            return
        else:
            flist = os.path.join(self.path, flist_ls[0])
        temp = {}
        with open(flist) as f:
            line = f.readline()
            while line:
                _l = line.split('\t')
                line = f.readline()
                if len(_l) == 5:
                    temp[_l[0]] = _l[1:]
                else:
                    _log.warning(f"Line is malformed: {_l}")
        od = collections.OrderedDict(sorted(temp.items()))
        for k, v in od.items():
            zip_path = v[0]
            base_name_ls = v[1]
            process_ids_ls = v[2]
            ver = v[-1].strip()
            if args.test:
                _log.info(f"zip_path = {zip_path}")
                _log.info(f"base_name_ls = {base_name_ls}")
                _log.info(f"process_id_ls = {process_ids_ls}")
                _log.info(f"ver = {ver}")
            try:
                if not args.test:
                    self.publisher.transfer_basebadm(zip_path)
                for process_id, filename, version in \
                        zip(ast.literal_eval(process_ids_ls),
                            ast.literal_eval(base_name_ls),
                            ast.literal_eval(ver)):
                    # NOTE: only site-res combos with updated flux data are
                    #       will have the report_publish_base run b/c
                    #       processIDs were only supplied for these sites.
                    if args.test:
                        info = f"{process_id} : {filename} : {version}"
                        _log.info("info passed to report_publish_base: "
                                  f"{info}")
                    if not process_id:
                        _log.info(f"No processID for file: {filename}. "
                                  "Skipping report_status.")
                        continue
                    try:
                        self.report_status.report_publish_base(
                            process_id=process_id, version=version)
                        info_msg = ("Wrote report_status report_publish_base "
                                    f"for processID {process_id} "
                                    f"(file: {filename}).")
                        _log.info(info_msg)
                    except Exception as e:
                        # YWC: This will happen because we refresh BASE-BADM
                        # files that are not yet in the new QAQC pipeline
                        # DSC: check for processID should take care of this
                        info_msg = ("Failed report_status report_publish_base "
                                    f"for processID {process_id} "
                                    f"(file: {filename}) with error {e}")
                        _log.warning(info_msg)
            except Exception as e:
                _log.error(f"Xfer failed for file {zip_path} with error {e}")
                for process_id, filename in \
                        zip(ast.literal_eval(process_ids_ls),
                            ast.literal_eval(base_name_ls)):
                    if not process_id:
                        _log.info(f"No processID for file: {filename}. "
                                  "Skipping report_status.")
                        continue
                    try:
                        self.report_status.enter_new_state(
                            process_id=process_id,
                            action=self.process_actions.BASEBADMPubFailed,
                            status=self.process_states.BASEBADMPubFailed)
                        info_msg = ("Wrote report_status BASEBADMPubFailed "
                                    f"for processID {process_id} "
                                    f"(file: {filename}).")
                        _log.info(info_msg)
                    except Exception as e:
                        info_msg = ("Failed report_status BASEBADMPubFailed "
                                    f"for processID {process_id} "
                                    f"(file: {filename}) with errror {e}")
                        _log.warning(info_msg)

        for filename in os.listdir(self.BADM_mnt):
            if '_INTERNAL_' in filename:
                continue
            if args.test:
                print(filename)
            else:
                self.publisher.transfer_badm(
                    os.path.join(self.BADM_mnt, filename))

        if not args.test:
            # Publish display tables
            status = self.remote_ssh_util.update_base_badm('publish_display')
            if not status:
                _log.error('Failed to publish display tables')
                return

        # Marked filelist as processed
        os.rename(flist, flist + '.processed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Publish Phase III')
    parser.add_argument('-t', '--test', action='store_true',
                        help='Test mode: for running on Roz')
    args = parser.parse_args()
    PublishBASEBADM().driver(args)
