import os
from random import randint
import subprocess
from configparser import ConfigParser
from logger import Logger
from report_status import ReportStatus
from process_states import ProcessStates
from utils import Decode
from future.standard_library import install_aliases
install_aliases()

__author__ = "You-Wei Cheah", "Danielle Christianson"
__email__ = "ycheah@lbl.gov", "dschristianson@lbl.gov"

_log = Logger().getLogger(__name__)


class Publish():
    def __init__(self):
        config = ConfigParser()
        self.decode = Decode()
        cwd = os.getcwd()

        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            config.read_file(cfg)
            pub_cfg = 'PUBLISH'
            if config.has_section(pub_cfg):
                self.scp_hostname = config.get(pub_cfg, 'scp_hostname')
                self.scp_username = config.get(pub_cfg, 'scp_username')
                self.scp_ssh_key = config.get(pub_cfg, 'scp_ssh_key')
                self.scp_port = config.get(pub_cfg, 'scp_port')
                self.scp_verbose = config.getboolean(
                    pub_cfg, 'scp_verbose')
                self.scp_src = config.get(pub_cfg, 'scp_src')
                self.scp_target = config.get(pub_cfg, 'scp_target')
                self.scp_badm_target = config.get(
                    pub_cfg, 'scp_badm_target')
                self.scp_basebadm_target = config.get(
                    pub_cfg, 'scp_base_badm_target')
            else:
                _log.error('Unable to read properties for copying files.')

    def _build_path(self, base_path, args):
        """
        Helper function to build a path by concatenating
        list of args to a base path
        """
        path = base_path
        for a in args:
            path = os.path.join(path, a)
        return path

    def _ssh_getdirname(self, site_id):
        """
        Gets ssh_dir name given a site ID if it exists
        :param site_id: site_id to get directory name in target ssh dir
        :type site_id: str
        """
        dir_name = None
        base_cmd_args = ['ssh', '-i', self.scp_ssh_key]
        remote_args = [''.join([self.scp_username, '@', self.scp_hostname])]
        ls_dir_args = " 'ls -a {t} | grep {s}'".format(
            t=self.scp_target, s=site_id)
        base_cmd_args.extend(remote_args)
        cmd = ' '.join(base_cmd_args) + ls_dir_args
        try:
            result = subprocess.check_output(
                cmd, stderr=subprocess.STDOUT, shell=True)
            _log.info('Get directory completed successfully')
            dir_name = self.decode.byte_to_str(result.strip())
            return dir_name
        except subprocess.CalledProcessError as ex:
            _log.error('Get directory failed with error: {e}'.format(
                e=str(ex)))
            return dir_name

    def _ssh_mkdir(self, site_id):
        """
        Create directory for site IDs
        :param site_id: site_id to create directory
        :type site_id: str
        """
        dir_name = self._ssh_getdirname(site_id)
        if dir_name:
            info_msg = 'Site directory exists with name {n}'
            _log.info(info_msg.format(n=dir_name))
            return
        base_cmd_args = ['ssh', '-i', self.scp_ssh_key]
        remote_args = [''.join([self.scp_username, '@', self.scp_hostname])]
        # Anonymizing code for directory names
        dir_name_args = ['.', site_id, '_', str(randint(int(1e6), int(1e7)-1))]
        dir_name = ''.join(dir_name_args)
        target_path = os.path.join(self.scp_target, dir_name)
        create_args = " 'mkdir -p {t}'".format(t=target_path)
        base_cmd_args.extend(remote_args)
        create_cmd = ' '.join(base_cmd_args) + create_args
        try:
            _log.info('FTP creating site directory {s}'.format(s=site_id))
            execution_result = subprocess.call(
                create_cmd, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, shell=True)
            if execution_result != 0:
                _log.error("SSH mkdir failed for: '{c}'".format(c=create_cmd))
            else:
                _log.info('Create directory completed successfully')
        except Exception as e:
            _log.error('Create directory failed with error: {e}'.format(e=e))

    def _scp_xfer_process(self, site_id, process_id):
        """
        Upload filelist of files to file server

        :param filelist: list of files
        :type filelist: fpp.fvm.FileListFluxnet
        """
        dir_name = self._ssh_getdirname(site_id)
        _log.info('Building arguments for FTP xfer of process')
        verbose_args = ['-v'] if self.scp_verbose else []
        base_cmd_args = [
            'scp', '-i', self.scp_ssh_key, '-P', self.scp_port]
        local_args = ['-r', self._build_path(
            self.scp_src, [site_id, process_id])]
        remote_args = [''.join([self.scp_username, '@', self.scp_hostname, ':',
                       os.path.join(self.scp_target, dir_name)])]

        base_cmd_args.extend(verbose_args)
        base_cmd_args.extend(local_args)
        base_cmd_args.extend(remote_args)

        xfer_cmd = ' '.join(base_cmd_args)
        rs = ReportStatus()
        try:
            _log.info('FTP xfering')
            execution_result = subprocess.call(
                xfer_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                shell=True)
            if execution_result != 0:
                s = ProcessStates.RepublishReport
                _log.error("SCP failed for: '{c}'".format(c=xfer_cmd))
                rs.report_status(
                    state_id=s, report_json=None,
                    log_file_path=None, process_id=process_id)
            else:
                _log.info('Xfer completed successfully')
        except Exception as e:
            s = ProcessStates.RepublishReport
            _log.error('Xfer failed with error: {e}'.format(e=e))
            rs.report_status(
                state_id=s, report_json=None,
                log_file_path=None, process_id=process_id)

    def _scp_xfer_BADM_file(self, path):
        self._scp_xfer_file(path, self.scp_badm_target)

    def _scp_xfer_BASEBADM_file(self, path):
        self._scp_xfer_file(path, self.scp_basebadm_target)

    def _scp_xfer_file(self, path, target):
        _log.info('Building arguments for FTP xfer of single file')
        verbose_args = ['-v'] if self.scp_verbose else []
        base_cmd_args = [
            'scp', '-i', self.scp_ssh_key, '-P', self.scp_port]
        remote_args = [''.join([self.scp_username, '@', self.scp_hostname, ':',
                                target])]
        base_cmd_args.extend(verbose_args)
        base_cmd_args.append(path)
        base_cmd_args.extend(remote_args)
        xfer_cmd = ' '.join(base_cmd_args)
        try:
            _log.info('FTP xfering')
            execution_result = subprocess.call(
                xfer_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                shell=True)
            if execution_result != 0:
                _log.error("SCP failed for: '{c}'".format(c=xfer_cmd))
            else:
                _log.info('Xfer completed successfully')
        except Exception as e:
            _log.error('Xfer failed with error: {e}'.format(e=e))

    def transfer(self, site_id, process_id):
        """
        Execution wrapper
        """
        self._ssh_mkdir(site_id)
        self._scp_xfer_process(site_id, process_id)

    def transfer_basebadm(self, path):
        self._scp_xfer_BASEBADM_file(path)

    def transfer_badm(self, path):
        self._scp_xfer_BADM_file(path)
