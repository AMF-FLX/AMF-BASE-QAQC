import os
import logging
from configparser import ConfigParser
from datetime import datetime as dt
from path_util import PathUtil

__author__ = "You-Wei Cheah", "Norm Beekwilder", "Danielle Christianson"
__email__ = "ycheah@lbl.gov", "norm.beekwilder@gmail.com", \
            "dschristianson@lbl.gov"


class Logger(logging.Logger):
    """Class to standardize logging format"""
    def __init__(self, setup=False, upload_id=0,
                 site_id='UNK', process_type=None, log_timestamp=None):
        """Constructor for logger class. Setup logger format here

        :param setup: Optional parameter to setup file handler
        :type setup: boolean.
        """
        self.info_count = 0
        self.warning_count = 0
        self.error_count = 0
        self.fatal_count = 0
        self.fatal_msg = None
        if not setup:
            return

        logging.root.handlers = []  # Start with no handlers
        config = ConfigParser()
        cwd = os.getcwd()
        phase_cfg = None
        with open(os.path.join(cwd, 'qaqc.cfg')) as cfg:
            log_cfg = 'LOG'
            if process_type in ('File Format', 'FormatQAQCDriver'):
                phase_cfg = 'PHASE_I'
            elif process_type == 'BASE Generation':
                phase_cfg = 'PHASE_II'
            elif process_type in ('GenBASEBADM', 'preBASERegen'):
                phase_cfg = 'PHASE_III'
            else:
                raise Exception("Unrecognized process_type.")

            config.read_file(cfg)
            if config.has_section(log_cfg) and config.has_section(phase_cfg):
                if phase_cfg == 'PHASE_II':
                    self.base_dir = PathUtil().create_dir_for_run(
                        site_id, upload_id,
                        config.get(phase_cfg, 'output_dir'))
                else:
                    self.base_dir = config.get(phase_cfg, 'output_dir')
                self.log_dir = PathUtil().create_valid_path(
                    self.base_dir, config.get(log_cfg, 'log_output_dir'))

                default_log_level = config.get(log_cfg, 'default_level')
                try:
                    default_log_level = eval(default_log_level)
                except Exception:
                    err_msg = ('Unable to evaluate config file log level: '
                               f'{default_log_level}. Using default log '
                               'level INFO.')
                    print(err_msg)
                    default_log_level = logging.INFO
            else:
                self.log_dir = os.path.join(cwd, 'logs')
                default_log_level = logging.INFO
                warning_msg = ('Cannot find log configurations from config. '
                               f'Setting to local output dir: {self.log_dir}. '
                               'Setting default log level to INFO.')
                print(warning_msg)

        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir)

        # Setup defaults
        if not log_timestamp:
            self.log_file_timestamp = dt.now()
        else:
            self.log_file_timestamp = log_timestamp
        log_ts_fmt = '%Y%m%d%H%M%S'
        timestamp_str = self.log_file_timestamp.strftime(log_ts_fmt)
        if phase_cfg == 'PHASE_I' and process_type != 'FormatQAQCDriver':
            self.log_file_name = (f'QAQC_report_{site_id}_{upload_id}_'
                                  f'{timestamp_str}.log')
        elif phase_cfg == 'PHASE_II':
            self.log_file_name = f'QAQC_report_{upload_id}_{timestamp_str}.log'
        else:
            process_type_fmt = '-'.join(process_type.split(' '))
            self.log_file_name = f'{process_type_fmt}_{timestamp_str}.log'
        print(self.log_file_name)
        self.default_log = os.path.join(
            self.log_dir, self.log_file_name)
        default_format = ('%(asctime)s [%(levelname)s] '
                          '%(name)s - %(message)s')
        default_formatter = logging.Formatter(default_format)
        print(self.base_dir)
        print(self.log_dir)

        # Initialize root level logger
        logger = logging.getLogger('')
        logger.setLevel(default_log_level)
        file_handler = logging.FileHandler(self.default_log)
        file_handler.setLevel(default_log_level)
        file_handler.setFormatter(default_formatter)

        # Setup console output
        console = logging.StreamHandler()
        console.setLevel(default_log_level)
        console.setFormatter(default_formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console)

    def getLogger(self, name):
        """Return the logger from logging library"""
        self._log = logging.getLogger(str(name))
        return self

    def getOriginalLogger(self):
        """Return original logger"""
        return self._log

    def getName(self):
        """Return the logger name"""
        return self.getOriginalLogger().name

    def error(self, *args, **kwargs):
        """Increase error count and log error"""
        self.error_count += 1
        self._log.error(*args, **kwargs)

    def warning(self, *args, **kwargs):
        """Increase warning count and log warning"""
        self.warning_count += 1
        self._log.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        """Increase info count and log info"""
        self.info_count += 1
        self._log.info(*args, **kwargs)

    def fatal(self, *args, **kwargs):
        """Set fatal message and log fatal error"""
        self.fatal_count += 1
        self.fatal_msg = args[0]
        self._log.fatal(*args, **kwargs)

    def critical(self, *args, **kwargs):
        """Log critical error"""
        self._log.critical(*args, **kwargs)

    def debug(self, *args, **kwargs):
        """Log debug message"""
        self._log.debug(*args, **kwargs)

    def resetStats(self):
        """Reset all stats"""
        self.info_count = 0
        self.warning_count = 0
        self.error_count = 0
        self.fatal_count = 0
        self.fatal_msg = None

    def _getModuleName(self):
        """Internal call to get module that is calling the logger"""
        module = self.findCaller()[0]
        module_noext = os.path.splitext(module)[0]
        return os.path.basename(module_noext)

    def get_log_dir(self):
        return self.log_dir
