from logger import Logger
from configparser import ConfigParser
import json
from messages import Messages

__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class StatusCode(object):
    """Class describing status codes """
    OK = 0
    WARNING = -1
    ERROR = -2
    FATAL = -3
    status_map = {}

    def __init__(self):
        """Setup status map that maps status codes to
        string representations
        """

        self.status_map[self.OK] = "OK"
        self.status_map[self.WARNING] = "WARNING"
        self.status_map[self.ERROR] = "ERROR"
        self.status_map[self.FATAL] = "CRITICAL"

    def get_valid_status(self):
        """Return a list of valid status codes

        :rtype: tuple
        :return: List of valid status codes
        """
        return (self.OK, self.WARNING, self.ERROR, self.FATAL)

    def get_str_repr(self, status_code):
        """Return a string representation of a status code

        :rtype: str
        :return: String representation of status code
        """
        if status_code in self.get_valid_status():
            return self.status_map.get(status_code)
        else:
            return None

    def get_str_list(self):
        """ Return the list of valid string representations of codes

        :rtype: list
        :return: list of valid string representatives of codes
        """
        return list(self.status_map.values())

    def get_value_for_str(self, status):
        for s in self.status_map:
            if self.status_map[s] == status:
                return s
        return None


class Status(object):

    def __init__(self, status_code, qaqc_check, src_logger_name,
                 n_warning=0, n_error=0, status_msg=None,
                 plot_paths=None, sub_status=None, report_type='single_msg',
                 report_section='table', NS_ext='',
                 summary_stats=None):
        """Constructor for the Status object.

        Each Status object should always have a status code
        and a QAQC check name

        :param status_code: Status code as described in the StatusCode class
        :type status_code: StatusCode.
        :param qaqc_check: Describes the QAQC check applicable to the status
        :type qaqc_check: str.
        :param src_logger_name: Logger name from where status is derived
        :type src_logger_name: str.
        :param n_warning: Number of warnings (mandatory for StatusCode.WARNING)
        :type n_warning: int.
        :param n_error: Number of errors (mandatory for StatusCode.ERROR)
        :type n_error: int.
        :param plot_paths: List of path plots
        :type plot_paths: list.
        :param sub_status: Holds children Status objects.
        Keys should be specified for each value (Status).
        :type sub_status: dictionary.
        :param report_type: type of report to write
        :type report_type: str

        :rtype: None
        :return: None
        """
        self.read_report_types()
        self.read_section_types()
        self._validate_status(status_code, qaqc_check, src_logger_name,
                              n_warning, n_error, status_msg, plot_paths,
                              sub_status, report_type, report_section, NS_ext)
        self._status = (status_code, qaqc_check, src_logger_name,
                        n_warning, n_error, status_msg, plot_paths, sub_status,
                        report_type, report_section, NS_ext)
        self.summary_stats = summary_stats
        self.msg = Messages()
        # read config file
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            if config.has_section('PHASE_II'):
                self.plot_path = config.get('PHASE_II', 'output_dir')
            else:
                self.plot_path = None
                _log.critical('Cannot find data QAQC output directory '
                              'from config.')

    def __eq__(self, other):
        if not isinstance(other, Status):
            return False

        status_fields = {
            'status_code': 0,
            'qaqc_check': 1,
            'src_logger_name': 2,
            'n_warning': 3,
            'n_errors': 4,
            'status_msg': 5,
            'sub_status': 7,
            'report_type': 8,
            'report_section': 9,
            'NS_ext': 10,
        }

        idx = 6  # index for 'plot_paths' field in _status tuple
        if self._status[idx] is None:
            plot_paths_match = other._status[idx] is None
        else:
            plot_paths_match = len(self._status[idx]) == \
                               len(other._status[idx])

        result = plot_paths_match
        for idx in status_fields.values():
            result = result and (self._status[idx] == other._status[idx])

        return result

    def get_status(self):
        """Returns the current Status object as a tuple

        :rtype: tuple
        :return: Status as a tuple
        """
        return self._status

    def get_status_code(self):
        """Returns the current StatusCode

        :rtype: StatusCode
        :return: StatusCode object
        """
        return self._status[0]

    def get_qaqc_check(self):
        """Returns the Status QAQC check name

        :rtype: str
        :return: QAQC check name
        """
        return self._status[1]

    def get_src_logger_name(self):
        """

        :rtype: str
        :return: Status logger source
        """
        return self._status[2]

    def get_warning_count(self):
        """Returns the Status object warning count

        :rtype: int
        :return: warning count
        """
        return self._status[3]

    def get_error_count(self):
        """Returns the Status object error count

        :rtype: int
        :return: error count
        """
        return self._status[4]

    def get_status_msg(self):
        """Return status message"""
        return self._status[5]

    def get_plot_paths(self):
        """Return ths path for plots

        :rtype: list
        :return: list of plot paths
        """
        return self._status[6]

    def get_sub_status(self):
        """Returns the current sub_statuses as a dict

        :rtype: dict
        :return: dict
        """
        return self._status[7]

    def get_report_type(self):
        """Returns the current report type

        :rtype: str
        :return: str
        """
        return self._status[8]

    def get_report_section(self):
        """Returns the current report section

        :rtype: str
        :return: str
        """
        return self._status[9]

    def get_NS_ext(self):
        """Returns the current NS extension

        :rtype: str
        :return: str
        """
        return self._status[10]
    '''
    def get_status_msg_dict(self):
        """Returns the current status message dictionary as a dict

        :rtype: dict
        :return: dict
        """
        return self._status[10]
    '''

    def set_qaqc_check(self, qaqc_check):
        """Reassigns the QAQC check name

        :rtype: str
        """
        self._status = (self._status[0], qaqc_check, *self._status[2:])

    def assert_status(self, expected_values: dict,
                      ignore_ok_status=False) -> None:
        """ Asserts that the instance variables of the current status
            object matches a dict of expected values. `ignore_ok_status`
            controls whether sub_status objects are recursively checked when
            they have `status_code=0` """

        assert self.get_status_code() == expected_values['status_code']
        assert self.get_src_logger_name() == expected_values['src_logger_name']
        assert self.get_warning_count() == expected_values['n_warning']
        assert self.get_error_count() == expected_values['n_error']
        assert self.get_status_msg() == expected_values['status_msg']
        assert self.get_report_type() == expected_values['report_type']
        assert self.get_report_section() == expected_values['report_section']

        plot_paths = self.get_plot_paths()
        if plot_paths is None:
            assert expected_values['plot_paths'] is None
        else:
            assert isinstance(plot_paths, list)
            assert len(plot_paths) == len(expected_values['plot_paths'])

        sub_status = self.get_sub_status()
        if sub_status is None:
            assert expected_values['sub_status'] is None
        else:
            assert isinstance(sub_status, dict)
            for key, sub_stat in sub_status.items():
                if sub_stat is not None:
                    # If the status_code is 0 and ignore_ok_status=True don't
                    # check that sub_status (assume it's okay)
                    if not(sub_stat.get_status_code() == 0 and
                           ignore_ok_status):
                        assert expected_values['sub_status'] is not None
                        assert key in expected_values['sub_status']
                        sub_stat.assert_status(
                            expected_values['sub_status'][key])

        if 'summary_stats' in expected_values:
            self.assert_summary_stats(expected_values['summary_stats'])
        else:
            self.assert_summary_stats(None)

    def assert_summary_stats(self, expected_values: dict) -> None:
        if self.summary_stats is None:
            assert expected_values is None
        else:
            for key in expected_values:
                assert key in self.summary_stats
                assert self.get_summary_stat(key) == expected_values[key]

    def set_status(self, status_code, qaqc_check, src_logger_name,
                   n_warning=0, n_error=0, status_msg=None,
                   plot_paths=None, sub_status=None, report_type='single_msg',
                   report_section='table', NS_ext=''):
        """Setter for Status Object.

        :param status_code: Status code as described in the StatusCode class
        :type status_code: StatusCode.
        :param qaqc_check: Describes the QAQC check applicable to the status
        :type qaqc_check: str.
        :param n_warning: Number of warnings (mandatory for StatusCode.WARNING)
        :type n_warning: int.
        :param n_error: Number of errors (mandatory for StatusCode.ERROR)
        :type n_error: int.
        :param status_msg: General status message
        :type status_msg: str.
        :param plot_paths: List of path plots
        :type plot_paths: list.
        :param sub_status: Holds children Status objects.
        Keys should be specified for each value (Status).
        :type sub_status: dictionary.
        :param report_type: type of report to write
        :type report_type: str
        :param report_section: section of report
        :type report_section: str

        :rtype: None
        :return: None
        """
        args = (status_code, qaqc_check, src_logger_name,
                n_warning, n_error, status_msg, plot_paths, sub_status,
                report_type, report_section, NS_ext)
        self._validate_status(*args)
        self._status = args

    def add_summary_stat(self, key, value):
        """ Adds a new key value pair to self.summary_stats. Raises an
            exception if that key is already present. """

        if self.summary_stats is None:
            self.summary_stats = {}

        if key in self.summary_stats:
            raise Exception('Field already present in summary_stats. To '
                            'overwrite use set_summary_stat method instead.')
        else:
            self.summary_stats[key] = value

    def add_summary_stats(self, summary_stats: dict):
        """ Adds a dict to self.summary_stats. Raises an exception if a
            previously set key/value pair would be overwritten"""

        if len(summary_stats.keys()) == 0:
            return

        if self.summary_stats is None:
            self.summary_stats = {}

        if any(key in self.summary_stats.keys()
                for key in summary_stats.keys()):
            raise Exception('A previously set key/value pair would be '
                            'overwritten. Use set_summary_stat method '
                            'instead.')

        self.summary_stats.update(summary_stats)

    def set_summary_stat(self, key, value):
        """ Sets a key/value pair in self.summary_stats. Overrides any
            previously set pair."""

        if self.summary_stats is None:
            self.summary_stats = {}

        self.summary_stats[key] = value

    def get_summary_stats(self) -> dict:
        """ Returns self.summary_stats """
        return self.summary_stats

    def get_summary_stat(self, key):
        """ Returns self.summary_stats[key] """
        if key not in self.summary_stats:
            raise KeyError(key)

        return self.summary_stats[key]

    def _validate_status(self, status_code, qaqc_check, src_logger_name,
                         n_warning, n_error, status_msg, plot_paths,
                         sub_status, report_type, report_section, NS_ext):
        """Internal validator to check whether Status is valid.

        :param status_code: Status code as described in the StatusCode class
        :type status_code: StatusCode.
        :param qaqc_check: Describes the QAQC check applicable to the status
        :type qaqc_check: str.
        :param src_logger_name: Logger name from where status is derived
        :type src_logger_name: str.
        :param n_warning: Number of warnings (mandatory for StatusCode.WARNING)
        :type n_warning: int.
        :param n_error: Number of errors (mandatory for StatusCode.ERROR)
        :type n_error: int.
        :param status_msg: General status message
        :type status_msg: str.
        :param plot_paths: List of path plots
        :type plot_paths: list.
        :param sub_status: Holds children Status objects.
        Keys should be specified for each value (Status).
        :type sub_status: dictionary.
        :param report_type: type of report to write
        :type report_type: str
        :param report_section: section of report
        :type report_section: str

        :rtype: None
        :return: None

        :raises TypeError: Invalid type or empty variables.
        :raises StatusCodeException: Invalid StatusCode specified.
        :raises StatusException: Invalid Status requirements.
        """
        info_msg = ('Validating Status with attributes: \n'
                    f'\tStatus Code: {status_code}\n'
                    f'\tQAQC check name: {qaqc_check}\n'
                    f'\tSource logger name: {src_logger_name}\n'
                    f'\tNumber of warnings: {n_warning}\n'
                    f'\tNumber of errors: {n_error}\n'
                    f'\tStatus message: {status_msg}\n'
                    f'\tPlot paths: {plot_paths}\n'
                    f'\tSub_statuses: {sub_status}\n'
                    f'\tReport_type: {report_type}\n'
                    f'\tReport_section: {report_section}\n'
                    f'\tNS_ext: {NS_ext}\n')
        #            + '\tStatus_msg_dict: {smd}\n'.
        #                 format(smd=status_msg_dict))
        _log.info(info_msg)

        # Status Code check
        if not isinstance(status_code, int):
            raise TypeError(
                f'status_code {status_code} is not of type int')
        if status_code not in StatusCode().get_valid_status():
            raise StatusCodeException(
                f'status_code {status_code} is not invalid')

        # QAQC check
        if not qaqc_check:
            err_msg = f'QAQC check {qaqc_check} cannot be empty'
            raise TypeError(err_msg)
        if not isinstance(qaqc_check, str):
            err_msg = f'QAQC check {qaqc_check} is not of type str'
            raise TypeError(err_msg)

        # Source logger name check
        if not src_logger_name:
            err_msg = f'Source logger {src_logger_name} cannot be empty'
            raise TypeError(err_msg)
        if not isinstance(src_logger_name, str):
            err_msg = f'Source logger {src_logger_name} is not of type str'
            raise TypeError(err_msg)

        # Warning count check
        if not isinstance(n_warning, int):
            raise TypeError(f'n_warning {n_warning} is not of type int')

        # Error count check
        if not isinstance(n_error, int):
            raise TypeError(f'n_error {n_error} is not of type int')

        if status_msg and not isinstance(status_msg, str):
            raise TypeError(f'status_msg {status_msg} is not of type str')

        # Minimum specification checks
        if status_code == StatusCode.WARNING and n_warning <= 0:
            err_msg = 'Number of warnings unspecified for status code WARNING'
            raise StatusException(err_msg)
        if status_code == StatusCode.ERROR and n_error <= 0:
            err_msg = 'Number of errors unspecified for status code ERROR'
            raise StatusException(err_msg)

        # Optional param check
        if plot_paths and not isinstance(plot_paths, list):
            raise TypeError(f'plot_paths {plot_paths} is not of type list')

        if sub_status and not isinstance(sub_status, dict):
            raise TypeError(f'sub_status {sub_status} is not of type dict')
        if sub_status:
            for s in sub_status.values():
                if not isinstance(s, Status):
                    err_msg = f'{s} is not of type Status'
                    raise StatusException(err_msg)

        # Report type check
        if report_type and not isinstance(report_type, str):
            raise TypeError(f'report_type {report_type} is not of type str')
        if report_type and report_type not in self._report_types:
            raise TypeError(
                f'report_type {report_type} is not in {self._report_types}')

        # Report section check
        if report_section and not isinstance(report_section, str):
            raise TypeError(
                f'report_section {report_section} is not of type str')
        if report_section and report_section not in self._report_section_types:
            raise TypeError(
                f'report_section {report_section} '
                f'is not in {self._report_section_types}')

        # NS_ext check
        if NS_ext and not isinstance(NS_ext, str):
            raise TypeError(f'NS_ext {report_section} is not of type str')

    def read_report_types(self):
        """Read report_types from qaqc config file"""
        self._report_types = [
            'single_rows', 'single_msg', 'single_list', 'numbers',
            'list_out', 'sub_status_single_msg', 'sub_status_row',
            'sub_status_list_out']

    def read_section_types(self):
        """Read report_types from qaqc config file"""
        self._report_section_types = [
            'info', 'high_level', 'table']

    def make_report_object(self):
        db = False
        # set up status map so that can transfer status code to text
        status_map = StatusCode()

        # set up reused variables
        report_type = self.get_report_type()
        check_name = self.get_qaqc_check()
        src_logger_name = self.get_src_logger_name() + self.get_NS_ext()
        status_code = self.get_status_code()
        status_code_str = status_map.get_str_repr(status_code)
        if not self.get_status_msg():
            status_body = []
        else:
            status_body = [self.get_status_msg()]

        # dict for test specific prefixes --
        # NEED to clean this up!! some messages are in the module some here
        msg_text = {
            'file_name_verifier': {
                'WARNING': self.msg.get_msg('file_name_verifier', 'WARNING'),
                'ERROR': self.msg.get_msg('file_name_verifier', 'ERROR'),
                'FATAL': self.msg.get_msg('file_name_verifier', 'CRITICAL')
                },
            'data_headers': {
                'WARNING': self.msg.get_msg('data_headers', 'WARNING')
                },
            'missing_value_format': {
                'ERROR': self.msg.get_msg('missing_value_format', 'ERROR'),
                'CRITICAL': self.msg.get_msg(
                    'missing_value_format', 'CRITICAL')
                },
            'timestamp_headers_present': {
                'CRITICAL': self.msg.get_msg('timestamp_headers_present',
                                             'CRITICAL')
            },
            'timestamp_headers': {
                'ERROR': self.msg.get_msg('timestamp_headers', 'ERROR')
            },
            'data_missing': {
                'WARNING': self.msg.get_msg('data_missing', 'WARNING')
            },
            'zip_file': {
                'WARNING': self.msg.get_msg('zip_file', 'WARNING')
            },
            'mand_nonfill': {
                'ERROR': self.msg.get_msg('mand_nonfill', 'ERROR')
            }
        }

        msg_text2 = {
            'timestamp_headers_present': {
                'CRITICAL': self.msg.get_msg('timestamp_headers_present',
                                             'CRITICAL', 'report_suffix')
            },
            'data_headers': {
                'WARNING': self.msg.get_msg('data_headers', 'WARNING',
                                            'report_suffix')
            },
            'data_missing': {
                'WARNING': self.msg.get_msg('data_missing', 'WARNING',
                                            'report_suffix')
            },
            'zip_file': {
                'WARNING': self.msg.get_msg('zip_file', 'WARNING',
                                            'report_suffix')
            },
            'mand_nonfill': {
                'ERROR': self.msg.get_msg('mand_nonfill', 'ERROR',
                                          'report_suffix')
            }
        }

        # dictionary for message emphasis -- eventually make this a function
        # that generates codes from external file
        #     or group into types of emphasis
        #     -- i.e., this will need to be refactored!
        msg_emphasis = {'single_list': {'prefix': 0, 'body': 1, 'suffix': 0},
                        'single_rows': {'prefix': 0, 'body': 0, 'suffix': 0},
                        'numbers': {'prefix': 1, 'body': 0, 'suffix': 0},
                        'list_out': {'prefix': 0, 'body': 0, 'suffix': 0},
                        'sub_status_list_out': {'prefix': 0, 'body': 0,
                                                'suffix': 0},
                        'sub_status_row': {'prefix': 1, 'suffix': 0},
                        'sub_status_single_msg': {'prefix': 1, 'suffix': 0},
                        'single_msg': {'prefix': 0, 'body': 0, 'suffix': 0}}

        # if one of the sub_statuses, collapse it
        if 'sub_status' in report_type:
            msg_info = None
            if src_logger_name in msg_text.keys():
                msg_info = msg_text[src_logger_name]
            sub_dict = self._collapse_sub_status(
                sub_statuses=self.get_sub_status(),
                report_type=report_type,
                msg_info=msg_info)
            status_code_list = sub_dict['status_code']
            sub_type = sub_dict['sub_type']
            sub_body = sub_dict['status_body']
            sub_fix = sub_dict['status_fix']
            sub_tar_plots = sub_dict['targeted_plots']
            # if no s_codes, then empty -- see if main message:
            #     treat like single msg
            if not status_code_list and not status_body:
                status_msg_dict = {}
            else:  # case if status_code_list
                status_msg_dict = {}
                for sc in status_code_list:
                    if 'list_out' in report_type:
                        sub_body[sc], eb = self._build_list_status_parts(
                            list_msg=sub_body[sc])
                    len_sc = len(sub_body[sc])
                    if not sub_fix:
                        sub_fix_sc = []
                        emphasis_prefix = []
                    else:
                        sub_fix_sc = sub_fix[sc]
                        emphasis_prefix = [msg_emphasis[sub_type]['prefix']
                                           for i in range(len_sc)]
                    emphasis_body = [msg_emphasis[sub_type]['body']
                                     for i in range(len_sc)]
                    status_msg_dict[sc] = self._get_status_code_dict_obj(
                        status_prefix=sub_fix_sc,
                        status_body=sub_body[sc],
                        emphasis_prefix=emphasis_prefix,
                        emphasis_body=emphasis_body,
                        targeted_plots=sub_tar_plots[sc])
                if status_body or (status_code < -2
                                   and 'CRITICAL' not in status_code_list):
                    if status_code < -2 and not status_body:
                        status_body = ['CRITICAL without msg: check into it.']
                    if status_code_str not in status_code_list:
                        status_code_list.append(status_code_str)
                        status_msg_dict[status_code_str] = \
                            self._get_status_code_dict_obj(
                                status_body=status_body,
                                emphasis_body=[0])
                    else:
                        status_msg_dict[status_code_str]['status_prefix']\
                            .append('')
                        status_msg_dict[status_code_str]['status_body']\
                            .append(status_body[0])
                        status_msg_dict[status_code_str]['emphasize_prefix']\
                            .append(0)
                        status_msg_dict[status_code_str]['emphasize_body']\
                            .append(0)
                        status_msg_dict[status_code_str]['targeted_plots']\
                            .append([])
        else:
            # dict for status_prefix with a crappy work around
            if src_logger_name in msg_text.keys():
                single_list_prefix = \
                    [msg_text[src_logger_name][status_code_str]]
                list_suffix = [msg_text[src_logger_name][status_code_str]]
            else:
                single_list_prefix = []
                list_suffix = []
            if src_logger_name in msg_text2.keys():
                single_list_suffix = \
                    [msg_text2[src_logger_name][status_code_str]]
            else:
                single_list_suffix = []

            msg_prefix = {'single_list': single_list_prefix,
                          'numbers': [self.get_error_count().__str__()],
                          'list_out': [],
                          'single_msg': [],
                          'single_rows': single_list_prefix
                          }

            msg_suffix = {'single_list': single_list_suffix,
                          'numbers': [],
                          'list_out': list_suffix,
                          'single_msg': [],
                          'single_rows': []
                          }

            emphasis_prefix = [msg_emphasis[report_type]['prefix']]
            emphasis_body = [msg_emphasis[report_type]['body']]
            emphasis_suffix = [msg_emphasis[report_type]['suffix']]

            if report_type in msg_prefix.keys():
                status_prefix = msg_prefix[report_type]
            else:
                status_prefix = []

            if report_type in msg_suffix.keys():
                status_suffix = msg_suffix[report_type]
            else:
                status_suffix = []

            if 'list_out' in report_type:
                status_body, emphasis_body = self._build_list_status_parts(
                    list_msg=status_body,
                    emphasis=msg_emphasis['list_out']['body'])
                emphasis_prefix = []
                emphasis_suffix = []

            status_code_list = self._build_status_code_list()
            if db:
                print(status_code_list)

            status_msg_dict = {}
            for i in status_code_list:
                status_msg_dict[i] = self._get_status_code_dict_obj(
                    status_prefix=status_prefix,
                    status_body=status_body,
                    status_suffix=status_suffix,
                    emphasis_prefix=emphasis_prefix,
                    emphasis_body=emphasis_body,
                    emphasis_suffix=emphasis_suffix)
            if db:
                print(status_msg_dict)

        # build object
        report_object = {
            'check_name': check_name,
            'general_code': None,
            'general_msg': None,
            'status_code': status_code_list,
            'status_msg': status_msg_dict,
            'sub_status': []
        }
        if db:
            print(report_object)

        # return json.dumps(report_object)
        return report_object

    def _build_status_code_list(self):
        """
        base the reture status codes on whether on n_counts
        :return: status code list
        """
        # set up status map so that can transfer status code to text
        status_map = StatusCode()

        # get reused variables
        status_code = self.get_status_code()
        n_error = self.get_error_count()
        n_warning = self.get_warning_count()
        status_msg = self.get_status_msg()
        status_code_list = []

        for i in (0, -3):
            if i == status_code and status_msg is not None:
                status_code_list.append(status_map.get_str_repr(i))

        for i in zip((-1, -2), (n_warning, n_error)):
            if i[1] > 0 and status_msg is not None:
                status_code_list.append(status_map.get_str_repr(i[0]))

        return status_code_list

    def _get_status_code_dict_obj(
            self, status_prefix=[], status_body=[], status_suffix=[],
            emphasis_prefix=[], emphasis_body=[], emphasis_suffix=[],
            one_plot=None, all_plots=[], targeted_plots=[],
            plot_dir_path=None):
        """
        :param: status_code_list
        :return: dict of status message parts for each
        """

        dict_obj = {
            'status_prefix': status_prefix,
            'status_body': status_body,
            'status_suffix': status_suffix,
            'emphasize_prefix': emphasis_prefix,
            'emphasize_body': emphasis_body,
            'emphasize_suffix': emphasis_suffix,
            'one_plot': one_plot,
            'all_plots': all_plots,
            'targeted_plots': targeted_plots,
            'plot_dir_path': plot_dir_path
        }

        return dict_obj

    def _collapse_sub_status(self, sub_statuses, report_type, msg_info=None):
        """
        look thru all sub statuses to return report object pieces
        :param: sub_statuses = dictionary of substatuses
        :param: sub_status_type = 'sub_status_row' or 'sub_status_single_msg'
        :return: dict of status_code and msg_pieces
        """
        status_map = StatusCode()
        status_codes = []
        status_body = {}
        status_fix = {}
        status_tar_plots = {}
        sub_type1 = None

        for s in sub_statuses.keys():
            s_code = sub_statuses[s].get_status_code()
            sub_type = sub_statuses[s].get_report_type()
            s_msg = sub_statuses[s].get_status_msg()
            s_plot_paths = sub_statuses[s].get_plot_paths()
            s_tar_plots = s_plot_paths
            if s_code > -1:  # if status code is OK, move to next item
                continue
            # set the general sub_type
            if not sub_type1:
                sub_type1 = sub_type
            # set up the output lists
            # get the str version of the status code
            str_code = status_map.get_str_repr(s_code)
            # if the status code is not already in the list...
            if str_code not in status_codes:
                # add it to the list
                status_codes.append(str_code)
                # add the code the body dict and set up an empty list for it
                status_body[str_code] = []
                # add the code to the targeted plot dict and set up list
                status_tar_plots[str_code] = []
                if sub_type not in ('single_msg'):
                    # set up the pre or suffix dictionary
                    status_fix[str_code] = []
            # this is a fatal case
            if s_code < -2:
                status_body[str_code].append(s_msg)
                if s_tar_plots:
                    status_tar_plots[str_code].extend(s_tar_plots)
                else:
                    status_tar_plots[str_code].append([])
            elif sub_type == 'single_msg':
                status_body[str_code].append(s_msg)
                if s_tar_plots:
                    status_tar_plots[str_code].extend(s_tar_plots)
                else:
                    status_tar_plots[str_code].append([])
            elif sub_type == 'single_list':
                status_body[str_code].append(s_msg)
                status_fix[str_code].append(msg_info[str_code])
            else:  # for warnings and errors
                for sc, n in zip((status_map.get_str_repr(-1),
                                  status_map.get_str_repr(-2)),
                                 (sub_statuses[s].get_warning_count(),
                                  sub_statuses[s].get_error_count())):
                    if n < 1:
                        continue
                    if sc not in status_codes:
                        status_codes.append(sc)
                        status_body[sc] = []
                        status_fix[sc] = []
                        status_tar_plots[sc] = []
                    if sub_type == 'numbers':
                        fix_text = str(n)
                        if report_type == 'sub_status_single_msg':
                            fix_text = s + '(' + str(n) + ')'
                        # put msg in body to match numbers
                        status_body[sc].append(s_msg)
                        # put numbers in prefix to match numbers
                        status_fix[sc].append(fix_text)
                        if s_tar_plots:
                            status_tar_plots[sc].extend(s_tar_plots)
                        else:
                            status_tar_plots[sc].append([])
                    # probably will need other sub_type here to
                    #     deal with the nested sub-sub-statuses
                    else:
                        print('fatal_msg')
                        _log.fatal('Sub_report_type not supported '
                                   'in making report object. Breaking')
                        # need some help breaking nicely here
        if report_type == 'sub_status_single_msg' and sub_type == 'numbers':
            for sc in status_codes:
                status_fix[sc] = [', '.join(status_fix[sc])]
                # swap to match single_list stats in body
                temp_holder = status_fix[sc]
                if sc != status_map.get_str_repr(-3):
                    if len(set(status_body[sc])) > 1:
                        _log.info('Mixed sub_status messages. '
                                  'Expect jumbled messaging.')
                    status_fix[sc] = [', '.join(set(status_body[sc]))]
                    status_body[sc] = temp_holder  # swap to match single list
                    if s_tar_plots:
                        status_tar_plots[sc].extend(s_tar_plots)
                    else:
                        status_tar_plots[sc].append([])

        for sc in status_codes:
            if [len(p) > 0 for p in status_tar_plots[sc]].count(True) < 1:
                status_tar_plots[sc] = []

        return {'status_code': status_codes, 'status_body': status_body,
                'status_fix': status_fix, 'sub_type': sub_type1,
                'targeted_plots': status_tar_plots}

    def _build_list_status_parts(self, list_msg, emphasis=0):
        """
        converts a comma separated str list into a list of strings
            for the status dictionary.
        :param list_msg:
        :return:
        """
        # print(list_msg)
        status_body = list_msg[0].split(", ")
        length_status_body = len(status_body)
        emphasis_body = []
        for i in range(length_status_body):
            emphasis_body.append(emphasis)
        return status_body, emphasis_body

    def _write_status_json_form(self):
        """
        write the status object in a dictionary form so that
            it can be dumped into a json
        :return:
        """
        pass


class StatusGenerator():

    def _get_best_status(self, logger):
        n_warning = logger.warning_count
        n_error = logger.error_count
        n_fatal = logger.fatal_count

        if n_fatal:
            status_code = StatusCode.FATAL
        elif n_error:
            status_code = StatusCode.ERROR
        elif n_warning:
            status_code = StatusCode.WARNING
        else:
            status_code = StatusCode.OK

        return status_code

    def status_generator(self, logger, qaqc_check,
                         plots=None, status_msg=None, report_type='single_msg',
                         report_section='table', NS_ext=''):
        """Helper to generate status based on logger class"""
        n_warning = logger.warning_count
        n_error = logger.error_count
        src_logger_name = logger.getName()
        status_args = []
        status_code = self._get_best_status(logger)
        sub_status = None

        status_args.extend(
            [status_code, qaqc_check, src_logger_name, n_warning,
             n_error, status_msg, plots, sub_status, report_type,
             report_section, NS_ext])
        return Status(*status_args)

    def composite_status_generator(self, logger, qaqc_check, plot_paths=None,
                                   keep_sub_status_name=False,
                                   status_msg=None, statuses=None,
                                   report_type='single_msg',
                                   report_section='table', NS_ext=''):
        n_warning = logger.warning_count
        n_error = logger.error_count
        src_logger_name = logger.getName()

        status_code = self._get_best_status(logger)
        status_args = []
        sub_status = {}
        if statuses:
            for s, s_key in zip(statuses.values(), statuses.keys()):
                n_warning += s.get_warning_count()
                n_error += s.get_error_count()
                status_code = min(status_code, s.get_status_code())
                if keep_sub_status_name:
                    sub_status[s_key] = s
                else:
                    sub_status[s.get_qaqc_check()] = s
        status_args.extend(
            [status_code, qaqc_check, src_logger_name,
             n_warning, n_error, status_msg, plot_paths, sub_status,
             report_type, report_section, NS_ext])
        return Status(*status_args)

    def split_status_generator(self, logger, qaqc_check, plot_paths=None,
                               status_msg=None,
                               report_type='sub_status_single_msg',
                               report_section='table', status_msgs=None,
                               sub_type='single_msg'):
        n_warning = logger.warning_count
        n_error = logger.error_count
        n_counts = [logger.fatal_count, n_error, n_warning, logger.info_count]
        src_logger_name = logger.getName()

        status_code = self._get_best_status(logger)
        status_args = []
        sub_status = {}

        if 'fatal' in status_msgs.keys():
            status_msgs[StatusCode().get_str_repr(-3).lower()] = \
                status_msgs.pop('fatal')

        any_msg_parts = False
        for s in status_msgs.values():
            if s:
                any_msg_parts = True

        if any_msg_parts:
            for s in [0, -1, -2, -3]:
                s_counts = [0, 0, 0, 0]
                status_str = StatusCode().get_str_repr(s)
                if not status_msgs[status_str.lower()]:
                    continue
                stat_msg = status_msgs[status_str.lower()]
                s_counts[s+3] = n_counts[s+3]
                sub_name = qaqc_check + status_str
                sub_status[sub_name] = Status(status_code=s,
                                              qaqc_check=qaqc_check,
                                              src_logger_name=src_logger_name,
                                              n_warning=s_counts[2],
                                              n_error=s_counts[1],
                                              status_msg=stat_msg,
                                              report_type=sub_type)
        else:
            sub_status = None
        status_args.extend(
            [status_code, qaqc_check, src_logger_name,
             n_warning, n_error, status_msg, plot_paths, sub_status,
             report_type, report_section])
        return Status(*status_args)

    def check_for_empty_status(self, init_status_len, current_status_len,
                               warning_msg=None,
                               current_log=None):
        """
        check for empty status for given lists of status pre and post check.
            if there is no status, return an warning status
        :param init_status_len: initial list length
        :param current_status_len: current list length
        :param warning_msg: warning message. IF exists, then warning
            status will be generated
        :param current_log: log object to write to
        :return: logical and single msg status object if list is empty
        """
        if current_status_len > init_status_len:
            return False, None
        else:
            if warning_msg:
                if not current_log:
                    _log.info('Single warning message for check_for_'
                              'empty_status did not have logger. Status '
                              'object not returned.')
                    return True, None
                return True, self.generate_single_msg_warning_status(
                                log_obj=current_log,
                                qaqc_check=current_log.getName(),
                                status_msg=warning_msg)
            return True, None

    def generate_single_msg_warning_status(self, log_obj, qaqc_check,
                                           status_msg):
        """
        generate a single message warning status
        *** ASSUMES no other statuses logged to the submitted logger ***
        :param log_obj: logger object
        :param qaqc_check: name of the qaqc check
        :param status_msg: status message
        :return: the status object
        """
        log_obj.warning(status_msg)
        return self.status_generator(
            logger=log_obj, qaqc_check=qaqc_check,
            status_msg=status_msg, report_type='single_msg')


class StatusCodeException(Exception):
    """Exception class for StatusCode """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class StatusException(Exception):
    """Exception class for Status """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class StatusEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Status):
            return obj._status
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def _test():
    # Outdated Examples (This has changed and needs to be updated)

    # Create a status object with status code OK corresponding to
    # QAQC check with specified name.
    test_logger = Logger().getLogger("test")
    s = Status(StatusCode.OK,
               'multivariate intercomparison for variables TA and TS',
               test_logger.getName())

    print(s.get_status())

    # Modify existing status with new status code and number of warnings
    s.set_status(StatusCode.WARNING, 'test',
                 test_logger.getName(), n_warning=1)

    # get status
    print(s.get_status())

    # Status with annual status
    qaqc_check = ('Multivariate intercomparison for variables TA and TS '
                  'between {s} and {e}')
    annual_status = {}
    q1 = qaqc_check.format(s='2008', e='2009')
    annual_status['2008'] = Status(StatusCode.OK, q1, test_logger.getName())

    q2 = qaqc_check.format(s='2009', e='2010')
    annual_status['2009'] = Status(StatusCode.WARNING, qaqc_check=q2,
                                   src_logger_name=test_logger.getName(),
                                   n_warning=5)

    s = Status(StatusCode.WARNING,
               qaqc_check.format(s='2008', e='2010'),
               test_logger.getName(),
               n_warning=5, sub_status=annual_status)

    print(s.get_status())


if __name__ == "__main__":
    _test()
