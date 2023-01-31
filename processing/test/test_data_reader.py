from data_reader import DataReader
from fp_vars import FPVariables
from logger import Logger
from status import Status
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'

_log = Logger().getLogger(__name__)


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'FC': 'dummy',
            'TIMESTAMP_END': 'dummy', 'TIMESTAMP_START': 'dummy'}


def mock_FPVariables_load_fp_vars_dict(dummyself):
    return {}


@pytest.fixture
def data_reader(monkeypatch):
    monkeypatch.setattr(FPVariables, '_load_fp_vars_dict',
                        mock_FPVariables_load_fp_vars_dict)
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)

    ff = DataReader(test_mode=True)
    if not ff.filename:
        ff.filename = 'US-UMB_HH_200001010000_200012312330.csv'
    return ff


def test_check_timestamp_header(data_reader):
    # tv = test_vars
    tv = ['TIMESTAMP_START', 'TIMESTAMP_END', 'TIMESTAMP',
          ' TIMESTAMP_END', 'TIMESTAMP_END ', 'TIMESTAMP_ END',
          '"TIMESTAMP_END"']
    assert data_reader._check_timestamp_header(tv[0:2], check_log=_log) is None
    assert data_reader._check_timestamp_header(tv[1::-1], check_log=_log) == \
        'TIMESTAMP_END, TIMESTAMP_START'
    assert data_reader._check_timestamp_header(
        [tv[2], 'FC_1_1_1'], check_log=_log) == 'TIMESTAMP, FC_1_1_1'
    assert data_reader._check_timestamp_header(
        [tv[0], tv[3]], check_log=_log) is None
    assert data_reader._check_timestamp_header(
        [tv[0], tv[4]], check_log=_log) is None
    assert data_reader._check_timestamp_header(
        [tv[0], tv[5]], check_log=_log) == 'TIMESTAMP_ END'
    assert data_reader._check_timestamp_header(
        [tv[0], tv[6]], check_log=_log) is None


def test_check_data_header(data_reader):
    good_headers = ['FC_1_1_1', 'FC']
    bad_headers = ['FC_1_1', 'FC_1_1_1_PI', 'FC_1_1_1_F', ' FC_1_1_1',
                   'FC _1_1_1', 'FC_1_1_1 ', ' "FC_1_1_1 "',
                   'FC_PI_F_1_1_1']
    # Note: 'FC_PI_F_1_1_1' is technically not FP-In. It needs to go thru
    #       the fixer to remove PI which will get put back when published.
    quote_headers = ['"FC_1_1_1"', '"FC_1_2_1"']
    time_vars = ['TIMESTAMP_START', 'TIMESTAMP_END']
    test_vars = time_vars + good_headers
    assert data_reader._check_data_header(test_vars, check_log=_log) is None
    test_vars = time_vars + bad_headers + quote_headers
    assert data_reader._check_data_header(
        test_vars, check_log=_log) == ', '.join(bad_headers + quote_headers)
    test_vars = time_vars + quote_headers
    assert data_reader._check_data_header(
        test_vars, check_log=_log) == ', '.join(quote_headers)
    test_vars = ['TIMESTAMP_START ', '"TIMESTAMP_END"']
    assert data_reader._check_data_header(
        test_vars, check_log=_log) == ', '.join(test_vars)
    test_vars = ['"TIMESTAMP_START"', '"TIMESTAMP_END"'] + quote_headers
    assert data_reader._check_data_header(test_vars, check_log=_log) is None


def test_all_headers_have_quotes(data_reader):
    test_vars = [['TIMESTAMP_START', 'TIMESTAMP_END', 'FC', 'SW_IN'],
                 ['TIMESTAMP_START', 'TIMESTAMP_END', 'FC', '"SW_IN"'],
                 ['TIMESTAMP_START', 'TIMESTAMP_END', '"FC"', '"SW_IN"'],
                 ['"TIMESTAMP_START"', '"TIMESTAMP_END"', '"FC"', '"SW_IN"']]
    results = [False, False, False, True]
    for test_var, result in zip(test_vars, results):
        assert data_reader._all_headers_have_quotes(test_var, _log) is result


def test_check_any_valid_header(data_reader):
    test_base_headers = [
        {'FC': ['FC_1_1_1', 'FC_1_2_1']},
        {'FC': ['FC_1_2_1'], 'FC2': ['FC2_1_1_1']},
        {'FC2': ['FC2_1_1_1', 'FC2_1_2_1']}]
    results = [True, True, False]

    for base_headers, result in zip(test_base_headers, results):
        data_reader.base_headers = base_headers
        assert data_reader._check_any_valid_header(check_log=_log) is result


def test_check_mandatory_data_headers(data_reader):
    test_base_headers = [
        {'FC': ['FC_1_1_1', 'FC_1_2_1'], 'H': 'H', 'LE': 'LE'},
        {'FC': ['FC_1_2_1'], 'FC2': ['FC2_1_1_1']},
        {'FC2': ['FC2_1_1_1', 'FC2_1_2_1']},
        {'FC4': ['FC_1_2_1'], 'FC2': ['FC2_1_1_1']}]
    status_results = [0, 0, -1, 0]

    for base_headers, status_result in zip(test_base_headers, status_results):
        data_reader.base_headers = base_headers
        _log.resetStats()
        status = data_reader._check_mandatory_data_headers(check_log=_log)
        assert status.get_status_code() == status_result


def test_get_base_header_no_parameter(data_reader):
    ''' test get_base_header without any parameters '''

    # NO VAR & NO QUAL
    assert data_reader.get_base_header('') == ''

    # NO VAR & OK QUAL
    assert data_reader.get_base_header('1') == '1'
    # ABOVE should be '', but ignored as edge case
    assert data_reader.get_base_header('1_1_1') == ''
    assert data_reader.get_base_header('_1') == ''
    assert data_reader.get_base_header('_1_1_1') == ''
    assert data_reader.get_base_header('_1_1_A') == ''
    assert data_reader.get_base_header('_1_1_A_SD') == ''
    assert data_reader.get_base_header('_1_1_A_N') == ''
    assert data_reader.get_base_header('_F_1') == ''
    assert data_reader.get_base_header('_F_1_1_1') == ''
    assert data_reader.get_base_header('_F_1_1_A') == ''
    assert data_reader.get_base_header('_F_1_1_A_SD') == ''
    assert data_reader.get_base_header('_F_1_1_A_N') == ''

    # NO VAR & BAD QUAL
    assert data_reader.get_base_header('1_1_1_1') == '1_1_1_1'
    assert data_reader.get_base_header('_SD') == '_SD'
    assert data_reader.get_base_header('_A_SD_SD_F') == '_A_SD_SD_F'

    # OK VAR & NO QUAL
    assert data_reader.get_base_header('TA') == 'TA'
    assert data_reader.get_base_header('HELLO') == 'HELLO'
    assert data_reader.get_base_header('SW_IN') == 'SW_IN'

    # OK VAR & OK QUAL
    assert data_reader.get_base_header('TA_1') == 'TA'
    assert data_reader.get_base_header('TA_1_1_1') == 'TA'
    assert data_reader.get_base_header('TA_1_1_A') == 'TA'
    assert data_reader.get_base_header('SW_IN_1_1_A_SD') == 'SW_IN'
    assert data_reader.get_base_header('SW_IN_1_1_A_N') == 'SW_IN'
    assert data_reader.get_base_header('HELLO_F_1') == 'HELLO'
    assert data_reader.get_base_header('HELLO_F_1_1_1') == 'HELLO'
    assert data_reader.get_base_header('HELLO_F_1_1_A') == 'HELLO'
    assert data_reader.get_base_header('A_STR_F_1_1_A_SD') == 'A_STR'
    assert data_reader.get_base_header('A_STR_F_1_1_A_N') == 'A_STR'
    assert data_reader.get_base_header('1_1_#_1_1_1') == '1_1_#'
    # ABOVE: odd, but still potetially valid...

    # OK VAR & BAD QUAL
    assert data_reader.get_base_header('TA_1_1') == 'TA_1_1'
    assert data_reader.get_base_header('TA_1_1_1_1') == 'TA_1_1_1_1'
    assert data_reader.get_base_header('TA_#_SD') == 'TA_#_SD'
    assert data_reader.get_base_header('TA_@') == 'TA_@'
    assert data_reader.get_base_header('1_1_1_#_1_1') == '1_1_1_#_1_1'


def test_get_base_header_bad_header(data_reader):
    ''' test get_base_header asking to return bad headers '''

    # NO VAR & NO QUAL
    assert data_reader.get_base_header('',
                                       return_bad_header=True,
                                       ) == ('', None)

    # NO VAR & OK QUAL
    assert data_reader.get_base_header('1',
                                       return_bad_header=True,
                                       ) == ('1', None)
    # ABOVE should be '', but ignored as edge case
    assert data_reader.get_base_header('1_1_1',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_1',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_1_1_1',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_1_1_A',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_1_1_A_SD',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_1_1_A_N',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_F_1',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_F_1_1_1',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_F_1_1_A',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_F_1_1_A_SD',
                                       return_bad_header=True,
                                       ) == ('', None)
    assert data_reader.get_base_header('_F_1_1_A_N',
                                       return_bad_header=True,
                                       ) == ('', None)

    # NO VAR & BAD QUAL
    assert data_reader.get_base_header('1_1_1_1',
                                       return_bad_header=True,
                                       ) == ('1_1_1_1', None)
    assert data_reader.get_base_header('_SD',
                                       return_bad_header=True,
                                       ) == ('_SD', None)
    assert data_reader.get_base_header('_A_SD_SD_F',
                                       return_bad_header=True,
                                       ) == ('_A_SD_SD_F', None)

    # OK VAR & NO QUAL
    assert data_reader.get_base_header('TA',
                                       return_bad_header=True,
                                       ) == ('TA', None)
    assert data_reader.get_base_header('HELLO',
                                       return_bad_header=True,
                                       ) == ('HELLO', None)
    assert data_reader.get_base_header('SW_IN',
                                       return_bad_header=True,
                                       ) == ('SW_IN', None)

    # OK VAR & OK QUAL
    assert data_reader.get_base_header('TA_1',
                                       return_bad_header=True,
                                       ) == ('TA', None)
    assert data_reader.get_base_header('TA_1_1_1',
                                       return_bad_header=True,
                                       ) == ('TA', None)
    assert data_reader.get_base_header('TA_1_1_A',
                                       return_bad_header=True,
                                       ) == ('TA', None)
    assert data_reader.get_base_header('SW_IN_1_1_A_SD',
                                       return_bad_header=True,
                                       ) == ('SW_IN', None)
    assert data_reader.get_base_header('SW_IN_1_1_A_N',
                                       return_bad_header=True,
                                       ) == ('SW_IN', None)
    assert data_reader.get_base_header('HELLO_F_1',
                                       return_bad_header=True,
                                       ) == ('HELLO', None)
    assert data_reader.get_base_header('HELLO_F_1_1_1',
                                       return_bad_header=True,
                                       ) == ('HELLO', None)
    assert data_reader.get_base_header('HELLO_F_1_1_A',
                                       return_bad_header=True,
                                       ) == ('HELLO', None)
    assert data_reader.get_base_header('A_STR_F_1_1_A_SD',
                                       return_bad_header=True,
                                       ) == ('A_STR', None)
    assert data_reader.get_base_header('A_STR_F_1_1_A_N',
                                       return_bad_header=True,
                                       ) == ('A_STR', None)
    assert data_reader.get_base_header('1_1_#_1_1_1',
                                       return_bad_header=True,
                                       ) == ('1_1_#', None)
    # ABOVE: odd, but still potetially valid...

    # OK VAR & BAD QUAL
    assert data_reader.get_base_header('TA_1_1',
                                       return_bad_header=True,
                                       ) == ('TA_1_1', None)
    assert data_reader.get_base_header('TA_1_1_1_1',
                                       return_bad_header=True,
                                       ) == ('TA_1_1_1_1', None)
    assert data_reader.get_base_header('TA_#_SD',
                                       return_bad_header=True,
                                       ) == ('TA_#_SD', None)
    assert data_reader.get_base_header('TA_@',
                                       return_bad_header=True,
                                       ) == ('TA_@', None)
    assert data_reader.get_base_header('1_1_1_#_1_1') == '1_1_1_#_1_1'


def test_get_base_header_qualifier_list(data_reader):
    ''' test get_base_header asking to return qualifier list '''

    # NO VAR & NO QUAL
    assert data_reader.get_base_header('',
                                       return_qualifier_list=True,
                                       ) == ('', [])

    # NO VAR & OK QUAL
    assert data_reader.get_base_header('1',
                                       return_qualifier_list=True,
                                       ) == ('1', [])
    # ABOVE should be '', but ignored as edge case
    assert data_reader.get_base_header('1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('', ['1', '1', '1'])
    assert data_reader.get_base_header('_1',
                                       return_qualifier_list=True,
                                       ) == ('', ['1'])
    assert data_reader.get_base_header('_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('', ['1', '1', '1'])
    assert data_reader.get_base_header('_1_1_A',
                                       return_qualifier_list=True,
                                       ) == ('', ['1', '1', 'A'])
    assert data_reader.get_base_header('_1_1_A_SD',
                                       return_qualifier_list=True,
                                       ) == ('', ['1', '1', 'A', 'SD'])
    assert data_reader.get_base_header('_1_1_A_N',
                                       return_qualifier_list=True,
                                       ) == ('', ['1', '1', 'A', 'N'])
    assert data_reader.get_base_header('_F_1',
                                       return_qualifier_list=True,
                                       ) == ('', ['F', '1'])
    assert data_reader.get_base_header('_F_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('', ['F', '1', '1', '1'])
    assert data_reader.get_base_header('_F_1_1_A',
                                       return_qualifier_list=True,
                                       ) == ('', ['F', '1', '1', 'A'])
    assert data_reader.get_base_header('_F_1_1_A_SD',
                                       return_qualifier_list=True,
                                       ) == ('', ['F', '1', '1', 'A', 'SD'])
    assert data_reader.get_base_header('_F_1_1_A_N',
                                       return_qualifier_list=True,
                                       ) == ('', ['F', '1', '1', 'A', 'N'])

    # NO VAR & BAD QUAL
    assert data_reader.get_base_header('1_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('1_1_1_1', [])
    assert data_reader.get_base_header('_SD',
                                       return_qualifier_list=True,
                                       ) == ('_SD', [])
    assert data_reader.get_base_header('_A_SD_SD_F',
                                       return_qualifier_list=True,
                                       ) == ('_A_SD_SD_F', [])

    # OK VAR & NO QUAL
    assert data_reader.get_base_header('TA',
                                       return_qualifier_list=True,
                                       ) == ('TA', [])
    assert data_reader.get_base_header('HELLO',
                                       return_qualifier_list=True,
                                       ) == ('HELLO', [])
    assert data_reader.get_base_header('SW_IN',
                                       return_qualifier_list=True,
                                       ) == ('SW_IN', [])

    # OK VAR & OK QUAL
    assert data_reader.get_base_header('TA_1',
                                       return_qualifier_list=True,
                                       ) == ('TA', ['1', ])
    assert data_reader.get_base_header('TA_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('TA', ['1', '1', '1'])
    assert data_reader.get_base_header('TA_1_1_A',
                                       return_qualifier_list=True,
                                       ) == ('TA', ['1', '1', 'A'])
    assert data_reader.get_base_header('SW_IN_1_1_A_SD',
                                       return_qualifier_list=True,
                                       ) == ('SW_IN', ['1', '1', 'A', 'SD'])
    assert data_reader.get_base_header('SW_IN_1_1_A_N',
                                       return_qualifier_list=True,
                                       ) == ('SW_IN', ['1', '1', 'A', 'N'])
    assert data_reader.get_base_header('HELLO_F_1',
                                       return_qualifier_list=True,
                                       ) == ('HELLO', ['F', '1'])
    assert data_reader.get_base_header('HELLO_F_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('HELLO', ['F', '1', '1', '1'])
    assert data_reader.get_base_header('HELLO_F_1_1_A',
                                       return_qualifier_list=True,
                                       ) == ('HELLO', ['F', '1', '1', 'A'])
    assert data_reader.get_base_header('A_STR_F_1_1_A_SD',
                                       return_qualifier_list=True,
                                       ) == ('A_STR',
                                             ['F', '1', '1', 'A', 'SD'])
    assert data_reader.get_base_header('A_STR_F_1_1_A_N',
                                       return_qualifier_list=True,
                                       ) == ('A_STR',
                                             ['F', '1', '1', 'A', 'N'])
    assert data_reader.get_base_header('1_1_#_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('1_1_#', ['1', '1', '1'])
    # ABOVE: odd, but still potetially valid...

    # OK VAR & BAD QUAL
    assert data_reader.get_base_header('TA_1_1',
                                       return_qualifier_list=True,
                                       ) == ('TA_1_1', [])
    assert data_reader.get_base_header('TA_1_1_1_1',
                                       return_qualifier_list=True,
                                       ) == ('TA_1_1_1_1', [])
    assert data_reader.get_base_header('TA_#_SD',
                                       return_qualifier_list=True,
                                       ) == ('TA_#_SD', [])
    assert data_reader.get_base_header('TA_@',
                                       return_qualifier_list=True,
                                       ) == ('TA_@', [])
    assert data_reader.get_base_header('1_1_1_#_1_1',
                                       return_qualifier_list=True,
                                       ) == ('1_1_1_#_1_1', [])


def test_check_root_qualifier_headers(data_reader):
    test_headers = {
        'CO2': ['CO2', 'CO2_F'],
        'G': ['G', 'G_1_1_1'],
        'SW_IN': ['SW_IN'],
        'TA': ['TA_1', 'TA_1_1_2', 'TA_F'],
        'TS': ['TS', 'TS_1_1_1', 'TS_F'],
        'USTAR': ['USTAR'],
    }
    data_reader.base_headers = test_headers

    status = data_reader.check_root_qualifier_headers()
    expected_status = Status(
        status_code=-1,
        qaqc_check='Check for duplicate root/qualifier headers',
        src_logger_name='check_root_qualifier_headers',
        n_warning=2,
        n_error=0,
        status_msg='Found duplicate root/qualifier headers: G:G_1_1_1, '
                        'TS:TS_1_1_1.',
    )

    assert status == expected_status
