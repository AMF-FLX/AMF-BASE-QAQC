from file_fixer import FileFixer
from file_name_verifier import FileNameVerifier
from fp_vars import FPVariables
from status import Status
from site_attrs import SiteAttributes
import datetime as dt
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_FPVariables_get_fp_vars_dict(dummyself):
    return {'TA': 'dummy', 'SW_IN': 'dummy', 'FC': 'dummy',
            'TS': 'dummy', 'VPD': 'dummy', 'WS': 'dummy',
            'TIMESTAMP_END': 'dummy', 'FETCH_FILTER': 'dummy'}


def mock_FPVariables_load_fp_vars_dict(dummyself):
    return {}


@pytest.fixture
def file_fixer(monkeypatch):
    ''' Initializes FileFixer '''

    monkeypatch.setattr(FPVariables, '_load_fp_vars_dict',
                        mock_FPVariables_load_fp_vars_dict)
    monkeypatch.setattr(FPVariables, 'get_fp_vars_dict',
                        mock_FPVariables_get_fp_vars_dict)

    ff = FileFixer(test_mode=True)
    return ff


def test_get_full_year(file_fixer):
    assert file_fixer._get_full_year('90') == 1990
    assert file_fixer._get_full_year('1990') == 1990
    assert file_fixer._get_full_year('2000') == 2000
    assert file_fixer._get_full_year('00') == 2000
    assert file_fixer._get_full_year('2090') == 2090
    assert file_fixer._get_full_year('2100') == 2100
    assert file_fixer._get_full_year('2010') == 2010
    assert file_fixer._get_full_year('10') == 2010
    assert file_fixer._get_full_year('2.0e3') == 2000
    assert file_fixer._get_full_year('2.0e5') is None
    assert file_fixer._get_full_year('100') is None
    assert file_fixer._get_full_year('999') is None
    assert file_fixer._get_full_year('-9999') is None
    assert file_fixer._get_full_year('Inf') is None


def test_fix_header(file_fixer):
    test_vars = ['ws', 'TSOIL', 'TA_1_1_1_F', 'TA_PI_F_1_1_1', 'VPD_PI',
                 ' TIMESTAMP_END', 'TA_TOP', 'SW_IN', 'FETCH_FILTER',
                 'TA_2', 'TS _1_2_1', ' SW_ IN ', '    TS_1_1_1_ZL',
                 '"FC_1_1_1"', 'FC_"_1_1', ' "FC_1_1_1 "', 'TS_F',
                 'TS_1_1_A', 'TS_1_A', 'TS_1_1_A_F', 'TS_PI']
    msg_dict = file_fixer.fix_header_msgs
    quote_msg = msg_dict['rm_character'].format(c='quotes')
    whitespace_msg = msg_dict['rm_character'].format(c='whitespace')
    assert file_fixer.fix_header('') == (False, '', None)
    assert file_fixer.fix_header(test_vars[0]) == (
        True, 'WS', msg_dict['case'])
    assert file_fixer.fix_header(test_vars[1]) == (
        True, 'TS', msg_dict['synonym'])
    assert file_fixer.fix_header(test_vars[2]) == (
        True, 'TA_F_1_1_1', msg_dict['reorder'])
    assert file_fixer.fix_header(test_vars[3]) == (
        True, 'TA_F_1_1_1', msg_dict['rm_pi'])
    assert file_fixer.fix_header(test_vars[4]) == (
        True, 'VPD', msg_dict['rm_pi'])
    assert file_fixer.fix_header(test_vars[5]) == (
        True, 'TIMESTAMP_END', whitespace_msg)
    assert file_fixer.fix_header(test_vars[6]) == (False, 'TA_TOP', None)
    assert file_fixer.fix_header(test_vars[7]) == (True, 'SW_IN', None)
    assert file_fixer.fix_header(test_vars[8]) == (True, 'FETCH_FILTER', None)
    assert file_fixer.fix_header(test_vars[9]) == (True, 'TA_2', None)
    assert file_fixer.fix_header(test_vars[10]) == (
        True, 'TS_1_2_1', whitespace_msg)
    assert file_fixer.fix_header(test_vars[11]) == (
        True, 'SW_IN', whitespace_msg)
    assert file_fixer.fix_header(test_vars[12]) == (False, 'TS_1_1_1_ZL', None)
    assert file_fixer.fix_header(test_vars[13]) == (
        True, 'FC_1_1_1', quote_msg)
    assert file_fixer.fix_header(test_vars[14]) == (False, 'FC__1_1', None)
    assert file_fixer.fix_header(test_vars[15]) == (
        True, 'FC_1_1_1', '; '.join([whitespace_msg, quote_msg]))
    assert file_fixer.fix_header(test_vars[16]) == (True, 'TS_F', None)
    assert file_fixer.fix_header(test_vars[17]) == (True, 'TS_1_1_A', None)
    assert file_fixer.fix_header(test_vars[18]) == (False, 'TS_1_A', None)
    assert file_fixer.fix_header(test_vars[19]) == (
        True, 'TS_F_1_1_A', msg_dict['reorder'])
    assert file_fixer.fix_header(test_vars[20]) == (False, 'TS_PI', None)


def mock_site_attrs_load_site_dict(dummyself):
    return {'US-Ton': 'Tonzi'}


def generate_file_attrs(filename):
    file_attrs = {'site_id': 'US-Ton', 'resolution': 'HH', 'ext': '.csv',
                  'ts_start': '201001010000', 'ts_end': '201112312330'}
    if 'NS' in filename:
        file_attrs['optional'] = 'NS'
    if 'v1' in filename:
        file_attrs['optional'] = 'v1'
    if '_-' in filename:
        file_attrs['optional'] = ''
    if '__' in filename:
        file_attrs['ts_start'] = ''
        file_attrs['ts_end'] = '201001010000'
        file_attrs['optional'] = '201112312330'
    if 'other' in filename:
        file_attrs['site_id'] = 'other'
    if 'uploadtime' in filename:
        file_attrs['ts_upload'] = 'uploadtime'
    return file_attrs


def mock_filename_verifier_driver(self, filename, fixer):
    self.fname_attrs = generate_file_attrs(filename)
    msg = None
    if any(text in filename for text in ['NS', 'v1', '_-', '__']):
        msg = 'optional param'
    if 'US_Ton' in filename:
        msg = 'incorrect number of components'
    return Status(-3, 'Is Filename Format valid?', 'file_name_verifier',
                  0, 0, msg)


def mock_filename_verifier_is_AMF_site_id(dummyvar, site_id):
    if site_id in ('US-Ton', 'US-PFa'):
        return True
    return False


def mock_filename_verifier_is_valid_resolution(dummyvar, resolution):
    if resolution in ('HR', 'HH'):
        return True
    return False


def mock_get_upload_info(process_id):
    if process_id == '1234':
        return {'SITE_ID': 'other'}
    return {'SITE_ID': 'US-Ton'}


def test_fix_filename(file_fixer, monkeypatch):

    monkeypatch.setattr(file_fixer, 'get_upload_info', mock_get_upload_info)

    monkeypatch.setattr(SiteAttributes, '_load_site_dict',
                        mock_site_attrs_load_site_dict)

    monkeypatch.setattr(FileNameVerifier, 'driver',
                        mock_filename_verifier_driver)

    monkeypatch.setattr(FileNameVerifier, 'is_AMF_site_id',
                        mock_filename_verifier_is_AMF_site_id)

    monkeypatch.setattr(FileNameVerifier, 'is_valid_resolution',
                        mock_filename_verifier_is_valid_resolution)

    valid_filename_base = '_HH_201001010000_201112312330'

    bad_filenames = ['US-Ton_HH_201001010000_201112312330_-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312330_NS-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312330_NS-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312330_v1-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312330_v1-uploadtime.csv',
                     'other_HH_201001010000_201112312330-uploadtime.csv',
                     'US_Ton_HH_201001010000_201112312330-uploadtime.csv',
                     'US-Ton_HH_201001010030_201112312330-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312300-uploadtime.csv',
                     'US-Ton_HR_201001010000_201112312330-uploadtime.csv',
                     'US-Ton_HH__201001010000_201112312330-uploadtime.csv',
                     'US-Ton_HH_201001010000_201112312330-uploadtime.csv']

    for filename in bad_filenames:
        valid_site_id = 'US-Ton'
        process_id = '9999'
        if 'other' in filename:
            valid_site_id = 'other'
            process_id = '1234'

        remade_filename, site_id, status_msg = file_fixer.fix_filename(
            dir_name='~/test', filename_noext=filename.split(".")[0],
            process_id=process_id, timespan=dt.timedelta(minutes=30),
            corrected_data=[['201001010000', '201001010030', 'more_data'],
                            ['201001010030', '201001010100', 'more_data'],
                            ['201112312230', '201112312300', 'more_data'],
                            ['201112312300', '201112312330', 'more_data']])

        assert remade_filename == f'{valid_site_id}{valid_filename_base}.csv'
        assert site_id == valid_site_id


def test_strip_whitespace(file_fixer):
    test_and_comparison = []

    test_tokens = (
        '201601010000, 201601010030, -63.3900, -37.3606, -6.1844, '
        '-14.8509, 0.1838, 0.1112, 408.7241, 7.2133, 5.7312, 86.3784, '
        '3.1180, 80.5577, 2.1530,  80.7000, 5.2200, 74.3869, 2.3016, '
        '82.3000, 0.0000, 0.0000, 0.0000, 266.5000, 325.9000, 2.0850, '
        '0.1039, 0.2195, 0.1039, 2.0000, 7.0000, 9.0000, 4.5000, 8.2000, '
        '9.3000').split(',')
    result_tokens = (
        '201601010000,201601010030,-63.3900,-37.3606,-6.1844,'
        '-14.8509,0.1838,0.1112,408.7241,7.2133,5.7312,86.3784,'
        '3.1180,80.5577,2.1530,80.7000,5.2200,74.3869,2.3016,'
        '82.3000,0.0000,0.0000,0.0000,266.5000,325.9000,2.0850,'
        '0.1039,0.2195,0.1039,2.0000,7.0000,9.0000,4.5000,8.2000,'
        '9.3000').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    tokens = ('201601010000,201601010030,-63.3900,-37.3606,-6.1844').split(',')
    test_and_comparison.append((tokens, tokens, False))

    test_tokens = (' 201601010000,201601010030 ').split(',')
    result_tokens = ('201601010000,201601010030').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    for test_tokens, result_tokens, result_boolean in test_and_comparison:
        assert file_fixer._strip_whitespace(test_tokens) == \
            (result_tokens, result_boolean)


def test_strip_quotes(file_fixer):
    test_and_comparison = []

    test_tokens = (
        '"201601010000","201601010030","-63.3900","-37.3606","-6.1844",'
        '"-14.8509","0.1838","0.1112","408.7241","7.2133","5.7312","86.3784",'
        '"3.1180","80.5577","2.1530","80.7000","5.2200","74.3869","2.3016",'
        '"9.3000"').split(',')
    result_tokens = (
        '201601010000,201601010030,-63.3900,-37.3606,-6.1844,'
        '-14.8509,0.1838,0.1112,408.7241,7.2133,5.7312,86.3784,'
        '3.1180,80.5577,2.1530,80.7000,5.2200,74.3869,2.3016,'
        '9.3000').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    tokens = ('201601010000,201601010030,-63.3900,-37.3606,-6.1844').split(',')
    test_and_comparison.append((tokens, tokens, False))

    test_tokens = ('"201601010000,2016010"10030').split(',')
    result_tokens = ('201601010000,2016010"10030').split(',')
    test_and_comparison.append((test_tokens, result_tokens, True))

    for test_tokens, result_tokens, result_boolean in test_and_comparison:
        assert file_fixer._strip_quotes(test_tokens) == \
            (result_tokens, result_boolean)


def test_duplicate_variables(file_fixer):
    headers = ['TIMESTAMP_START', 'TIMESTAMP_END', 'TA', 'FC', 'SW_IN']
    assert file_fixer.duplicate_variables(headers) is None
    headers = ['TIMESTAMP_START', 'TIMESTAMP_END', 'TA', 'FC', 'TA_d0']
    assert file_fixer.duplicate_variables(headers) == 'TA (column 5)'
    headers = ['TIMESTAMP_START', 'TIMESTAMP_END',
               'TA', 'FC', 'TA_d0', 'FC_d0']
    assert file_fixer.duplicate_variables(headers) == ('TA (column 5); '
                                                       'FC (column 6)')
    headers = ['TIMESTAMP_START', 'TIMESTAMP_END',
               'TA_1', 'FC', 'TA_1_d12', 'FC_d0']
    assert file_fixer.duplicate_variables(headers) == ('TA_1 (column 5); '
                                                       'FC (column 6)')
