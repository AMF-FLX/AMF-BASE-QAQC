import pytest
from utils import VarUtil

__author__ = 'You-Wei Cheah', 'Danielle Christianson'
__email__ = 'ycheah@lbl.gov', 'dschristianson@lbl.gov'


@pytest.fixture
def var_util():
    ''' Initializes VarUtil '''
    v = VarUtil()
    # Mock var dict if unable to get from WS
    if not v.var_dict:
        v.var_dict = {'TA': 'dummy', 'SW_IN': 'dummy', 'FETCH_70': 'dummy',
                      'FETCH_MAX': 'dummy', 'CO2': 'dummy'}
    if not v.PI_vars:
        v.PI_vars = ['VPD', 'NEE', 'RECO', 'GPP']
    return v


def test_is_valid_variable(var_util):
    valid_variables = [
        'SW_IN',
        'SW_IN_1_1_1',
        'SW_IN_1_1_A',
        'SW_IN_1_1_A_SD',
        'SW_IN_1_1_A_N',
        'SW_IN_1',
        'SW_IN_1_SD',
        'SW_IN_1_N',
        'SW_IN_F',
        'SW_IN_F_1_1_1',
        'SW_IN_F_1_1_A',
        'SW_IN_F_1_1_A_SD',
        'SW_IN_F_1_1_A_N',
        'SW_IN_F_1',
        'SW_IN_F_1_SD',
        'SW_IN_F_1_N',
        'FETCH_70',
        'FETCH_70_1_1_1',
        'FETCH_70_1',
        'FETCH_MAX',
        'FETCH_MAX_1_1_1',
        'FETCH_MAX_1'
    ]
    invalid_variables = [
        'NONONON',
        'SW_IN_1_1',
        'SW_IN_1_1_1_1',
        'SW_IN_A',
        'SW_IN_SD',
        'SW_IN_N',
        'TIMESTAMP_START_1',
        'SW_IN_F_SD',
        'SW_IN_N_F',
        'SW_IN_d1'
    ]
    for v in valid_variables:
        assert var_util.is_valid_variable(label=v)
    for v in invalid_variables:
        assert not var_util.is_valid_variable(label=v)


def test_is_valid_qualifier(var_util):
    valid_qualifiers = [
        '',
        '1_1_1',
        '1_1_A',
        '1_1_A_SD',
        '1_1_A_N',
        '1',
        '_1_SD',
        '_1_N',
        '_F',
        '_F_1_1_1',
        'F_1_1_A',
        'F_1_1_A_SD',
        'F_1_1_A_N',
        'F_1',
        'F_1_SD',
        'F_1_N',
    ]
    invalid_qualifiers = [
        'NONONON',
        '1_1',
        '_1_1',
        '1_1_1_1',
        '_A',
        '_SD',
        '_N',
        '_F_SD',
        '_SD_F'
    ]
    for v in valid_qualifiers:
        assert var_util.is_valid_qualifier(label=v)
    for v in invalid_qualifiers:
        assert not var_util.is_valid_qualifier(label=v)


def test_parse_h_v_r(var_util):
    assert var_util.parse_h_v_r(None) == (None, None, None, None)
    assert var_util.parse_h_v_r('TA') == (None, None, None, None)
    assert var_util.parse_h_v_r('TS_1_1_1') == ('TS', 1, 1, 1)
    assert var_util.parse_h_v_r('PPFD_IN_1_2_1') == ('PPFD_IN', 1, 2, 1)
    assert var_util.parse_h_v_r('T_SONIC_1_2_A') == (None, None, None, None)
    assert var_util.parse_h_v_r('T_SONIC_1_2_#') == (None, None, None, None)


def test_get_top_level_variables(var_util):
    test_vars = [
        'PPFD_IN_1_2_1', 'PPFD_IN_1_3_1', 'PPFD_IN_2_2_1',
        'PPFD_IN_1_4_2', 'PPFD_IN_2_2_4']
    test_vars1 = ['PPFD_IN_1_2_1', 'PPFD_IN_1_3_1', 'PPFD_IN_2_2_A']
    test_vars2 = ['PPFD_IN_1_4_2', 'PPFD_IN_2']
    test_vars3 = ['PPFD_IN_1_4_A', 'PPFD_IN_2_1_1']
    test_vars4 = ['PPFD_IN_1', 'PPFD_IN_2_1_1']
    test_vars5 = ['PPFD_IN_1_1_A', 'PPFD_IN_1_1_1', 'PPFD_IN_1_1_2',
                  'PPFD_IN_1_2_1']
    # PPFD_IN_1_A is not a valid aggregate variable
    test_vars6 = ['PPFD_IN_1_A', 'PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables([]) == []
    assert var_util.get_top_level_variables(test_vars) == ['PPFD_IN_1_2_1',
                                                           'PPFD_IN_2_2_1',
                                                           'PPFD_IN_2_2_4']
    assert var_util.get_top_level_variables(test_vars1) == ['PPFD_IN_1_2_1',
                                                            'PPFD_IN_2_2_A']
    assert var_util.get_top_level_variables(test_vars2) == ['PPFD_IN_2']
    assert var_util.get_top_level_variables(test_vars3) == ['PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables(test_vars4) == ['PPFD_IN_1',
                                                            'PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables(test_vars5) == ['PPFD_IN_1_1_A',
                                                            'PPFD_IN_1_1_1',
                                                            'PPFD_IN_1_1_2']
    assert var_util.get_top_level_variables(test_vars6) == ['PPFD_IN_2_1_1']
    test_vars7 = [
        'PPFD_IN_F_1_2_1', 'PPFD_IN_F_1_3_1', 'PPFD_IN_2_2_1',
        'PPFD_IN_1_4_2', 'PPFD_IN_2_2_4']
    assert var_util.get_top_level_variables(test_vars7) == ['PPFD_IN_2_2_1',
                                                            'PPFD_IN_2_2_4']
    assert var_util.get_top_level_variables(
        var_ls=test_vars7, include_filled_vars=True) == ['PPFD_IN_F_1_2_1',
                                                         'PPFD_IN_2_2_1',
                                                         'PPFD_IN_2_2_4']
    assert var_util.get_top_level_variables(
        var_ls=test_vars7, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1_2_1', 'PPFD_IN_2_2_1',
                                       'PPFD_IN_2_2_4']
    assert var_util.get_top_level_variables(
        var_ls=test_vars7, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_2_2_1',
                                       'PPFD_IN_2_2_4', 'PPFD_IN_F_1_2_1']
    test_vars8 = ['PPFD_IN_1_1_1', 'PPFD_IN_F_1_1_1']
    assert var_util.get_top_level_variables(test_vars8) == ['PPFD_IN_1_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars8, include_filled_vars=True) == ['PPFD_IN_1_1_1',
                                                         'PPFD_IN_F_1_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars8, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars8, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_1_1_1']
    test_vars9 = ['PPFD_IN_1_1_1', 'PPFD_IN_F_1_1_1', 'PPFD_IN',
                  'PPFD_IN_2_1_1', 'PPFD_IN_1_2_1']
    assert var_util.get_top_level_variables(test_vars9) == [
        'PPFD_IN_1_1_1', 'PPFD_IN', 'PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars9, include_filled_vars=True) == [
        'PPFD_IN_1_1_1', 'PPFD_IN_F_1_1_1', 'PPFD_IN', 'PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars9, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1_1_1', 'PPFD_IN',
                                       'PPFD_IN_2_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars9, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_1_1_1', 'PPFD_IN',
                                       'PPFD_IN_2_1_1']
    test_vars10 = ['PPFD_IN_F_1_2_1', 'PPFD_IN_F_1_3_1', 'PPFD_IN_F_2_2_1']
    assert var_util.get_top_level_variables(test_vars10) == []
    assert var_util.get_top_level_variables(
        var_ls=test_vars10, include_filled_vars=True) == ['PPFD_IN_F_1_2_1',
                                                          'PPFD_IN_F_2_2_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars10, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1_2_1', 'PPFD_IN_F_2_2_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars10, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_F_1_2_1', 'PPFD_IN_F_2_2_1']
    test_vars11 = ['PPFD_IN_F_1', 'PPFD_IN_F_2_1_1']
    assert var_util.get_top_level_variables(test_vars11) == []
    assert var_util.get_top_level_variables(
        var_ls=test_vars11, include_filled_vars=True) == ['PPFD_IN_F_1',
                                                          'PPFD_IN_F_2_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars11, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1', 'PPFD_IN_F_2_1_1']
    assert var_util.get_top_level_variables(
        var_ls=test_vars11, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_F_1', 'PPFD_IN_F_2_1_1']
    test_vars12 = ['PPFD_IN_F_1_1_A', 'PPFD_IN_F_1_1_1', 'PPFD_IN_F_1_1_2',
                   'PPFD_IN_F_1_2_1']
    assert var_util.get_top_level_variables(test_vars12) == []
    assert var_util.get_top_level_variables(
        var_ls=test_vars12, include_filled_vars=True) == [
        'PPFD_IN_F_1_1_A', 'PPFD_IN_F_1_1_1', 'PPFD_IN_F_1_1_2']
    assert var_util.get_top_level_variables(
        var_ls=test_vars12, include_filled_vars=True,
        var_preference='gap-fill') == ['PPFD_IN_F_1_1_A', 'PPFD_IN_F_1_1_1',
                                       'PPFD_IN_F_1_1_2']
    assert var_util.get_top_level_variables(
        var_ls=test_vars12, include_filled_vars=True,
        var_preference='non-fill') == ['PPFD_IN_F_1_1_A', 'PPFD_IN_F_1_1_1',
                                       'PPFD_IN_F_1_1_2']


def test_get_lowest_horiz_variables(var_util):
    test_vars = [
        'PPFD_IN_1_2_1', 'PPFD_IN_1_3_1', 'PPFD_IN_2_2_1',
        'PPFD_IN_1_4_2', 'PPFD_IN_2_2_4']
    test_vars1 = [
        'PPFD_IN_1_2_1', 'PPFD_IN_1_3_1', 'PPFD_IN_2_2_A']
    test_vars2 = ['PPFD_IN_1_4_2', 'PPFD_IN_2_A']
    test_vars3 = ['PPFD_IN_1_4_A', 'PPFD_IN_2_1_1']
    # PPFD_IN_1_A is not a valid aggregate variable
    test_vars4 = ['PPFD_IN_1_A', 'PPFD_IN_2_1_1']
    test_vars5 = ['PPFD_IN_1_1_A', 'PPFD_IN_2_1_1']
    # PPFD_IN_1 does not have a horizontal position
    test_vars6 = ['PPFD_IN_1', 'PPFD_IN_2_1_1']
    assert var_util.get_lowest_horiz_variables([]) == []
    assert var_util.get_lowest_horiz_variables(test_vars) == [
        'PPFD_IN_1_2_1', 'PPFD_IN_1_3_1', 'PPFD_IN_1_4_2']
    assert var_util.get_lowest_horiz_variables(test_vars1) == ['PPFD_IN_1_2_1',
                                                               'PPFD_IN_1_3_1']
    assert var_util.get_lowest_horiz_variables(test_vars2) == ['PPFD_IN_1_4_2']
    assert var_util.get_lowest_horiz_variables(test_vars3) == ['PPFD_IN_1_4_A']
    assert var_util.get_lowest_horiz_variables(test_vars4) == ['PPFD_IN_2_1_1']
    assert var_util.get_lowest_horiz_variables(test_vars5) == ['PPFD_IN_1_1_A']
    assert var_util.get_lowest_horiz_variables(test_vars6) == ['PPFD_IN_2_1_1']


def test_get_nearest_lower_level_variables(var_util):
    test_vars = ['TA', 'TA_1_2_1', 'TA_1_2_2', 'TA_2_2_1', 'TA_1_4_2',
                 'TA_2_2_4']
    test_vars2 = ['SW_IN', 'SW_IN_1_2_1', 'SW_IN_1_2_2', 'SW_IN_1_1_A']
    assert var_util.get_nearest_lower_level_variables([], 0) == []
    assert var_util.get_nearest_lower_level_variables([], float('inf')) == []
    assert var_util.get_nearest_lower_level_variables(test_vars, 0) == ['TA']
    assert var_util.get_nearest_lower_level_variables(
        test_vars, float('-inf')) == ['TA']
    assert var_util.get_nearest_lower_level_variables(test_vars, 1) == [
        'TA_1_2_1', 'TA_1_2_2', 'TA_2_2_1', 'TA_2_2_4']
    assert var_util.get_nearest_lower_level_variables(
        test_vars, 2) == ['TA_1_4_2']
    assert var_util.get_nearest_lower_level_variables(test_vars, 3) == [
        'TA_1_4_2']
    assert var_util.get_nearest_lower_level_variables(test_vars2, 0) == [
        'SW_IN', 'SW_IN_1_1_A']
    assert var_util.get_nearest_lower_level_variables(test_vars2, 1) == [
        'SW_IN_1_2_1', 'SW_IN_1_2_2']


def test_get_lowest_r_variable(var_util):
    test_vars = ['TA', 'TA_1_1_1', 'TA_1_1_A']
    test_vars2 = ['SW_IN', 'SW_IN_1_1_3', 'SW_IN_1_1_2']
    test_vars3 = ['SW_IN_1_1_3', 'SW_IN_1_1_4', 'SW_IN_1_1_5']
    test_vars4 = ['SW_IN_2_1_3', 'SW_IN_1_1_4', 'SW_IN_1_1_5']
    test_vars5 = ['SW_IN_2_1_3', 'SW_IN_1_1_3', 'SW_IN_1_1_5']
    test_vars6 = ['SW_IN_2_1_3', 'SW_IN_1_1_3', 'SW_IN_1_1_1']
    test_vars7 = ['SW_IN_2_1_3', 'SW_IN_1_1_3', 'SW_IN_3_1_1']
    assert var_util.get_lowest_r_variable([]) is None
    assert var_util.get_lowest_r_variable(test_vars) is None
    assert var_util.get_lowest_r_variable(test_vars2) == 'SW_IN'
    assert var_util.get_lowest_r_variable(test_vars3) == 'SW_IN_1_1_3'
    assert var_util.get_lowest_r_variable(test_vars4) == 'SW_IN_2_1_3'
    assert var_util.get_lowest_r_variable(test_vars5) is None
    assert var_util.get_lowest_r_variable(test_vars6) == 'SW_IN_1_1_1'
    assert var_util.get_lowest_r_variable(test_vars7) == 'SW_IN_3_1_1'
    test_vars8 = ['TA_F', 'TA_1_1_1', 'TA_1_1_A']
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars8) == 'TA_1_1_1'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars8, include_filled_vars=True) is None
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars8, include_filled_vars=True,
        var_preference='gap') is None
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars8, include_filled_vars=True,
        var_preference='non') == 'TA_1_1_1'
    test_vars9 = ['TA_F', 'TA_F_1_1_1', 'TA_1_1_A']
    assert var_util.get_lowest_r_variable(top_level_var_ls=test_vars9) is None
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars9, include_filled_vars=True) is None
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars9, include_filled_vars=True,
        var_preference='gap') == 'TA_F_1_1_1'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars9, include_filled_vars=True,
        var_preference='non') is None
    test_vars10 = ['SW_IN_F', 'SW_IN_F_1_1_3', 'SW_IN_1_1_2']
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars10) == 'SW_IN_1_1_2'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars10, include_filled_vars=True) == 'SW_IN_F'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars10, include_filled_vars=True,
        var_preference='gap') == 'SW_IN_F'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars10, include_filled_vars=True,
        var_preference='non') == 'SW_IN_F'
    test_vars11 = ['SW_IN_F_1_1_3', 'SW_IN_F_1_1_4', 'SW_IN_F_1_1_5']
    assert var_util.get_lowest_r_variable(top_level_var_ls=test_vars11) is None
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars11,
        include_filled_vars=True) == 'SW_IN_F_1_1_3'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars11, include_filled_vars=True,
        var_preference='gap') == 'SW_IN_F_1_1_3'
    assert var_util.get_lowest_r_variable(
        top_level_var_ls=test_vars11, include_filled_vars=True,
        var_preference='non') == 'SW_IN_F_1_1_3'


def test_gen_base_var_with_idx(var_util):
    test_vars = [
        'TA_1_1_1', 'SWC_1_2_1_F', 'WS_1_1_5', 'FC_SSITC_TEST_PI', 'CH4_1_2_1']
    for var in test_vars:
        assert var_util.gen_base_var_with_idx(var) == var
    assert var_util.gen_base_var_with_idx('TA') == 'TA_1_1_1'
    assert var_util.gen_base_var_with_idx('PPFD_IN') == 'PPFD_IN_1_1_1'
    assert var_util.gen_base_var_with_idx('FC_SSITC_TEST') == \
        'FC_SSITC_TEST_1_1_1'


def test_remove_dup_filled_nonfilled_var(var_util):
    test_vars = ['SW_IN_F_1_1_1', 'SW_IN_1_1_1', 'TA_2_1_1', 'TA_F_1_2_1']
    test_vars2 = ['SW_IN_1_1_1', 'TA_2_1_1', 'TA_F_1_2_1']
    test_vars3 = ['SW_IN_PI_F_1_1_1', 'SW_IN_1_1_1', 'TA_2_1_1', 'TA_F_1_2_1']
    test_vars4 = ['SW_IN_PI_1_1_1', 'SW_IN_1_1_1', 'TA_2_1_1', 'TA_1_2_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars, rm_which='gap-filled') == [
        'SW_IN_1_1_1', 'TA_2_1_1', 'TA_F_1_2_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars, rm_which='non-filled') == [
        'SW_IN_F_1_1_1', 'TA_F_1_2_1', 'TA_2_1_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars2, rm_which='gap-filled') == [
        'SW_IN_1_1_1', 'TA_2_1_1', 'TA_F_1_2_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars2, rm_which='non-filled') == [
        'TA_F_1_2_1', 'SW_IN_1_1_1', 'TA_2_1_1']
    # the function doesn't handle _PI qualifiers but there should
    # not be _PI qualifiers hitting Data QAQC
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars3, rm_which='gap-filled') == [
        'SW_IN_1_1_1', 'TA_2_1_1', 'SW_IN_PI_F_1_1_1', 'TA_F_1_2_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars3, rm_which='non-filled') == [
        'SW_IN_PI_F_1_1_1', 'TA_F_1_2_1', 'SW_IN_1_1_1', 'TA_2_1_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars4, rm_which='gap-filled') == [
        'SW_IN_PI_1_1_1', 'SW_IN_1_1_1', 'TA_2_1_1', 'TA_1_2_1']
    assert var_util.remove_dup_filled_nonfilled_var(
        test_vars4, rm_which='non-filled') == [
        'SW_IN_PI_1_1_1', 'SW_IN_1_1_1', 'TA_2_1_1', 'TA_1_2_1']


def test_is_var_with_horiz_layer_aggregation(var_util):
    test_vars = ['TA_1', 'TA_F_1', 'TA_1_F', 'TA_1_1_1_F', 'TA_F_1_2_A',
                 'TA_F_2_3_1', 'TA_1_3_A', 'SW_IN_5', 'SW_IN_2_A',
                 'SW_IN_F_2', 'SW_IN_1F', 'CO2_4', 'FETCH_MAX', 'FETCH_70',
                 'FETCH_70_F', 'FETCH_70_1_1_1', 'FETCH_70_F_1_1_1',
                 'FETCH_MAX_5']
    assert not var_util.is_var_with_horiz_layer_aggregation('')
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[0])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[1])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[2])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[3])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[4])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[5])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[6])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[7])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[8])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[9])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[10])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[11])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[12])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[13])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[14])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[15])
    assert not var_util.is_var_with_horiz_layer_aggregation(test_vars[16])
    assert var_util.is_var_with_horiz_layer_aggregation(test_vars[17])


def test_tag_PI_var(var_util):
    test_vars = ['VPD', 'VPD_1_1_1', 'NEE_F', 'SW_IN', 'TA_F_1_1_1', 'RECO',
                 'RICO', 'GPP', 'RECO_5', 'RECO_1_2_A']
    assert var_util.tag_PI_var('') == ''
    assert var_util.tag_PI_var(test_vars[0]) == 'VPD_PI'
    assert var_util.tag_PI_var(test_vars[1]) == 'VPD_PI_1_1_1'
    assert var_util.tag_PI_var(test_vars[2]) == 'NEE_PI_F'
    assert var_util.tag_PI_var(test_vars[3]) == 'SW_IN'
    assert var_util.tag_PI_var(test_vars[4]) == 'TA_F_1_1_1'
    assert var_util.tag_PI_var(test_vars[5]) == 'RECO_PI'
    assert var_util.tag_PI_var(test_vars[6]) == 'RICO'
    assert var_util.tag_PI_var(test_vars[7]) == 'GPP_PI'
    assert var_util.tag_PI_var(test_vars[8]) == 'RECO_PI_5'
    assert var_util.tag_PI_var(test_vars[9]) == 'RECO_PI_1_2_A'


def test_tag_PI(var_util):
    test_vars = ['TA_1', 'TA_F_1', 'TA_1_F', 'TA_1_1_1_F', 'TA_F_1_2_A',
                 'TA_F_2_3_1', 'TA_1_3_A', 'SW_IN_5', 'SW_IN_2_A',
                 'SW_IN_F_2', 'SW_IN_1F', 'CO2_4', 'CO2_1_1_1', 'CO2',
                 'FH2O', 'VPD', 'VPD_1_1_1', 'NEE_F', 'SW_IN', 'TA_F_1_1_1',
                 'FETCH_70_F', 'FETCH_MAX_1_1_1']
    assert var_util.tag_PI('') == ''
    assert var_util.tag_PI(test_vars[0]) == 'TA_PI_1'
    assert var_util.tag_PI(test_vars[1]) == 'TA_PI_F_1'
    # Below will not happen if the variable is correctly caught in format
    assert var_util.tag_PI(test_vars[2]) == 'TA_1_PI_F'
    assert var_util.tag_PI(test_vars[3]) == 'TA_1_1_1_PI_F'
    assert var_util.tag_PI(test_vars[4]) == 'TA_PI_F_1_2_A'
    assert var_util.tag_PI(test_vars[5]) == 'TA_PI_F_2_3_1'
    assert var_util.tag_PI(test_vars[6]) == 'TA_PI_1_3_A'
    assert var_util.tag_PI(test_vars[7]) == 'SW_IN_PI_5'
    # Below will not happen if the variable is correctly caught in format
    assert var_util.tag_PI(test_vars[8]) == 'SW_PI_IN_2_A'
    assert var_util.tag_PI(test_vars[9]) == 'SW_IN_PI_F_2'
    assert var_util.tag_PI(test_vars[10]) == 'SW_IN_1F'
    assert var_util.tag_PI(test_vars[11]) == 'CO2_PI_4'
    assert var_util.tag_PI(test_vars[12]) == 'CO2_1_1_1'
    assert var_util.tag_PI(test_vars[13]) == 'CO2'
    assert var_util.tag_PI(test_vars[14]) == 'FH2O'
    assert var_util.tag_PI(test_vars[15]) == 'VPD_PI'
    assert var_util.tag_PI(test_vars[16]) == 'VPD_PI_1_1_1'
    assert var_util.tag_PI(test_vars[17]) == 'NEE_PI_F'
    assert var_util.tag_PI(test_vars[18]) == 'SW_IN'
    assert var_util.tag_PI(test_vars[19]) == 'TA_PI_F_1_1_1'
    assert var_util.tag_PI(test_vars[20]) == 'FETCH_70_PI_F'
    assert var_util.tag_PI(test_vars[21]) == 'FETCH_MAX_1_1_1'
