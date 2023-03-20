import pytest
from amf_utils.flux_vars.utils import VarUtils

__author__ = 'You-Wei Cheah'
__email__ = 'ycheah@lbl.gov'


@pytest.fixture
def var_utils():
    return VarUtils()


@pytest.fixture
def var_utils_with_known_vars():
    return VarUtils(('FETCH_70', 'FETCH_80', 'H', 'H_SSITC_TEST', 'P',
                     'VPD', 'RECO'))


def test_parse_var_with_known_vars(var_utils_with_known_vars):
    results = var_utils_with_known_vars.parse_var('FETCH_80')
    assert results.base_var == 'FETCH_80'
    for attr in (results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('H__1_1_1')
    assert results.is_invalid is True

    for attr in (results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index,
                 results.aggregation_layer_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('PO_1_1_1')
    assert results.base_var == 'P'
    assert results.horizontal_index == 1
    assert results.vertical_index == 1
    assert results.replicate_index == 1
    assert results.aggregation_layer_index is None
    assert results.is_invalid is True

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('H_ABC_PI_76_45_125')
    assert results.base_var == 'H'
    assert results.horizontal_index == 76
    assert results.vertical_index == 45
    assert results.replicate_index == 125
    assert results.is_PI_provided is True
    assert results.aggregation_layer_index is None
    assert results.is_invalid is True

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('H_80')
    assert results.base_var == 'H'
    assert results.aggregation_layer_index == 80
    for attr in (results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('FETCH_90')
    assert results.is_invalid is True

    for attr in (results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index,
                 results.aggregation_layer_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('P_PI_F')
    assert results.base_var == 'P'
    assert results.is_gap_filled is True
    assert results.is_PI_provided is True

    for attr in (results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('FETCH_70_1_1_1')
    assert results.base_var == 'FETCH_70'
    assert results.aggregation_layer_index is None
    for attr in (results.horizontal_index,
                 results.vertical_index,
                 results.replicate_index):
        assert attr == 1

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('FETCH_80_F')
    assert results.base_var == 'FETCH_80'
    assert results.is_gap_filled is True

    for attr in (results.horizontal_index,
                 results.vertical_index,
                 results.aggregation_layer_index,
                 results.replicate_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils_with_known_vars.parse_var('H_SSITC_TEST_PI')
    assert results.base_var == 'H_SSITC_TEST'
    assert results.is_PI_provided is True

    for attr in (results.horizontal_index,
                 results.vertical_index,
                 results.aggregation_layer_index,
                 results.replicate_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False


def test_parse_var(var_utils):
    ###########################################################################
    results = var_utils.parse_var('P_100_95_195')
    assert results.base_var == 'P'
    assert results.horizontal_index == 100
    assert results.vertical_index == 95
    assert results.replicate_index == 195
    assert results.aggregation_layer_index is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_standard_deviation,
                 results.is_PI_provided,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('CO2_PI_76_45_125')
    assert results.base_var == 'CO2'
    assert results.horizontal_index == 76
    assert results.vertical_index == 45
    assert results.replicate_index == 125
    assert results.is_PI_provided is True
    assert results.aggregation_layer_index is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('LW_IN_100_95_A')
    assert results.base_var == 'LW_IN'
    assert results.horizontal_index == 100
    assert results.vertical_index == 95
    assert results.is_aggregate is True

    for attr in (results.aggregation_layer_index,
                 results.replicate_index):
        assert attr is None

    for attr in (results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('SW_IN_77_95_A_SD')
    assert results.base_var == 'SW_IN'
    assert results.horizontal_index == 77
    assert results.vertical_index == 95
    assert results.is_aggregate is True
    assert results.is_standard_deviation is True

    for attr in (results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('SW_IN_F_77_95_A_SD')
    assert results.base_var == 'SW_IN'
    assert results.horizontal_index == 77
    assert results.vertical_index == 95
    assert results.is_aggregate is True
    assert results.is_gap_filled is True
    assert results.is_standard_deviation is True

    for attr in (results.replicate_index,
                 results.aggregation_layer_index):
        assert attr is None

    for attr in (results.is_number_of_samples,
                 results.is_PI_provided,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('AA_BLAH_BLAH_F_77_A')
    assert results.is_invalid is True

    for attr in (results.base_var,
                 results.horizontal_index,
                 results.vertical_index,
                 results.replicate_index,
                 results.aggregation_layer_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('SW_IN_1234')
    assert results.base_var == 'SW_IN'
    assert results.aggregation_layer_index == 1234

    for attr in (results.horizontal_index,
                 results.vertical_index,
                 results.replicate_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('LW_BC_OUT_F')
    assert results.base_var == 'LW_BC_OUT'
    assert results.is_gap_filled is True
    for attr in (results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('AA_BLAH_BLAH_QC_F_77_77_77')
    assert results.base_var == 'AA_BLAH_BLAH'
    assert results.horizontal_index == 77
    assert results.vertical_index == 77
    assert results.replicate_index == 77
    assert results.aggregation_layer_index is None

    for attr in (results.is_aggregate,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units):
        assert attr is False

    for attr in (results.is_gap_filled,
                 results.has_QC_flag):
        assert attr is True

    ###########################################################################
    results = var_utils.parse_var('SPEC_PRI_REF_IN')
    assert results.base_var == 'SPEC_PRI_REF_IN'
    for attr in (results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('FETCH_70_F_1_1_1')
    for attr in (results.base_var,
                 results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    assert results.is_invalid is True

    ###########################################################################
    results = var_utils.parse_var('NO2_1_1_1_')
    for attr in (results.base_var,
                 results.aggregation_layer_index,
                 results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    assert results.is_invalid is True

    ###########################################################################
    results = var_utils.parse_var('CH4_7_N')
    assert results.base_var == 'CH4'
    assert results.aggregation_layer_index == 7
    assert results.is_number_of_samples is True

    for attr in (results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_inst_units,
                 results.has_QC_flag):
        assert attr is False

    ###########################################################################
    results = var_utils.parse_var('P_IU_1')
    assert results.base_var == 'P'
    assert results.aggregation_layer_index == 1
    assert results.has_inst_units is True

    for attr in (results.horizontal_index,
                 results.replicate_index,
                 results.vertical_index):
        assert attr is None

    for attr in (results.is_aggregate,
                 results.is_gap_filled,
                 results.is_invalid,
                 results.is_number_of_samples,
                 results.is_PI_provided,
                 results.is_standard_deviation,
                 results.has_QC_flag):
        assert attr is False


def test_tag_PI_for_BASE_var(var_utils_with_known_vars):
    result = var_utils_with_known_vars.tag_PI_for_BASE_var('VPD')
    assert result == 'VPD_PI'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('RECO_5_1_7')
    assert result == 'RECO_PI_5_1_7'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('VPD_QC_F_IU_1_1_1')
    assert result == 'VPD_PI_QC_F_IU_1_1_1'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var(
        'VPD_F_SD_IU_1_1_1_N')
    assert result is None

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('VPD_F_1_1_1')
    assert result == 'VPD_PI_F_1_1_1'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('P_1_1_1')
    assert result == 'P_1_1_1'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('FETCH_70_1_N')
    assert result == 'FETCH_70_PI_1_N'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('H_3_SD')
    assert result == 'H_PI_3_SD'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('H_PI_F')
    assert result == 'H_PI_F'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('P_F')
    assert result == 'P_PI_F'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('P_3')
    assert result == 'P_PI_3'

    result = var_utils_with_known_vars.tag_PI_for_BASE_var('FETCH_70')
    assert result == 'FETCH_70'
