#!/usr/bin/env python

import collections
from jira_interface import JIRAInterface
# import os
from path_util import PathUtil
import pytest
from translate_earlyFP_base import TranslateEarlyBase

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


@pytest.fixture
def trans_early_base(monkeypatch):
    def mock_jira_get_organization(notsurewhatthisargisfor):
        return {
            'US-Ton': '207',
            'BR-Sa1': '78',
            'BR-Sa3': '103',
            'CA-Gro': '187'}

    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_jira_get_organization)

    def mock_path_util_create_valid_path(selfholder, path, sub_dir):
        return '/temp/fake/path_name/'

    monkeypatch.setattr(PathUtil, 'create_valid_path',
                        mock_path_util_create_valid_path)

    t = TranslateEarlyBase()
    if not t.var_info_map:
        t.var_info_map = {'BR-Sa1':
                          {'CO2_2': 'CO2_1_2_1', 'CO2_1': 'CO2_1_1_1',
                           'FC': 'FC', 'GPP_PI': 'GPP', 'H': 'H',
                           'H2O': 'H2O_1_2_1', 'LE': 'LE',
                           'NEE_PI': 'NEE', 'PPFD_IN': 'PPFD_IN',
                           'P': 'P', 'PA': 'PA', 'RECO_PI': 'RECO',
                           'RH': 'RH', 'NETRAD': 'NETRAD', 'SC': 'SC',
                           'TA': 'TA_1_2_1', 'USTAR': 'USTAR',
                           'VPD_PI': 'VPD', 'WD': 'WD', 'WS': 'WS'
                           },
                          'BR-Sa3':
                          {'RH': 'RH', 'NETRAD': 'NETRAD', 'SC': 'SC',
                           'CO2_1': 'CO2', 'FC': 'FC', 'G': 'G',
                           'GPP_PI': 'GPP', 'H': 'H', 'H2O': 'H2O',
                           'LE': 'LE', 'NEE_PI': 'NEE',
                           'PPFD_IN': 'PPFD_IN', 'PPFD_OUT': 'PPFD_OUT',
                           'PA': 'PA', 'RECO_PI': 'RECO', 'P': 'P',
                           'SW_IN': 'SW_IN', 'LW_IN': 'LW_IN',
                           'LW_OUT': 'LW_OUT', 'SW_OUT': 'SW_OUT',
                           'SWC_1': 'SWC_1_1_1', 'SWC_2': 'SWC_1_2_1',
                           'TA': 'TA_1_1_1', 'TS_1': 'TS_1_1_1',
                           'TS_2': 'TS_1_2_1', 'USTAR': 'USTAR',
                           'VPD_PI': 'VPD', 'WD': 'WD', 'WS': 'WS_1_1_2'
                           },
                          'CA-Gro':
                          {'CO2_1': 'CO2_1_1_1', 'CO2_2': 'CO2_1_2_1',
                           'GPP_PI': 'GPP', 'P': 'P_1_1_1',
                           'RECO_PI': 'RECO', 'FC': 'FC', 'G': 'G_1_1_1',
                           'H': 'H', 'H2O': 'H2O', 'LE': 'LE',
                           'NEE_PI': 'NEE', 'PPFD_IN': 'PPFD_IN_1_1_1',
                           'PPFD_DIF': 'PPFD_DIF', 'PPFD_OUT': 'PPFD_OUT',
                           'PA': 'PA', 'SW_IN': 'SW_IN', 'LW_IN': 'LW_IN',
                           'LW_OUT': 'LW_OUT', 'SW_OUT': 'SW_OUT',
                           'RH': 'RH_1_1_1', 'NETRAD': 'NETRAD_1_1_1',
                           'SC': 'SC_1_1_1', 'SH': 'SH', 'SLE': 'SLE',
                           'SWC_1': 'SWC_1_1_1', 'SWC_2': 'SWC_1_2_1',
                           'TA': 'TA_1_1_1', 'TS_1': 'TS_1_1_1',
                           'TS_2': 'TS_1_2_1', 'USTAR': 'USTAR',
                           'VPD_PI': 'VPD', 'WD': 'WD_1_1_1',
                           'WS': 'WS_1_1_1'
                           }
                          }
        if not t.site_list:
            t.site_list = ['BR-Sa1']
    return t


def test_get_existing_site_base_issue_info(trans_early_base, monkeypatch):

    SiteInfo = collections.namedtuple(
        'SiteInfo', 'site_id process_id issue_id base_file upload_file '
        'new_process_id new_issue_id comment')

    def mock_jira_run_base_query(base_query, count):
        return {
            'maxResults': 50,
            'issues': [{
                'key': 'TESTQAQC-1590',
                'fields': {
                    'customfield_10203': '5294',
                    'customfield_10206': 'BR-Sa1'
                }
            },
                {
                'key': 'TESTQAQC-1589',
                'fields': {
                    'customfield_10203': '5293',
                    'customfield_10206': 'CA-Gro'
                }
            }]
        }

    monkeypatch.setattr(trans_early_base.jira, 'run_query',
                        mock_jira_run_base_query)

    def mock_get_filename_for_process(process_id):
        if process_id == '5294':
            return 'G:\\AmeriFluxUploads\\BR-Sa1\\BR-Sa1_HR_' \
                   '200201010000_201201010000-2017082408485882.csv'
        elif process_id == '5293':
            return 'G:\\AmeriFluxUploads\\CA-Gro\\CA-Gro_HH_' \
                   '200301010000_201501010000-2017082408495041.csv'
        else:
            raise Exception('http://roz.lbl.gov:8080/QAQC/BaseReprocess/{p} '
                            'returned status code 404'.format(p=process_id))

    monkeypatch.setattr(trans_early_base, 'get_filename_for_process',
                        mock_get_filename_for_process)

    trans_early_base.get_existing_site_base_issue_info()

    site_dict_results = {
        'BR-Sa1': SiteInfo(
            site_id='BR-Sa1', process_id='5294', issue_id='TESTQAQC-1590',
            base_file=None,
            upload_file='G:\\AmeriFluxUploads\\BR-Sa1\\BR-Sa1_'
                        'HR_200201010000_201201010000-2017082408485882.csv',
            new_process_id=None, new_issue_id=None, comment=None),
        'CA-Gro': SiteInfo(
            site_id='CA-Gro', process_id='5293', issue_id='TESTQAQC-1589',
            base_file=None,
            upload_file='G:\\AmeriFluxUploads\\CA-Gro\\CA-Gro_HH_'
                        '200301010000_201501010000-2017082408495041.csv',
            new_process_id=None, new_issue_id=None, comment=None)
    }

    assert list(trans_early_base.site_dict.keys()) == ['BR-Sa1']
    assert trans_early_base.site_dict['BR-Sa1'] == site_dict_results['BR-Sa1']


def test_get_site_list(trans_early_base):
    args = collections.namedtuple(
        'Namespace', 'defined_query sites skip_sites var_info_date')
    test_args = args(defined_query=None, sites='BR-Sa1,CA-Gro',
                     skip_sites='CA-Gro,US-Ton', var_info_date='YYYYMMDD')
    assert trans_early_base.get_site_list(test_args) == ['BR-Sa1']
