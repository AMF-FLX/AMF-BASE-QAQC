import os

from data_report_gen import DataReportGen
from jira_interface import JIRAInterface
from site_attrs import SiteAttributes

__author__ = 'Sy-Toan Ngo, Danielle Christianson'
__email__ = 'sytoanngo@lbl.gov, dschristianson@lbl.gov'


def mock_jira_get_organization(notsurewhatthisargisfor):
    return {
        'US-Ton': '207',
        'CA-DBB': '322',
        'US-MOz': '6',
        'US-PFa': '166'}


def mock_site_attrs_get_site_dict(dummyselfvariable):
    return {'test-Site': 'Testing location'}


def mock_read_sites_file(dummyself, cwd, filepath):
    return ['CC-sss']


def test_gen_message(monkeypatch):
    """
    Using driver to create and then test craft_email
    """

    monkeypatch.setattr(JIRAInterface, 'get_organizations',
                        mock_jira_get_organization)

    monkeypatch.setattr(SiteAttributes, 'get_site_dict',
                        mock_site_attrs_get_site_dict)

    monkeypatch.setattr(DataReportGen, '_read_sites_file',
                        mock_read_sites_file)

    data_report_gen = DataReportGen()
    site_id = 'test-Site'
    issue_key = 'QAQC-####'
    site_dict = {
        'participant_names': ['participant_0', 'participant_1'],
        'reporter_name': 'reporter_0',
        'process_ids': ['id_0', 'id_1']}
    format_filenames = ['test-Site_HH_startdate0_enddate0.csv',
                        'test-Site_HH_startdate2_enddate2.csv']
    format_process_ids = ['id_0', 'id_2']
    ftp_link = 'ftp_link'
    ui_url = data_report_gen.ui_prefix
    instruction_link = data_report_gen.instruction_link
    report_link = data_report_gen.report_link_template.replace(
        os.path.basename(
            data_report_gen.report_link_template), '')

    # AMP review case
    msg_body = data_report_gen.gen_message(
        site_id, False, issue_key, site_dict,
        format_filenames, format_process_ids, ftp_link)
    known_msg_body = detail_results.get(
        'msg_body_amp_review').format(ui_url=ui_url,
                                      report_link=report_link)
    assert msg_body == known_msg_body

    # Self-review case
    msg_body = data_report_gen.gen_message(
        site_id, True, issue_key, site_dict,
        format_filenames, format_process_ids, ftp_link)
    known_msg_body = detail_results.get(
        'msg_body_self_review').format(ui_url=ui_url,
                                       report_link=report_link,
                                       instruction_link=instruction_link
                                       )
    assert msg_body == known_msg_body


detail_results = {
    "msg_body_amp_review":
        "Dear reporter_0, participant_0, participant_1,"
        '\n\nThank you for your data '
        'submissions for test-Site (Testing location).'
        '\n\nThis report summarizes the results of '
        'AmeriFlux Data QA/QC, which provides '
        'an independent analysis of your data '
        'and helps identify potential issues in '
        'data formats and contents.'
        '\n\nBriefly, Data QA/QC includes the '
        'inspection of sign conventions, ranges, '
        'diurnal-seasonal patterns, and potential '
        'outliers of variables. Multivariate relations '
        '(e.g., WS vs USTAR, PPFD_IN vs SW_IN) are '
        'also analyzed to detect potentially erroneous '
        'data. The comparison of measured radiation '
        '(e.g., PPFD_IN, SW_IN) to the maximum, top '
        'of the atmosphere radiation expected for a '
        'given location (i.e., SW_IN_POT) is also '
        'analyzed to check the timestamp specification '
        'and alignment. Details of each Data QA/QC '
        'check module can be found at [Data QA/QC|{ui_url}'
        'data/flux-data-products/data-qaqc/].'
        '\n\nIn analyzing your data, we '
        'have the following questions where we request '
        'your expert opinion and suggestion. '
        'Please note that some issues we identify '
        'could be normal and expected at your site. '
        'Please verify, clarify, or correct the '
        'following issues before we can make your data '
        'available as an AmeriFlux BASE data product. '
        'To resubmit a corrected version, '
        'please upload files at [Upload Data|'
        '{ui_url}data/upload-data/].\n\n'
        '*[Data QA/QC]*\n\n'
        '< INSERT_data_qaqc_content >\n\n'
        'We hope that this will not take too much time '
        'from your work, but it will help to make your '
        'data more robust and clear. You can view '
        'the AmeriFlux QA/QC processing status for '
        'all your sites at [Data Processing Status|{ui_url}'
        'sites/data-processing-status/] (login required).'
        '\n\nPlease reply to this '
        'email with any questions. You can track '
        'communications on this Data QA/QC at QAQC-#### '
        'using your AmeriFlux user ID and password '
        'to login.'
        '\n\nBest regards and '
        'thanks for the collaboration,\nAmeriFlux '
        'Data Team\n\n----------------------------------'
        '-------'
        '\n\nFTP link to Data QA/QC, where you can '
        'access all figures and intermediate '
        'files generated during Data QA/QC:\nftp_link'
        '\n\nFormat QA/QC reports associated with '
        'this Data QA/QC, where you can glance at '
        'the file sources used in this Data QA/QC:\n'
        'test-Site_HH_startdate0_enddate0.csv ([online report id_0|'
        '{report_link}?site_id=test-Site&report_id=id_0])\n'
        'test-Site_HH_startdate2_enddate2.csv ([online report id_2|'
        '{report_link}?site_id=test-Site&report_id=id_2])',
    "msg_body_self_review":
        "Dear reporter_0, participant_0, participant_1,"
        '\n\nThank you for your data '
        'submissions for test-Site (Testing location).'
        '\n\nThe summary statistics and figures, generated by the AmeriFlux '
        'data pipeline (described at [Flux Data Products|{ui_url}'
        'data/flux-data-products/]), are ready for your team’s review.'
        '\n\n*Step 1:* As needed, review detailed instructions and specific '
        'QA/QC check information in the [technical document|'
        '{instruction_link}] _(pdf)_.'
        '\n\n*Step 2:* Review the summary statistics and/or figures for '
        'each QA/QC check:'
        '\n* [Variable Coverage|ftp_link/variable_coverage/'
        'test-Site-variable_coverage-by_year.png] _(png)_'
        '\n* [Timestamp Alignment|ftp_link/summary/'
        'timestamp_alignment_summary.csv] _(csv)_'
        '\n* [Physical Range (Percent-Ratio)|ftp_link/summary/'
        'physical_range_percent_ratio_summary.csv] _(csv)_'
        '\n* [Physical Range|ftp_link/summary/'
        'physical_range_summary.csv] _(csv)_'
        '\n* [Multivariate Comparison|ftp_link/summary/'
        'multivariate_comparison_summary.csv] _(csv)_'
        '\n* [Diurnal-Seasonal Pattern|ftp_link/summary/'
        'diurnal_seasonal_pattern_summary.csv] _(csv)_'
        '\n* [USTAR Filtering|ftp_link/summary/'
        'ustar_filtering_summary.csv] _(csv)_'
        '\n\nNote that certain QA/QC checks may not run if the target '
        'variable(s) or required info are absent. Data QA/QC assessment '
        'is performed on the entire data record. We recommend focusing your '
        'review on newly uploaded data; review of previously published '
        'data is optional.\n\nThe summary files linked above can also be '
        'accessed via the online output [summary directory|ftp_link/summary].'
        '\n\n*Step 3 - option A: If corrections are needed*'
        '\nCorrect any identified issues. Upload corrected files at '
        '[Upload Data|{ui_url}data/upload-data/].'
        '\n\n*Step 3 - option B: Data are ready for publication as '
        'AmeriFlux BASE*\nReply to this email to confirm that the data are '
        'ready for publication as AmeriFlux BASE. Provide brief clarification '
        'for any issues detected (FAIL or WARNING in the summary statistics) '
        'that are acceptable for your site.'
        '\n\n*Questions? Clarifications?* Reply to this email. You can view '
        'the AmeriFlux BASE QA/QC processing status for all your sites at '
        '[Data Processing Status|'
        '{ui_url}sites/data-processing-status/] (login required).'
        '\n\nBest regards and '
        'thanks for the collaboration,\nAmeriFlux '
        'Data Team\n\n----------------------------------'
        '-------\n*Additional Information*'
        '\n\nBriefly, Data QA/QC includes the '
        'inspection of sign conventions, ranges, '
        'diurnal-seasonal patterns, and potential '
        'outliers of variables. Multivariate relations '
        '(e.g., WS vs USTAR, PPFD_IN vs SW_IN) are '
        'also analyzed to detect potentially erroneous '
        'data. The comparison of measured radiation '
        '(e.g., PPFD_IN, SW_IN) to the maximum, top '
        'of the atmosphere radiation expected for a '
        'given location (i.e., SW_IN_POT) is also '
        'analyzed to check the timestamp specification '
        'and alignment. Details of each Data QA/QC '
        'check module can be found at [Data QA/QC|{ui_url}'
        'data/flux-data-products/data-qaqc/].'
        '\n\nThe full output with summary statistic files and figures '
        'is available in the online [output directory|ftp_link].'
        '\n\nFormat QA/QC reports associated with '
        'this Data QA/QC, where you can glance at '
        'the file sources used in this Data QA/QC:\n'
        'test-Site_HH_startdate0_enddate0.csv ([online report id_0|'
        '{report_link}?site_id=test-Site&report_id=id_0])\n'
        'test-Site_HH_startdate2_enddate2.csv ([online report id_2|'
        '{report_link}?site_id=test-Site&report_id=id_2])'
}
