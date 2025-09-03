# import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def test_get_sites_with_last_process_type():
    """ Test cases: AMP qaqc tables
        1. Format only
        2. Format last with unresolved data issue
        3. Format last with resolved data issue
        4. Site last with format next
        5. Site last with data next
        6. Site_id = other -- should be filtered out
        7. Site_id = None -- should be filtered out
        8. Mix of sites and above states
    """
    pass


def test_get_qaqc_jira_issue_info():
    """ Test cases: Jira DB
        1. Make sure that all fields are returned
    """
    pass


def test_get_last_flux_upload():
    """ Test cases: AMP tables
        1. one flux upload
        2. multiple flux uploads -- make sure last date is returned
        3. multiple files in an upload -- make sure last date is returned
    """
    pass
