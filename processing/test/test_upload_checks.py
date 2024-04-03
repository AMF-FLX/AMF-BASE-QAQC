from pathlib import Path
from upload_checks import upload_checks
import getpass
import json
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_getpass_getuser():
    return 'unittester'


def parse_json(filepath):
    var_values = []
    ids = []

    with open(file=filepath, mode='r') as f:
        cases = json.load(f)

        for key, details in cases.items():
            var_values.append((key,
                               details.get('run_type'),
                               details.get('output')))
            ids.append(key.split('.')[0])

    return var_values, ids


var_values, ids = parse_json(
    filepath='./test/testdata/format_qaqc/integration_cases.json')


@pytest.mark.parametrize('filename, run_type, output', var_values, ids=ids)
def test_upload_checks(filename, run_type, output, capsys, monkeypatch):

    monkeypatch.setattr(getpass, 'getuser', mock_getpass_getuser)

    filepath = Path().cwd() / 'test/testdata/format_qaqc' / filename
    process_id, is_upload_successful, upload_uuid = upload_checks(
        filename=str(filepath), upload_id=99999999, run_type=run_type,
        site_id='US-UMB', prior_process_id=None, zip_process_id=None,
        local_run=True)
    captured = capsys.readouterr()
    # Capture the print statements from the case; reads from the last read
    print_output = captured.out
    # Convert captured print statements into a list based on line breaks
    #   Example print list -- for last entries, see print statements
    #   at end of upload_checks function if local_run=True (~ln=355)
    #   [ ...,  # we don't care what is written at beginning of list
    #     <json_report>,
    #     <YYYY-MM-DD HH:MM or None>,
    #     <YYYY-MM-DD HH:MM or None>,
    #     ''
    #   ]
    print_list = print_output.split('\n')

    assert process_id == 999999
    assert is_upload_successful is output.get('is_upload_successful')

    expected_upload_uuid = output.get('upload_uuid')
    if expected_upload_uuid is not None:
        assert upload_uuid == expected_upload_uuid
    else:
        assert upload_uuid is None

    # last item is empty str
    ts_end_expected = output.get('data_timestamp_end')
    ts_start_expected = output.get('data_timestamp_start')

    if ts_end_expected is not None:
        assert print_list[-2] == output.get('data_timestamp_end')
    else:
        assert print_list[-2] == 'None'

    if ts_start_expected is not None:
        assert print_list[-3] == output.get('data_timestamp_start')
    else:
        assert print_list[-3] == 'None'

    report_expected = json.dumps(output.get('report'))
    assert print_list[-4] == report_expected
