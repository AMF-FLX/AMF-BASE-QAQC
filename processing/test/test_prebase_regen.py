import json
from prebase_regen import PreBASERegenerator
from process_states import ProcessStateHandler
import pytest

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


@pytest.fixture
def pre_base_regen(monkeypatch):
    monkeypatch.setattr(ProcessStateHandler, '_qaqc_process_lookup',
                        mock_process_states_qaqc_process_lookup)

    return PreBASERegenerator()


def mock_process_states_qaqc_process_lookup(
        dummy, initiate_lookup=False):
    with open(file='./test/resources/state_cv_type.json',
              mode='r') as f:
        return json.load(f)


def test_get_incomplete_phase3_process_states(pre_base_regen):
    results = pre_base_regen.get_incomplete_phase3_process_states()
    assert results == (22, 23, 24, 25, 26)


def test_remove_any_duplicates(pre_base_regen):
    assert [1, 2, 3] == pre_base_regen.remove_any_duplicates(
        [1, 2, 3])
    assert [1, 2, 3] == pre_base_regen.remove_any_duplicates(
        [1, 1, 2, 3])
    assert [1] == pre_base_regen.remove_any_duplicates(
        [1, 1, 1])


def test_find_preBASE_state(pre_base_regen):
    # state_entry = (log_id, state_id, log_timestamp)
    status_history = [(441, 26, 'ts'), (401, 23, 'ts'),
                      (399, 22, 'ts'), (375, 20, 'ts'),
                      (333, 14, 'ts'), (330, 13, 'ts'),
                      (244, 12, 'ts')]

    assert pre_base_regen.find_preBASE_state(
        status_history) == 20

    # This case should not happen b/c a published BASE
    #  run should not be assessed.
    status_history = [(441, 15, 'ts'), (401, 23, 'ts'),
                      (399, 22, 'ts'), (375, 20, 'ts'),
                      (333, 14, 'ts'), (330, 13, 'ts'),
                      (244, 12, 'ts')]

    assert pre_base_regen.find_preBASE_state(
        status_history) == 15
