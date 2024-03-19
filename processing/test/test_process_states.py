from process_states import ProcessStates, ProcessStateHandler

import json

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def mock_process_states_qaqc_process_lookup(
        dummy, initiate_lookup=False):
    with open(file='./test/resources/state_cv_type.json',
              mode='r') as f:
        return json.load(f)


def test_init():

    process_states = ProcessStateHandler(initialize_lookup=False)

    assert process_states.lookup == {}
    assert process_states.base_candidate_states == [
        'Passed by Curator', 'BASE Generated',
        'BASE Generation Failed', 'BASE-BADM Update Failed',
        'Regenerated preBASE']
    assert process_states.incomplete_phase3_states == [
        'BASE Generated', 'BASE-BADM Updated',
        'BASE Generation Failed', 'BASE-BADM Update Failed',
        'BASE-BADM Publish Failed']


def test_get_process_state(monkeypatch):
    monkeypatch.setattr(ProcessStateHandler, '_qaqc_process_lookup',
                        mock_process_states_qaqc_process_lookup)

    process_states = ProcessStateHandler()

    assert process_states.get_process_state(
        ProcessStates.Uploaded) == 1
    assert process_states.get_process_state(
        ProcessStates.StartBASEGen) == 12

    assert process_states.get_process_state('test') is None
