import json
import urllib.request

from configparser import ConfigParser
from pathlib import Path
from urllib.error import HTTPError


class ProcessStates:

    Uploaded = 'Uploaded'
    PassedQAQC = 'Passed Format QAQC'
    IssuesFound = 'Format Issues Found'
    FailedQAQC = 'Failed Format QAQC'
    FailedRepair = 'Failed Format AutoRepair'
    FailedRepairRetire = 'Unrepairable File Retired'
    AutoRepair = 'Replaced with AutoRepaired File'
    AutoRepairRetire = 'Repairable File Retired'
    FinishedQAQC = 'Finished QAQC'
    StartBASEGen = 'Start BASE Generation'
    FilesCombined = 'Files Combined'
    PublishedBASEBADM = 'BASE-BADM Published'
    CombinerFailed = 'Combiner Failed'
    PassedCurator = 'Passed by Curator'
    RepublishReport = 'Report Generation Failed'
    GeneratedBASE = 'BASE Generated'
    UpdatedBASEBADM = 'BASE-BADM Updated'
    BASEGenFailed = 'BASE Generation Failed'
    BADMUpdateFailed = 'BASE-BADM Update Failed'
    BASEBADMPubFailed = 'BASE-BADM Publish Failed'
    ArchiveUploaded = 'Archive Contents Uploaded'
    RetiredForReprocessing = 'Retired for Reprocessing'
    InitiatedPreBASERegen = 'Regenerated preBASE'


class ProcessStateHandler:
    def __init__(self, initialize_lookup=True,
                 cfg_filename='qaqc.cfg'):

        if initialize_lookup:
            self.lookup = self._qaqc_process_lookup(cfg_filename)
        else:
            self.lookup = {}

        self.base_candidate_states = [
            ProcessStates.PassedCurator,
            ProcessStates.GeneratedBASE,
            ProcessStates.BASEGenFailed,
            ProcessStates.BADMUpdateFailed,
            ProcessStates.InitiatedPreBASERegen]

        self.incomplete_phase3_states = [
            ProcessStates.GeneratedBASE,
            ProcessStates.UpdatedBASEBADM,
            ProcessStates.BASEGenFailed,
            ProcessStates.BADMUpdateFailed,
            ProcessStates.BASEBADMPubFailed]

    @staticmethod
    def _qaqc_process_lookup(cfg_filename):
        qaqc_state_cv = None
        with open(Path.cwd() / cfg_filename) as cfg:
            config = ConfigParser()
            config.read_file(cfg)
            cfg_section = 'WEBSERVICES'
            if config.has_section(cfg_section):
                qaqc_state_cv = config.get(cfg_section,
                                           'qaqc_state_cv')
        if not qaqc_state_cv:
            raise Exception('QAQC processing state '
                            'webservice not configured.')
        try:
            resp = urllib.request.urlopen(qaqc_state_cv)
            return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            err_msg = e.read().decode('utf-8')
            raise Exception(f'{qaqc_state_cv} returned '
                            f'status code {e.code}\n{err_msg}')

    def get_process_state(self, state_name: ProcessStates):
        return self.lookup.get(state_name)
