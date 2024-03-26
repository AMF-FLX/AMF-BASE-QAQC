from db_handler import NewDBHandler


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
    def __init__(self, initialize_lookup=True):

        if initialize_lookup:
            self.lookup = self._qaqc_process_lookup()
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
    def _qaqc_process_lookup():
        return NewDBHandler().get_qaqc_state_types()

    def get_process_state(self, state_name: ProcessStates):
        return self.lookup.get(state_name)
