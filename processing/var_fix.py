import re
from data_reader import DataReader
from fp_vars import FPVariables

"""
Take in headers
    get base header name
    check if its in var_names
        Yes=return get_base_header
        No:
            remove (3M), (-6), (-8cm), etc.
            remove .3m., .10cm , etc.
            remove _3CM, _10m, etc
            remove . at end of string
            remove ..8, etc
            remove Hemlock., HEMLOCK_ (Only occures in Ha2 so far)
            Check if base_header in var_names
                Yes=Return get_base_header
                No:
                    Var in var_fix_dict keys?
                    No:
                        "ERROR!!"


Return edited headers
"""
__author__ = 'Fianna O''Brien, You-Wei Cheah'
__email__ = 'flobrien@lbl.gov, ycheah@lbl.gov'


class VarFixer:
    def __init__(self):
        self.var_fix_dict = {
            # "APARPCT": "APAR",
            # "CO2TOP": "CO2",
            # "FCO2_NO_USTAR_FILTER": "FC",
            # "FH2O": "LE",
            # "G1_1_1_F": "G_1_1_F",
            # "GEE.ESTIMATE": "G",
            "ALBEDO": "ALB",
            "PAR_DIFF": "PPFD_DIF",
            "SIGMA_V": "V_SIGMA",
            "SIGMA_W": "W_SIGMA",
            "SONIC.TAIR": "T_SONIC",
            "SW_DF": "SW_DIF",
            "TSOIL": "TS",
            "U*": "USTAR",
            "WIND.DIRECTION": "WD",
            "TIMESTAMP": "TIMESTAMP_START",
            "START_TIME": "TIMESTAMP_START",
            "START_TIMESTAMP": "TIMESTAMP_START",
            "END_TIME": "TIMESTAMP_END",
            "END_TIMESTAMP": "TIMESTAMP_END",
            "TIME_START": "TIMESTAMP_START",
            "TIME_END": "TIMESTAMP_END"}

        self.reader = DataReader()
        self.var_dict = FPVariables().get_fp_vars_dict()

    def fix_header(self, variable):
        var_base_header = self.reader.get_base_header(variable).upper()
        if self.var_dict.get(var_base_header):
            return True, var_base_header
        else:
            # Why is this needed?
            string = variable
            string = re.sub(r'\(-*\d+(c|C)*(m|M)*\)', '', string)
            string = re.sub(r'\.\d+(c|C)*(m|M)\.*', '', string)
            string = re.sub(r'_\d+(C|c)*(m|M)', '', string)
            string = re.sub(r'\.$', '', string)
            string = re.sub(r'\.\.\d', '', string)
            string = re.sub(r'(Hemlock|HEMLOCK)(\.|_)', '', string)

            if var_base_header in self.var_fix_dict.keys():
                return True, self.var_fix_dict.get(var_base_header)
            else:
                return False, (
                    "ERROR: {} header ".format(variable)
                    + "could not be formatted to fit FP-IN")
