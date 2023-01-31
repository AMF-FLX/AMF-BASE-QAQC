#!/usr/bin/env python
import status
import numpy as np
from logger import Logger

__author__ = 'danielle_christianson'
__email__ = 'dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class SSITC_FF_check():  # Rename to algorithm name
    """This algorithm:

    For SSITC check:
    1) Checks to to see if _SSITC extension exists
    2) If not, nothing happens and status message reported
    3) If so,
       a) corresponding variable with _SSITC values of 2 are removed
       b  stats are calculated: fractions with 1 and 2 reported
       c) an array with color values are generated for making a
          figure to describe data quality
       d) figure generated

    For FETCH_FILTER (FF):
    1) Checks to see if FF variable exists
    2) If not, nothing happens and status message reported
    3) If so,
       a) corresponding variable with _SSITC values of 1 are removed
       b  stats are calculated: fractions removed
       c) an array with color values are generated for making a
          figure to describe data quality
       d) figure generated
    """

    def __init__(self):
        _log.info("Loading.")
        """ Initialize variables on loading of class here """
        self.SSITCvars = []  # Holds a list of variable headers with SSITC
        self.SSITCovars = []  # List of non-SSITCvars
        self.SSITCvd = {}  # Var dict that connects SSITC vars to the data vars
        self.SSITCcolors = None  # Rec array with colors matching data
        self.FFvars = []
        self.FFcolors = None

    def SSITC_vars_check(self, header):
        '''
        Determines if SSITC variables exist. If so, finds affected variables.
        :param header: the header returned by the DataReader
        '''
        for i in header:  # step thru header names to find any SSITC vars
            if "_SSITC" in i:
                self.SSITCvars.append(i)   # build the list of SSITC variables
                if i not in self.SSITCvd:
                    self.SSITCvd[i] = []   # build the dictionary of SSITC vars
            else:
                self.SSITCovars.append(i)  # build a list of non-SSITC vars

        if not self.SSITCvars:
            _log.info('No SSITC variables -- SSITC check COMPLETE')
            return False

        # Loop thru non-SSITC vars to find the match to the SSITC var
        for i in self.SSITCovars:
            ib = self.d.get_base_header(i)   # get the base header name
            dv = []   # set a variable to hold the SSITC variable
            # Find the SSITC variable entry that has same base name
            for j in self.SSITCvars:
                if ib in j:
                    dv.append(j)
            dv_len = len(dv)
            if dv_len == 0:
                _log.info('Problem creating SSITC dictionary: no SSITC var'
                          ' -- SSITC check NOT complete')
            elif dv_len == 1:
                self.SSITCvd[dv[0]].append(i)  # add ith to dictionary list
            else:
                _log.info('Problem creating SSITC dictionary: '
                          'too many SSITC vars'
                          ' -- SSITC check NOT complete')
        return True

    def FF_vars_check(self, header):
        """
        Determines if FETCH_FILTER variable exists and if so,
        which variables are affected.
        :param header: the header returned by the DataReader
        """
        ff = False

        for i in header:  # step thru header names to determine if FF exists
            if "FETCH_FILTER" in i:
                ff = True

        if ff:   # if FETCH_FILTER is in the data, do the following:
            for i in header:
                ib = self.d.get_base_header(i)  # get the base header name
                if ib in ("FC", "FCH4", "FNO", "FNO2",
                          "FN2O", "FO3", "LE", "H"):
                    self.FFvars.append(i)
        # If FETCH_FILTER is not in the data, send info
        # message that check is complete
        else:
            _log.info(
                'No FETCH_FILTER variable -- FETCH_FILTER check COMPLETE')

        return ff

    def stats(self, checktype):
        '''
        Calculate statistics to report
        :param checktype = "SSITC" or "FF"
        '''

        if checktype == "SSITC":
            for i in self.SSITCvars:
                _log.info('{i} has {n2} / {n} bad values and {n1} / {n} '
                          'suspicious values that affect variables: {dvar}'
                          .format(i=i, n2=self.data[i].tolist().count(2),
                                  n1=self.data[i].tolist().count(1),
                                  n=len(self.data), dvar=self.SSITCvd[i]))
        elif checktype == "FF":
            _log.info('FETCH_FILTER has {n2} / {n} bad values that '
                      'affect variables: {dvar}'
                      .format(n2=self.data["FETCH_FILTER"].tolist().count(1),
                              n=len(self.data), dvar=self.FFvars))

    def remove_bad_data(self, checktype):
        '''
        Removes data with certain flag value.
        :param checktype: indicator for which type of check, "SSITC" or "FF"
        '''
        if checktype == "SSITC":
            for i in self.SSITCvars:
                for j in self.SSITCvd[i]:
                    self.data[j] = np.where(
                        self.data[i] < 2, self.data[j], np.nan)
                _log.info('Bad values (2) indicated in '
                          '{i} were removed in {vid}'.format(
                              i=i, vid=self.SSITCvd[i]))

        elif checktype == "FF":
            for j in self.FFvars:
                self.data[j] = np.where(
                    self.data["FETCH_FILTER"] < 1, self.data[j], np.nan)
            _log.info(
                'Bad values (1) indicated in FETCH_FILTER '
                + 'were removed in {vid}'.format(vid=self.FFvars))

    def plot(self, checktype):
        '''
        Makes a plot of the data colored with flags
        :param checktype: indicator for which type of check: "SSITC" or "FF
        '''
        pass

    """ YWC: Unused function
    def colors(self, checktype):
        '''
        Sets colors scheme for plotting
        :param type args: Description of what args is
        '''

        cols = np.zeros()
        pass
    """
    def driver(self, data_reader, checktype="both", plot_dir=None):
        '''
        This is a driver to test and run QAQC Algorithm specific to this class
        :param checktype: determines which tests are run: "both", "SSITC", "FF"
        :param plot_dir: variable that indicates whether to
            -- make a plot (file name or data structure to hold
            -- None = don't plot
        '''
        # if not filename:
        #     parser = argparse.ArgumentParser(description=self.__doc__)
        #     parser.add_argument(
        #         'filename',
        #         type=str,
        #         help="Target filename")
        #     args = parser.parse_args()
        #     filename = args.filename

        # # Do something here
        # self.d = DataReader()
        # self.d.driver(filename)
        self.d = data_reader
        self.data = self.d.get_filled_data()

        if checktype in ("both", "SSITC"):
            _log.info('Running SSITC Check')
            if not self.SSITC_vars_check(self.d.header):
                pass
            else:
                self.stats(checktype="SSITC")
                if (plot_dir is not None):
                    self.plot(plot_dir)
                self.remove_bad_data(checktype="SSITC")
                _log.info('SSITC check COMPLETE')

        if checktype in ("both", "FF"):
            _log.info('Running FETCH_FILTER Check')
            if not self.FF_vars_check(self.d.header):
                pass
            else:
                self.stats(checktype="FF")
                if (plot_dir is not None):
                    self.plot(plot_dir)
                self.remove_bad_data(checktype="FF")
                _log.info('FETCH_FILTER check COMPLETE')
        return [status.StatusGenerator().composite_status_generator(
            _log, 'SSITC')]


if __name__ == "__main__":
    SSITC_FF_check().driver()
