#!/usr/bin/env python

import argparse
from data_reader import DataReader
from logger import Logger
from messages import Messages
from status import StatusGenerator
from utils import TextUtil


__author__ = 'Danielle Christianson, Norm Beekwilder'
__email__ = 'dschristianson@lbl.gov, norm.beekwilder@gmail.com'
_log = Logger().getLogger(__name__)


class DataMissing:
    ''' check to see if data file has been gap filled
    '''

    def __init__(self):
        ''' Initialize variables on loading of class here '''
        self.status_msg_parts = []
        self.txt_util = TextUtil()
        self.msg = Messages()

    def all_missing_data_detection(self, data_reader):
        '''
        :param data_reader: data reader object containing data to process
        :type data_reader: DataReader
        '''
        # Possible fix: change this to catch other forms of missing values...
        file_data = data_reader.get_data()
        number_data_variables = 0
        for header in file_data.dtype.names:
            if 'timestamp' in header.lower():
                continue
            number_data_variables += 1
            data_types = set(file_data.mask[header])
            if len(data_types) == 0 or (len(data_types) < 2
                                        and file_data.mask[header][0]):
                _log.warning('No data in column {c}'.format(c=header))
                self.status_msg_parts.append(header)
        if not self.status_msg_parts:
            _log.info('All variables have some data.')
            return None, number_data_variables
        else:
            return self.status_msg_parts, number_data_variables

    def driver(self, data_reader):
        """ This is a driver to test and run QAQC Algorithm specific
            to this class
        """
        statuses = []
        _log.info("Beginning to run all_missing_data_detection algorithm...")
        _log.resetStats()
        gap_list, number_data_variables = self.all_missing_data_detection(
            data_reader)
        if self.status_msg_parts:
            if len(self.status_msg_parts) == number_data_variables:
                plural = self.txt_util.decide_plurals([number_data_variables])
                # 'Is all Data Missing?'
                qaqc_check = self.msg.get_display_check('all_data_missing')
                status_msg = self.msg.get_msg(
                    'all_data_missing', 'WARNING').format(
                        dv=number_data_variables, pl=plural)
                check_log = Logger().getLogger(qaqc_check)
                check_log.warning(status_msg)
                statuses.append(StatusGenerator().status_generator(
                    logger=check_log, qaqc_check=qaqc_check,
                    status_msg=status_msg, report_type='single_msg'))
            gap_list = ', '.join(self.status_msg_parts)
        # 'Any Variables with ALL Data Missing?'
        qaqc_check = self.msg.get_display_check(_log.getName())
        statuses.append(StatusGenerator().status_generator(
            logger=_log, qaqc_check=qaqc_check,
            status_msg=gap_list, report_type='single_list'))
        return statuses

    def test(self, filename=None):
        """Test method to be used for testing module independently
        :param filename: File name of test file
        :type filename: str.
        """
        if not filename:
            parser = argparse.ArgumentParser(description=self.__doc__)
            parser.add_argument(
                'filename',
                type=str,
                help="Target filename")
            args = parser.parse_args()
            filename = args.filename

        d = DataReader()
        d.driver(filename)
        return self.driver(d)


if __name__ == "__main__":
    _log = Logger(True).getLogger(__name__)
    print(DataMissing().test().get_status())
