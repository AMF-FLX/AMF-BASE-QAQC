#!/usr/bin/env python

import argparse
from datetime import datetime
from logger import Logger
from messages import Messages
from pathlib import Path
from site_attrs import SiteAttributes
from status import StatusGenerator


__author__ = 'Norm Beekwilder, You-Wei Cheah'
__email__ = 'norm.beekwilder@gmail.com, ycheah@lbl.gov'

_log = Logger().getLogger(__name__)


class FileNameVerifier():
    '''
    This class is used to verify whether the filename is in compliance with
    the specifications from FP-In.

    Norm wrote original code, rewritten by You-Wei
    '''

    def __init__(self):
        ''' Initialize variables on loading of class here '''
        self.site_attrs = SiteAttributes()
        self.fname_attrs = {}
        self.status_msg_parts = {'fatal': [], 'error': [],
                                 'warning': [], 'ok': []}
        self.msg = Messages()

    def is_file_exist(self, file_path):
        '''
        Check if file exists

        :param Path file_path: File path
        '''
        file_name = file_path.name
        if file_path.exists():
            _log.info('File exists.')
            return True
        else:
            _log.fatal(f'File {file_name} does not exist.')
            self.status_msg_parts['fatal'].append('file not found')
            return False

    def has_csv_ext(self, fname_ext):
        '''
        Check if file has csv extension in filename.

        :param str fname_ext: Filename extension
        '''
        if fname_ext == '.csv':
            _log.info('File has CSV extension.')
            return True
        else:
            _log.error(f'CSV file has {fname_ext} extension instead.')
            self.status_msg_parts['error'].append('extension is not csv')
            return False

    def is_AMF_site_id(self, site_id):
        '''
        Check if site ID is a known AmeriFlux site ID.

        :param str site_id: Site ID from filename
        '''

        if site_id in self.site_attrs.get_site_dict().keys():
            _log.info('Filename has valid site ID.')
            return True
        else:
            _log.error(f'Filename has non-AmeriFlux site ID {site_id}')
            self.status_msg_parts['error'].append('SITE_ID')
            return False

    def is_valid_resolution(self, resolution):
        '''
        Check if resolution from filename is valid

        :param str resolution: Resolution specified in filename
        '''
        if resolution in ('HH', 'HR'):
            _log.info('Filename resolution is valid.')
            return True
        else:
            _log.error(f'Filename resolution is invalid: {resolution}')
            self.status_msg_parts['error'].append('resolution')
            return False

    def is_valid_timestamp(self, timestamp, label):
        '''
        Check if timestamp from filename is valid

        :param str timestamp: start or end time specified in filename
        :param str label: indicate if this is the start or stop time for
                          the file
        '''
        if len(timestamp) != 12:
            _log.warning(
                f'Filename {label} has unexpected length: {timestamp}')
            self.status_msg_parts['warning'].append(label)
            return False
        try:
            datetime.strptime(timestamp, '%Y%m%d%H%M')
        except ValueError:
            _log.error(
                f'Filename {label} has invalid time format: {timestamp}')
            self.status_msg_parts['error'].append(label)
            return False
        return True

    def has_no_optional_param(self, option):
        '''
        Check if optional parameter from filename is valid.

        :param str option: Optional parameter from filename
        '''
        if not option:
            _log.info('Filename does not have optional param.')
            return True
        _log.warning(f'Filename includes optional param: {option}')
        self.status_msg_parts['warning'].append(
            'optional parameter included '
            '(will be removed in autocorrected file)')
        return False

    def set_fname_attrs(self, fname_pieces):
        '''
        Parse out parts of filename and put them into a dictionary

        :param array fname_pieces: array containing parts of filename
        '''
        self.fname_attrs['site_id'] = fname_pieces[0]
        self.fname_attrs['resolution'] = fname_pieces[1]
        self.fname_attrs['ts_start'] = fname_pieces[2]
        self.fname_attrs['ts_end'] = fname_pieces[3]
        try:
            self.fname_attrs['optional'] = fname_pieces[4]
        except Exception:
            _log.info('Filename has no optional parameter.')
            pass

    def check_fname_pieces(self, fname_attrs):
        '''
        Perform various checks on filename attributes

        :param dict fname_attrs: Dictionary containing parts from filename
        '''
        res = []
        site_id = fname_attrs.get('site_id')
        resolution = fname_attrs.get('resolution')
        start = fname_attrs.get('ts_start')
        end = fname_attrs.get('ts_end')
        option = fname_attrs.get('optional')

        res.append(self.is_AMF_site_id(site_id))
        res.append(self.is_valid_resolution(resolution))
        res.append(self.is_valid_timestamp(start, 'ts-start (start time)'))
        res.append(self.is_valid_timestamp(end, 'ts-end (end time)'))
        res.append(self.has_no_optional_param(option))
        return all(res)

    def is_filename_FPIn_compliant(self, file_path):
        '''
        Check if filename is FP-Compliant

        :param Path file_path: file path
        '''
        is_compliant = []
        warning_msg = ('File name is not compliant with section 1.3 '
                       'of FP-IN standard.')
        # if its an absolute path strip out the path and look at just
        # the file name
        fname_noext, fname_ext = file_path.stem, file_path.suffix
        if not fname_noext or not fname_ext:
            _log.error(warning_msg)
            is_compliant.append(False)

        is_compliant.append(self.has_csv_ext(fname_ext))
        self.fname_attrs['ext'] = fname_ext

        # if this file was uploaded through the web interface strip
        # out the upload timestamp
        if fname_noext.count('-') > 1:
            fname_noext, upload_time = fname_noext.rsplit('-', 1)
            self.fname_attrs['ts_upload'] = upload_time

        if fname_noext.endswith('_'):
            _log.error(warning_msg)
            is_compliant.append(False)
            self.status_msg_parts['error'].append(
                'filename ends with underscore')

        fname_pieces = fname_noext.split('_')
        if len(fname_pieces) < 4 or len(fname_pieces) > 5:
            _log.warning(warning_msg)
            is_compliant.append(False)
            self.status_msg_parts['warning'].append(
                'incorrect number of components (expect timestamp errors)')
        else:
            self.set_fname_attrs(fname_pieces)
            is_compliant.append(self.check_fname_pieces(self.fname_attrs))
        return all(is_compliant)

    def make_filename(self):
        fname = '{s}_{r}_{ts}_{te}'.format(
            s=self.fname_attrs['site_id'], r=self.fname_attrs['resolution'],
            ts=self.fname_attrs['ts_start'], te=self.fname_attrs['ts_end'])
        if 'optional' in self.fname_attrs:
            fname += '_{o}'.format(o=self.fname_attrs['optional'])
        if 'ts_upload' in self.fname_attrs:
            fname += '-{t}'.format(t=self.fname_attrs['ts_upload'])
        fname += self.fname_attrs['ext']
        return fname

    def driver(self, file_path=None, fixer=False):
        '''
        Driver to test and run QAQC specific to this class

        :param str file_path
        '''
        _log.resetStats()
        # make sure any previous calls of filename verifier
        # are cleared from log
        if not file_path:
            parser = argparse.ArgumentParser(description=self.__doc__)
            parser.add_argument(
                'filename',
                type=str,
                help="Target filename")
            args = parser.parse_args()
            file_path = args.filename

        p_file_path = Path(file_path)
        # Various checks
        _log.info(f'Verifying filename for file {p_file_path.name}')
        check_exist = self.is_file_exist(p_file_path)
        if check_exist:
            self.is_filename_FPIn_compliant(p_file_path)
            # qaqc_check = "File name verifier for {f}".format(f=fname)
        qaqc_check = self.msg.get_display_check(_log.getName())
        # "Is Filename Format valid?"
        any_msg_parts = False
        for s in self.status_msg_parts.values():
            if s:
                any_msg_parts = True
        if not any_msg_parts or fixer:
            report_type = 'single_msg'
            status_msg = None
            if fixer:
                report_type = 'single_list'
                status_msgs = []
                for s in self.status_msg_parts.keys():
                    status_msgs.extend(self.status_msg_parts[s])
                status_msg = ', '.join(status_msgs)
            return StatusGenerator().status_generator(
                _log, qaqc_check, status_msg=status_msg,
                report_type=report_type)
        else:
            for s in self.status_msg_parts.keys():
                self.status_msg_parts[s] = ', '.join(self.status_msg_parts[s])
            report_type = 'sub_status_single_msg'
            status_msg = None
            return StatusGenerator().split_status_generator(
                _log, qaqc_check, status_msg=status_msg,
                report_type=report_type, status_msgs=self.status_msg_parts,
                sub_type='single_list')


if __name__ == "__main__":
    FileNameVerifier().driver()
