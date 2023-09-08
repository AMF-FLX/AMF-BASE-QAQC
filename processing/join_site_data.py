#!/usr/bin/env python

import argparse
import collections
import datetime
import status

from configparser import ConfigParser
from file_name_verifier import FileNameVerifier
from logger import Logger
from pathlib import Path
from report_status import ReportStatus
from utils import VarUtil

__author__ = 'Norm Beekwilder, Danielle Chrisitanson'
__email__ = 'norm.beekwilder@gmail.com, dschristianson@lbl.gov'
_log = Logger().getLogger(__name__)


class JoinSiteData:
    def __init__(self):
        self.FileInfo = collections.namedtuple(
            'FileInfo',
            'start end upload name status proc_id original_name prior_proc_id')
        self.var_util = VarUtil()
        self.reporter = ReportStatus()
        self.stat_gen = status.StatusGenerator()
        config = ConfigParser()
        with open(Path.cwd() / 'qaqc.cfg') as cfg:
            config.read_file(cfg)
            cfg_section = 'PHASE_II'
            if config.has_section(cfg_section):
                self.temp_dir = config.get(cfg_section, 'combined_file_dir')
                self.data_dir = config.get(cfg_section, 'data_dir')

    def get_file_order(self, candidates_data, skip_log=_log):
        upload_order = sorted(candidates_data, key=lambda t: t.start)
        upload_order.sort(key=lambda t: t.upload, reverse=True)
        file_order = []
        # build list of time ranges to copy in chronological order.
        #    start with most recent files and then
        #    fill gaps with older files
        skip_list = []
        for times in upload_order:
            overlap = [t for t in file_order
                       if t.start <= times.start < t.end]
            if len(overlap) > 0:
                # start time is part of existing range
                if overlap[0].end >= times.end:
                    if overlap[0].upload == times.upload:
                        _log.fatal(
                            'overlapping files are from the '
                            f'same upload: {overlap[0].name} '
                            f'and {times.name}')
                        return (
                            None,
                            self.stat_gen.status_generator(
                                _log, 'join_site_files',
                                'overlapping files are from the '
                                f'same upload: {overlap[0].name} '
                                f'and {times.name}',
                                report_section='high_level'),
                            None)
                    else:
                        # entire time range is overlapping, this
                        #     file has been superseded, skip file
                        skip_list.append(times)
                        skip_log.warning(
                            f'skipping file {times.name} entire '
                            'data range covered by newer file(s)')
                    continue
                if not self.insert_range(file_order,
                                         file_order.index(overlap[0]), times):
                    # entire time range is overlapping, this
                    #     file has been superseded, skip file
                    skip_list.append(times)
                    skip_log.warning(
                        f'skipping file {times.name} entire data '
                        'range covered by newer file(s)')
            else:
                # start time is not part of an existing range
                #     see if there is an existing range before
                #     the end time
                overlap = [t for t in file_order
                           if times.start <= t.start < times.end]
                if len(overlap) > 0:
                    # an existing range starts before
                    #     end of current range
                    i = file_order.index(overlap[0])
                    file_order.insert(i, self.FileInfo(
                        start=times.start, end=file_order[i].start,
                        upload=times.upload, name=times.name,
                        status=times.status, proc_id=times.proc_id,
                        original_name=times.original_name,
                        prior_proc_id=times.prior_proc_id))
                    i += 1
                    if file_order[i].end < times.end:
                        # current range extends past end of overlap
                        self.insert_range(file_order, i, times)
                else:
                    # this range is completely independent of
                    #     any other ranges
                    #     now figure out where to insert it
                    inserted = False
                    for i, t in enumerate(file_order):
                        if t.start >= times.end:
                            file_order.insert(i, times)
                            inserted = True
                            break
                    if not inserted:
                        file_order.append(times)
        return file_order, skip_list

    def join_site_files(self, proc_id, site_id, resolution, is_test=False):
        dir_site_path = Path(self.data_dir) / site_id
        if not dir_site_path.is_dir():
            _log.fatal(f'Directory not found for site {site_id}.')
            return None, self.stat_gen.status_generator(
                _log, 'join_site_files',
                status_msg=f'Directory not found for site {site_id}.',
                report_section='high_level'), None
        valid_files = self.reporter.get_available_base_input(site_id)
        dir_files = {}
        file_paths = [file_path for file_path in dir_site_path.glob('*')
                      if file_path.is_file()]
        for file_path in file_paths:
            file_name = file_path.name
            if file_name not in valid_files:
                continue
            fnv = FileNameVerifier()
            fn_stat = fnv.driver(str(file_path))
            if fn_stat.get_status_code() == status.StatusCode.OK:
                if fnv.fname_attrs['site_id'] not in dir_files:
                    dir_files[fnv.fname_attrs['site_id']] = {}
                if fnv.fname_attrs['resolution'] \
                        not in dir_files[fnv.fname_attrs['site_id']]:
                    dir_files[fnv.fname_attrs['site_id']][fnv.fname_attrs[
                        'resolution']] = {}
                if 'optional' in fnv.fname_attrs:
                    opt = fnv.fname_attrs['optional']
                else:
                    opt = 'base'
                if opt not in dir_files[fnv.fname_attrs[
                        'site_id']][fnv.fname_attrs['resolution']]:
                    dir_files[fnv.fname_attrs['site_id']][fnv.fname_attrs[
                        'resolution']][opt] = []
                dir_files[fnv.fname_attrs['site_id']][fnv.fname_attrs[
                    'resolution']][opt].append(self.FileInfo(
                        start=fnv.fname_attrs['ts_start'],
                        end=fnv.fname_attrs['ts_end'],
                        upload=fnv.fname_attrs.get('ts_upload', ''),
                        name=file_name, status=fn_stat,
                        proc_id=valid_files[file_name]['process_id'],
                        original_name=valid_files[file_name]['original_name'],
                        prior_proc_id=valid_files[file_name]['prior_process_id']))
        for site_id, site in dir_files.items():
            for res, res_data in site.items():
                if res != resolution:
                    continue
                for opt, opt_data in res_data.items():
                    if opt != 'base':
                        continue
                    file_status = []  # set up list for status objects
                    # set up logger to capture skipped files
                    skip_qaqc_check = f'{__name__}-skipped_files'
                    skip_log = Logger().getLogger(skip_qaqc_check)
                    # sort files from most recent upload to oldest upload
                    #     and then from oldest start time to most recent
                    file_order, skip_list = self.get_file_order(
                        candidates_data=opt_data, skip_log=skip_log)
                    fo = []
                    # now that all data ranges are in chronological
                    #     non-overlapping order check for gaps
                    #     set up logger for gaps
                    gap_qaqc_check = f'{__name__}-gaps'
                    gap_list = []
                    gap_log = Logger().getLogger(gap_qaqc_check)
                    # set up logger for trimming record start date
                    trim_start_date_check = f'{__name__}-trim_start_date'
                    trim_log = Logger().getLogger(trim_start_date_check)
                    trim_start_date = None
                    if file_order[0].start[4:8] == '1231':
                        file_info = file_order[0]
                        start_year = str(int(file_info.start[0:4]) + 1)
                        trim_start_date = f'{start_year}01010000'
                        file_order[0] = self.FileInfo(
                            start=trim_start_date,
                            end=file_info.end,
                            upload=file_info.upload,
                            name=file_info.name,
                            status=file_info.status,
                            proc_id=file_info.proc_id,
                            original_name=file_info.original_name,
                            prior_proc_id=file_info.prior_proc_id
                        )
                        trim_log.warning('Data record start date was trimmed '
                                         f'to {trim_start_date}')
                    elif not file_order[0].start.endswith('01010000'):
                        # gap fill file to start of year.
                        start = f'{file_order[0].start[:4]}01010000'
                        self.create_gap_filler(fo, gap_list, gap_log, start,
                                               file_order[0].start)
                    for index, times in enumerate(file_order[:-1]):
                        if times.end != file_order[index+1].start:
                            if times.end < file_order[index+1].start:
                                # there is a gap between the end of this file
                                #      and the start of the next file
                                fo.append(times)
                                self.create_gap_filler(
                                    fo, gap_list, gap_log,
                                    times.end, file_order[index + 1].start)
                            else:
                                # there is overlap between files
                                #       this should never happen
                                filename = file_order[index+1].name
                                _log.fatal(
                                    'Time overlap found between input '
                                    f'files {times.name} and {filename}.')
                                return None, self.stat_gen.status_generator(
                                    _log, 'join_site_files',
                                    status_msg='Time overlap found between '
                                               f'input files {times.name} '
                                               f'and {filename}.',
                                    report_section='high_level'), None
                        else:
                            fo.append(times)
                    fo.append(file_order[-1])
                    file_order = fo
                    potential_headers = {}
                    files = {}
                    for times in file_order:
                        if times.upload == -9999:
                            continue
                        if times.name in files:
                            continue
                        files[times.name] = open(dir_site_path / times.name, 'r')
                        header_ln = files[times.name].readline().rstrip('\n')
                        potential_headers[times.name] = header_ln.split(',')
                    header_order = self.get_valid_variables(potential_headers)
                    file_name_timestamp = datetime.datetime.strftime(
                        datetime.datetime.now(), '%Y%m%d%H%M')
                    out_file_name = (
                        f'{site_id}_{res}_{file_order[0].start}_'
                        f'{file_order[-1].end}-{file_name_timestamp}.csv')
                    temp_dir_path = Path(self.temp_dir)
                    temp_dir_path.mkdir(parents=True, exist_ok=True)
                    file_path = temp_dir_path / out_file_name
                    out_file = open(file_path, 'w')
                    out_file.write(','.join(header_order)+'\n')
                    sub_stat = {}
                    for times in file_order:
                        if times.upload == -9999:
                            self.fill_gap(out_file, len(header_order),
                                          res, times)
                            continue
                        self.copy_file(out_file, files[times.name],
                                       len(header_order),
                                       self.make_header_map(
                                           header_order,
                                           potential_headers[times.name]),
                                       times)
                        sub_stat[times.name] = times.status
                    for f in files.values():
                        f.close()
                    out_file.close()
                    # report the file name
                    file_status.append(self.stat_gen.status_generator(
                        logger=_log, qaqc_check=__name__,
                        status_msg=f'Combined File: {out_file_name}',
                        report_section='info'))
                    # report info on headers
                    header_text = ', '.join(header_order)
                    file_status.append(self.stat_gen.status_generator(
                        logger=_log, qaqc_check=f'{__name__}headers',
                        status_msg=f'Variables: {header_text}',
                        report_section='info'))
                    # report info on any skipped files
                    if skip_list:
                        skip_list_text = ', '.join([s.name for s in skip_list])
                        skip_list = ('The following files were skipped b/c '
                                     'the entire time period was included '
                                     f'in a newer file: {skip_list_text}')
                    else:
                        skip_list = 'All eligible files were incorporated.'
                    file_status.append(self.stat_gen.status_generator(
                        logger=skip_log, qaqc_check=skip_qaqc_check,
                        status_msg=skip_list, report_section='high_level'))
                    # report info on trim start date
                    if trim_start_date:
                        trim_start_date = ('Start of date record was trimmed '
                                           f'to {trim_start_date}')
                    file_status.append(self.stat_gen.status_generator(
                        logger=trim_log, qaqc_check=trim_start_date_check,
                        status_msg=trim_start_date,
                        report_section='high_level'))
                    # report info on gap-fill
                    if gap_list:
                        gap_list_text = ', '.join(gap_list)
                        gap_list = f'Gaps were filled: {gap_list_text}'
                    else:
                        gap_list = 'No time gaps detected'
                    file_status.append(self.stat_gen.status_generator(
                        logger=gap_log, qaqc_check=gap_qaqc_check,
                        status_msg=gap_list, report_section='high_level'))
                    if not is_test:
                        file_path_str = str(file_path)
                        self.reporter.register_base_files(
                            proc_id, file_path_str, file_order)
                    _log.resetStats()
                    return file_path, file_status, file_order
        return (None,
                self.stat_gen.status_generator(
                    _log, 'join_site_files',
                    status_msg=('Did not find any files for the given site '
                                f'{site_id} and resolution {resolution} '
                                'to combine')),
                None)

    def get_valid_variables(self, potential_variables: dict) -> list:
        """
        Find the valid variables in the files that are to be used.
        :param potential_variables: dict, key = file to be used,
                                        value = variables in file
        :return: list, the list of valid headers to use
        """
        variables_seen = []
        for file_variables in potential_variables.values():
            for variable in file_variables:
                if variable not in variables_seen:
                    variables_seen.append(variable)
        valid_variables = []
        for variable in variables_seen:
            is_valid_variable = self.var_util.is_valid_variable(variable)
            if is_valid_variable:
                valid_variables.append(variable)
        return valid_variables

    def create_gap_filler(self, fo, gap_list, gap_log, start, end):
        gap_info = f'{start} to {end}.'
        gap_list.append(gap_info)
        gap_log.warning(
            f'Time gap found from {gap_info} Filling with empty values.')
        fo.append(self.FileInfo(start=start, end=end, upload=-9999,
                                name=None, status=None, proc_id=None,
                                original_name=None, prior_proc_id=0))

    def insert_range(self, fo, i, times):
        rv = False
        while i < len(fo):
            if len(fo) == i + 1:
                # file extends data range add non overlapping
                #     part to end of list
                fo.append(self.FileInfo(
                    start=fo[i].end, end=times.end, upload=times.upload,
                    name=times.name, status=times.status,
                    proc_id=times.proc_id,
                    original_name=times.original_name,
                    prior_proc_id=times.prior_proc_id))
                rv = True
                break
            if fo[i].end != fo[i + 1].start:
                # gap between existing files use this file to fill it.
                if times.end < fo[i + 1].start:
                    # file doesn't cover entire gap
                    fo.insert(i + 1, self.FileInfo(
                        start=fo[i].end, end=times.end, upload=times.upload,
                        name=times.name, status=times.status,
                        proc_id=times.proc_id,
                        original_name=times.original_name,
                        prior_proc_id=times.prior_proc_id))
                    rv = True
                    break
                else:
                    # file extends past end of gap
                    fo.insert(i + 1, self.FileInfo(
                        start=fo[i].end, end=fo[i + 1].start,
                        upload=times.upload, name=times.name,
                        status=times.status, proc_id=times.proc_id,
                        original_name=times.original_name,
                        prior_proc_id=times.prior_proc_id))
                    rv = True
                    # after insert what was i+1 is now i+2
                    if times.end < fo[i + 2].end:
                        # the rest of this file is already covered
                        #     so we are done with it
                        break
            else:
                if times.end <= fo[i + 1].end:
                    break
            i += 1
        return rv

    def make_header_map(self, dest_headers, source_headers):
        header_map = {}
        for i, h in enumerate(source_headers):
            if h in dest_headers:
                header_map[i] = dest_headers.index(h)
        return header_map

    def copy_file(self, dest_file, source_file, col_count, header_map, times):
        while True:
            line = source_file.readline()
            if line == '':
                break  # hit eof
            in_line = line.rstrip('\n').split(',')
            if in_line[0] < times.start:
                continue
            out_line = ['-9999'] * col_count
            for s, d in header_map.items():
                out_line[d] = in_line[s]
            dest_file.write(','.join(out_line)+'\n')
            if in_line[1] == times.end:
                break

    def fill_gap(self, dest_file, col_count, resolution, times):
        out_line = ['-9999'] * col_count
        start = datetime.datetime.strptime(times[0], '%Y%m%d%H%M')
        if resolution == 'HH':
            step = datetime.timedelta(minutes=30)
        elif resolution == 'HR':
            step = datetime.timedelta(hours=1)
        else:
            _log.fatal(f'Unknown file resolution [{resolution}]')
            return
        end = datetime.datetime.strptime(times[1], '%Y%m%d%H%M')
        while start < end:
            out_line[0] = start.strftime('%Y%m%d%H%M')
            start += step
            out_line[1] = start.strftime('%Y%m%d%H%M')
            dest_file.write(','.join(out_line)+'\n')

    def driver(self, proc_id, site_id, resolution, is_test=False):
        """ This is a driver to test and run QAQC
        Algorithm specific to this class
        """
        return self.join_site_files(proc_id, site_id, resolution, is_test)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Join Site Data Test Mode')
    parser.add_argument('site_id', type=str, help='Target Site_ID')
    parser.add_argument('resolution', type=str, help='Target site')
    args = parser.parse_args()

    # Use process_ID = -1 for testing
    _log = Logger(True, '0', args.site_id,
                  'BASE Generation').getLogger(__name__)
    JoinSiteData().driver(proc_id=-1, site_id=args.site_id,
                          resolution=args.resolution, is_test=True)
    # [print(s.get_status()) for s in JoinSiteData().driver(sys.argv[1])]
