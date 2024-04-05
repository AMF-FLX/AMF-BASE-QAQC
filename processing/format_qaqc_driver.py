import argparse
import datetime as dt
import os
import multiprocessing as mp
import sys
import time

from configparser import ConfigParser
from collections import namedtuple
from db_handler import DBConfig, NewDBHandler
from email_gen import EmailGen, EmailGenError
from pathlib import Path
from upload_checks import upload_checks


__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'upload_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id', 'uuid'])


class FormatQAQCDriver:
    def __init__(self, lookback_h=None, test=False):
        config = ConfigParser()
        with open(os.path.join(os.getcwd(), 'qaqc.cfg'), 'r') as cfg:
            cfg_section = 'FORMAT_QAQC_DRIVER'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.log_dir = config.get(cfg_section, 'log_dir')
                self.time_sleep = config.getfloat(cfg_section, 'time_sleep_s')
                self.max_retries = config.getint(cfg_section, 'max_retries')
                self.max_timeout = config.getint(cfg_section, 'max_timeout_s')
                self.timeout = self.max_timeout / 10.0
                if not lookback_h:
                    self.lookback_h = config.getint(cfg_section, 'lookback_h')

            cfg_section = 'DB'
            if config.has_section(cfg_section):
                hostname = config.get(cfg_section, 'hostname')
                user = config.get(cfg_section, 'user')
                auth = config.get(cfg_section, 'auth')
                db_name = config.get(cfg_section, 'db_name')
                self.db = NewDBHandler()
                new_db_config = DBConfig(hostname, user, auth, db_name)
                self.conn = self.db.init_db_conn(new_db_config)

            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                self.qaqc_processor_source = config.get(cfg_section,
                                                        'file_upload_source')

            cfg_section = 'JIRA'
            if config.has_section(cfg_section):
                self.email_prefix = config.get(cfg_section,
                                               'project')

            cfg_section = 'PHASE_I'
            if config.has_section(cfg_section):
                self.data_directory = config.get(cfg_section,
                                                 'data_dir')

        log_file_date = dt.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        log_file_name = f'format_qaqc_driver_service_{log_file_date}.log'
        log_dir = os.path.join(os.getcwd(), self.log_dir)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file_path = os.path.join(os.getcwd(),
                                          self.log_dir,
                                          log_file_name)
        self.is_test = test
        self.email_gen = EmailGen()
        self.email_gen_team_path = './email_gen_team.py'
        self.stale_count = 0

    def recovery_process(self):
        rerun_uuids = []
        o_data_upload = \
            self.db.get_undone_data_upload_log_o(
                self.conn,
                self.qaqc_processor_source,
                self.lookback_h)
        for row in o_data_upload:
            uuid = row.get('upload_token')
            if uuid not in rerun_uuids:
                rerun_uuids.append(uuid)
        print(rerun_uuids)
        ac_data_upload = \
            self.db.get_undone_data_upload_log_ac(
                self.conn,
                self.qaqc_processor_source,
                self.lookback_h)
        for row in ac_data_upload:
            print(ac_data_upload)
            comment = row.get('upload_comment')
            if ('Archive upload for' in comment
                    or 'repair candidate for' in comment):
                process_id = comment.split()[-1]
                while process_id:
                    print(process_id)
                    d = self.db.trace_o_data_upload(
                        self.conn,
                        process_id)
                    if (d.get('prior_process_id')
                            or d.get('zip_process_id')):
                        process_id = d.get('log_id')
                    else:
                        uuid = d.get('upload_token')
                        print('uuid:', uuid)
                        break
            if uuid not in rerun_uuids:
                rerun_uuids.append(uuid)
        print(rerun_uuids)
        return rerun_uuids

    def get_new_upload_data(self,
                            log,
                            is_qaqc_processor=True,
                            uuid=None,
                            is_recovery=False):
        if is_recovery:
            new_data_upload_log = \
                self.db.get_data_upload_log_with_uuid(
                    self.conn,
                    uuid)
        else:
            new_data_upload_log = \
                self.db.get_new_data_upload_log(self.conn,
                                                self.qaqc_processor_source,
                                                is_qaqc_processor,
                                                uuid)
        # if no more new data upload log in test mode
        # terminate after 3 empty rounds

        log_ids_list = ' '.join([str(row.get('log_id'))
                                 for row in new_data_upload_log])
        if log_ids_list:
            log_msg = f'Run with list of upload log ids: {log_ids_list} '
            if uuid:
                log_msg += f'with token: {uuid}]\n'
            log.write(log_msg)
        tasks = {}
        for row in new_data_upload_log:
            upload_id = row.get('log_id')
            site_id = row.get('site_id')
            token = row.get('upload_token')

            # get origin, zip, repair run_type
            zip_process_id = None
            prior_process_id = None
            run_type = 'o'
            upload_comment = row.get('upload_comment', '')
            if 'repair candidate for' in upload_comment:
                run_type = 'r'
                prior_process_id = upload_comment.split()[-1]
            elif 'Archive upload for' in upload_comment:
                zip_process_id = upload_comment.split()[-1]
            filename = row.get('data_file')
            filename = str(Path(self.data_directory)/site_id/filename)

            tasks[upload_id] = Task(filename,
                                    upload_id,
                                    prior_process_id,
                                    zip_process_id,
                                    run_type,
                                    site_id,
                                    token)
        grouped_tasks = {}
        for upload_id, task_data in tasks.items():
            token = task_data.uuid
            if token not in grouped_tasks:
                grouped_tasks[token] = []
            grouped_tasks[token].append(upload_id)
        grouped_upload_id = list(grouped_tasks.values())
        return tasks, grouped_upload_id

    def run(self):
        num_processes = mp.cpu_count()
        processes = []
        with open(self.log_file_path, 'w+') as log:
            # run recovery process
            rerun_uuids = self.recovery_process()
            print(f"Rerun for these uuids: [{', '.join(rerun_uuids)}]")
            o_tasks = {}
            o_grouped_tasks = []
            for uuid in rerun_uuids:
                tasks, grouped_tasks = \
                    self.get_new_upload_data(log,
                                             False,
                                             uuid=uuid,
                                             is_recovery=True)
                o_tasks.update(tasks)
                o_grouped_tasks.extend(grouped_tasks)
            # get new task
            tasks, grouped_tasks = self.get_new_upload_data(
                log,
                False)
            o_tasks.update(tasks)
            o_grouped_tasks.extend(grouped_tasks)
            stop_run = False
            while True:
                if self.is_test:
                    if not o_tasks:
                        self.stale_count += 1
                        log.write(('[TEST MODE] Empty run '
                                   f'{self.stale_count} time(s)\n'))
                        print(self.stale_count)
                    if self.stale_count >= 3:
                        stop_run = True
                if stop_run:
                    break

                # upload_ids is a list of upload_id
                # that has the same token
                for upload_ids in o_grouped_tasks:
                    is_qaqc_successful = True
                    with mp.Pool(processes=num_processes) as pool:
                        for upload_id in upload_ids:
                            task = o_tasks.get(upload_id)
                            token = task.uuid
                            is_zip = '.zip' in task.filename
                            log.write(
                                (f'Start run: log id {task.upload_id}, '
                                    f'prior id: {task.prior_process_id}, '
                                    f'zip id: {task.zip_process_id}, '
                                    f'run type: {task.run_type}\n'))
                            processes.append(
                                {'process':
                                    pool.apply_async(upload_checks,
                                                     (task.filename,
                                                      task.upload_id,
                                                      task.run_type,
                                                      task.site_id,
                                                      task.
                                                      prior_process_id,
                                                      task.zip_process_id,
                                                      self.is_test)),
                                    'runtime': 0,
                                    'retry': 0,
                                    'task': task})
                        while processes:
                            time.sleep(self.time_sleep)
                            s_processes = []
                            for p in processes:
                                try:
                                    result = (p
                                              .get('process')
                                              .get(timeout=5))
                                    (process_id,
                                        is_upload_successful,
                                        uuid) = result
                                    s_tasks = {}
                                    if uuid and is_upload_successful:
                                        s_tasks, _ = \
                                            self.get_new_upload_data(log,
                                                                     True,
                                                                     uuid)
                                        if is_zip and len(s_tasks) > 1:
                                            token = uuid
                                    for task in s_tasks.values():
                                        log.write(
                                            ('Start run: log id '
                                                f'{task.upload_id}, '
                                                'prior id: '
                                                f'{task.prior_process_id}, '
                                                f'zip id: '
                                                f'{task.zip_process_id}, '
                                                f'run type: {task.run_type}\n'
                                                f'uuid: {task.uuid}'))
                                        s_processes.append(
                                            {'process':
                                                pool
                                                .apply_async(
                                                    upload_checks,
                                                    (task.filename,
                                                        task.upload_id,
                                                        task.run_type,
                                                        task.site_id,
                                                        task
                                                        .prior_process_id,
                                                        task
                                                        .zip_process_id,
                                                        self.is_test)),
                                                'runtime': 0,
                                                'retry': 0,
                                                'task': task})
                                except mp.TimeoutError:
                                    if p.get('runtime') > self.max_timeout:
                                        p.get('process').terminate()
                                        log.write('Process terminated '
                                                  'due to time out '
                                                  'for upload_id: '
                                                  f'{task.upload_id}')
                                        if p.get('retry') < \
                                                self.max_retries:
                                            task = p.get('task')
                                            p['process'] = \
                                                pool.apply_async(
                                                    upload_checks,
                                                    (task.filename,
                                                        task.upload_id,
                                                        task.run_type,
                                                        task.site_id,
                                                        task.prior_process_id,
                                                        task.zip_process_id,
                                                        self.is_test))
                                            p['runtime'] = 0
                                            retry = p.get('retry') + 1
                                            p['retry'] = retry
                                            log.write('Retry to run '
                                                      'upload_id: '
                                                      f'{task.upload_id}, '
                                                      f'retry: {retry}')
                                        else:
                                            # terminate all process
                                            # for this run
                                            # and send email to team
                                            if p.get('retry') > \
                                                    self.max_retries:
                                                (p
                                                    .get('process')
                                                    .terminate())
                                                is_qaqc_successful = False
                                    else:
                                        p['runtime'] += self.time_sleep
                            processes = s_processes
                    # it will get here if all good, send out email to token
                    if is_qaqc_successful:
                        try:
                            msg = self.email_gen.driver(token, True)
                            if msg.startswith(self.email_prefix):
                                log.write(f'Email gen for token: {token}, '
                                          'with message: {msg}\n')
                        except EmailGenError:
                            # send email to AMP
                            cmd = ('python '
                                   f'{self.email_gen_team_path} '
                                   f'{token}')
                    else:
                        # send email to AMP
                        cmd = ('python '
                               f'{self.email_gen_team_path} '
                               f'{token}')
                time.sleep(self.time_sleep)
                o_tasks, o_grouped_tasks = \
                    self.get_new_upload_data(log,
                                             False)
            if self.is_test:
                return
            sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Format QAQC Driver')
    parser.add_argument(
        'lookback_h', type=str, help='Look back x hours to check '
                                     'for any unfinished work')
    parser.add_argument(
        '-t', '--test', action='store_true', default=False,
        help='Sets flag for local run that does not write'
             ' to database')
    args = parser.parse_args()
    driver = FormatQAQCDriver(lookback_h=args.lookback_h,
                              test=args.test)
    driver.run()
