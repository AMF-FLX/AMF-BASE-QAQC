import datetime as dt
import os
import multiprocessing as mp
import subprocess
import shlex
import sys
import time

from configparser import ConfigParser
from collections import namedtuple
from db_handler import DBConfig, NewDBHandler
from upload_checks import upload_checks


__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'upload_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id', 'uuid'])


class FormatQAQCDriver:
    def __init__(self, test=True):
        config = ConfigParser()
        with open(os.path.join(os.getcwd(), 'qaqc.cfg'), 'r') as cfg:
            cfg_section = 'FORMAT_QAQC_DRIVER'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.log_dir = config.get(cfg_section, 'log_dir')
                self.time_sleep = config.getfloat(cfg_section, 'time_sleep')
                self.max_retries = config.getint(cfg_section, 'max_retries')
                self.max_timeout = config.getint(cfg_section, 'max_timeout')
                self.timeout = self.max_timeout / 10.0

            cfg_section = 'DB'
            if config.has_section(cfg_section):
                hostname = config.get(cfg_section, 'hostname')
                user = config.get(cfg_section, 'user')
                auth = config.get(cfg_section, 'auth')
                db_name = config.get(cfg_section, 'db_name')
            if all([hostname, user, auth, db_name]):
                self.db = NewDBHandler()
                new_db_config = DBConfig(hostname, user, auth, db_name)
                self.conn = NewDBHandler.init_db_conn(new_db_config)
        log_file_date = dt.datetime.now().strftime('%Y-%m-%d')
        log_file_name = f'format_qaqc_driver_service_{log_file_date}.log'
        log_dir = os.path.join(os.getcwd(), self.log_dir)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file_path = os.path.join(os.getcwd(),
                                          self.log_dir,
                                          log_file_name)
        self.is_test = test
        self.email_gen_path = './email_gen.py'
        self.stale_count = 0

    def run_proc(self, cmd):
        p = subprocess.Popen(shlex.split(cmd),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        return (out, err)

    def get_new_upload_data(self, log, uuid=None):
        new_data_upload_log = self.db.get_new_data_upload_log(uuid)
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
            o_tasks, grouped_tasks = self.get_new_upload_data(log)
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
                # run tasks
                while o_tasks:
                    # upload_ids is a list of upload_id
                    # that has the same token
                    for upload_ids in grouped_tasks:
                        is_qaqc_successful = True
                        with mp.Pool(processes=num_processes) as pool:
                            for upload_id in upload_ids:
                                task = o_tasks.get(upload_id)
                                token = task.uuid
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
                                                  .get())
                                        (process_id,
                                         is_upload_successful,
                                         uuid) = result
                                        s_tasks = {}
                                        if uuid:
                                            s_tasks, _ = \
                                                self.get_new_upload_data(log,
                                                                         uuid)
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
                                                    for p in processes:
                                                        (p
                                                         .get('process')
                                                         .terminate())
                                                    for p in s_processes:
                                                        (p
                                                         .get('process')
                                                         .terminate())
                                                    is_qaqc_successful = False
                                        else:
                                            p['runtime'] += self.time_sleep
                                processes = s_processes
                        # it will get here if all good, send out email to token
                    if is_qaqc_successful:
                        cmd = ('python '
                               f'{self.email_gen_path} '
                               f'{token}')
                    else:
                        cmd = ('python '
                               f'{self.email_gen_team_path} '
                               f'{token}')
                    if not self.is_test:
                        self.run_proc(cmd)
                    else:
                        log.write(f'Email gen for token: {token}\n')
                    o_tasks, grouped_tasks = self.get_new_upload_data(log)
            if self.is_test:
                return
            sys.exit(0)
