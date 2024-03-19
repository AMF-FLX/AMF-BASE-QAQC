import datetime as dt
import multiprocessing as mp
import os
import subprocess
import shlex
import sys
import time

from configparser import ConfigParser
from collections import namedtuple
from multiprocessing.pool import ThreadPool
from db_handler import DBConfig, NewDBHandler
from upload_checks import upload_checks

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'upload_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id'])


class UploadChecks():
    def __init__(self):
        pass

    def run(self):
        pass


class FormatQAQCDriver:
    def __init__(self, test=True):

        self.upload_checks = UploadChecks()

        config = ConfigParser()
        with open(os.path.join(os.getcwd(), 'qaqc.cfg'), 'r') as cfg:
            cfg_section = 'FORMAT_QAQC_DRIVER'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.data_dir = config.get(cfg_section, 'data_dir')
                self.log_dir = config.get(cfg_section, 'log_dir')
                self.time_sleep = config.getfloat(cfg_section, 'time_sleep')
                self.max_retries = config.getint(cfg_section, 'max_retries')

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

    def get_new_data_upload_log(self, log):
        new_data_upload_log = self.db.get_new_data_upload_log()
        # if no more new data upload log in test mode
        # terminate after 3 empty rounds

        log_ids_list = ' '.join([row.get('log_id')
                                 for row in new_data_upload_log])
        if log_ids_list:
            log.write(f'Run with list of log ids: {log_ids_list}\n')

        mail_uuid = {}  # map between upload_id and uuid
        tasks = {}
        for row in new_data_upload_log:
            upload_id = row.get('log_id')
            site_id = row.get('site_id')

            # get origin, zip, repair run_type
            zip_process_id = None
            prior_process_id = None
            run_type = 'o'
            upload_comment = row.get('upload_comment', '')
            if 'repair candidate for' in upload_comment:
                run_type = 'r'
                prior_process_id = upload_comment.split()[-1]
            elif 'Archive upload for' in upload_comment:
                run_type = 'o'
                zip_process_id = upload_comment.split()[-1]
            filename = row.get('data_file')

            # check if it is a parent process id
            if not prior_process_id and not zip_process_id:
                mail_uuid[upload_id] = row.get('upload_token')

            tasks[upload_id] = {'task': Task(filename,
                                             upload_id,
                                             prior_process_id,
                                             zip_process_id,
                                             run_type, site_id),
                                'retry': 0}
        return tasks, mail_uuid

    def run(self):
        mail_uuids = {}
        with open(self.log_file_path, 'w+') as log:
            (tasks,
             mail_uuid) = self.get_new_data_upload_log(log)
            mail_uuids.update(mail_uuid)
            stop_run = False
            while True:
                if self.is_test:
                    if not tasks:
                        self.stale_count += 1
                        log.write(('[TEST MODE] Empty run '
                                   f'{self.stale_count} time(s)\n'))
                        print(self.stale_count)
                    if self.stale_count >= 3:
                        stop_run = True
                if stop_run:
                    break
                # run tasks
                while tasks:
                    process_id_mapping = {}
                    sub_uuid_mapping = {}
                    if not self.is_test:
                        pool = ThreadPool(mp.cpu_count())
                        for upload_id, v in tasks.items():
                            task, _ = v.values()
                            log.write((f'Start run: log id {task.upload_id}, '
                                       f'prior id: {task.prior_process_id}, '
                                       f'zip id: {task.zip_process_id}, '
                                       f'run type: {task.run_type}\n'))
                            process_id, is_upload_sucessful, sub_uuid = \
                                pool.apply_async(upload_checks,
                                                 (task.filename,
                                                  task.upload_id,
                                                  task.run_type,
                                                  task.site_id,
                                                  task.prior_process_id,
                                                  task.zip_process_id))
                        pool.close()
                        pool.join()
                        process_id_mapping.setdefault(upload_id, process_id)
                    else:
                        for upload_id, v in tasks.items():
                            task, _ = v.values()
                            log.write((f'Start run: log id {task.upload_id}, '
                                       f'prior id: {task.prior_process_id}, '
                                       f'zip id: {task.zip_process_id}, '
                                       f'run type: {task.run_type}\n'))
                            process_id, is_upload_sucessful, sub_uuid = \
                                self.upload_checks.run(task.filename,
                                                       task.upload_id,
                                                       task.run_type,
                                                       task.site_id,
                                                       task.prior_process_id,
                                                       task.zip_process_id)
                            process_id_mapping.setdefault(upload_id,
                                                          process_id)
                            sub_uuid_mapping.setdefault(upload_id, None)
                            if sub_uuid:
                                sub_uuid_mapping[upload_id] = sub_uuid

                    for upload_id, process_id in process_id_mapping.items():
                        is_success = \
                            self.db.check_status_of_process_id(self.conn,
                                                               process_id)
                        if is_success:
                            del tasks[upload_id]
                            log.write((f'Log id: {upload_id} '
                                       'ended sucessfully\n'))
                        else:
                            retry = tasks.get(upload_id).get('retry')
                            if retry < self.max_retries:
                                tasks[upload_id]['retry'] += 1
                                log.write((f'Log id: {upload_id}, '
                                           f'retry for {retry} '
                                           'times and failed\n'))
                            else:
                                del tasks[upload_id]
                                log.write((f'Log id: {upload_id} '
                                           'ended unsucessfully\n'))
                    (tasks,
                     mail_uuid) = self.get_new_data_upload_log(log)
                    mail_uuids.update(mail_uuid)

                # here check all tasks completed
                if not self.is_test:
                    while True:
                        if self.db.is_all_task_done(self.conn):
                            break
                    # send emails
                    pool = ThreadPool(mp.cpu_count())
                    results = []
                    mail_tokens = set(mail_uuids.keys())
                    for token in mail_tokens:
                        log.write(f'Email gen for token: {token}\n')
                        if not self.is_test:
                            cmd = ('python '
                                   f'{self.email_gen_path} '
                                   f'{token} ')
                            if self.is_test:
                                cmd = cmd + '-t'
                        else:
                            print(f'send email to token {token}')
                        results.append(pool.apply_async(self.run_proc, (cmd,)))
                    pool.close()
                    pool.join()
                    mail_uuids = {}
                else:
                    for token in mail_uuids.values():
                        log.write(f'Email gen for token: {token}\n')
                        mail_uuids = {}
                time.sleep(self.time_sleep)

            if self.is_test:
                return
            sys.exit(0)
