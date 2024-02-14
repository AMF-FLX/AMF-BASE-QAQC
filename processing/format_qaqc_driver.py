import datetime as dt
import multiprocessing as mp
import os
import subprocess
import shlex
import sys
import time

from configparser import ConfigParser
from collections import namedtuple
from db_handler import NewDBHandler

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'process_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id'])


class FormatQAQCDriver:
    def __init__(self, test=True):
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
                self.db = NewDBHandler(hostname=hostname,
                                       user=user,
                                       password=auth,
                                       db_name=db_name)

        log_file_date = dt.datetime.now().strftime('%Y-%m-%d')
        log_file_name = f'format_qaqc_driver_service_{log_file_date}.log'
        log_dir = os.path.join(os.getcwd(), self.log_dir)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        self.log_file_path = os.path.join(os.getcwd(),
                                          self.log_dir,
                                          log_file_name)
        self.is_test = test
        self.upload_checks_path = "./upload_checks.py"
        self.email_gen_path = "./email_gen.py"

    def run_proc(self, cmd):
        p = subprocess.Popen(shlex.split(cmd),
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = p.communicate()
        return (out, err)

    def run(self):
        with open(self.log_file_path, 'w+') as log:
            stale_count = 0
            while True:
                upload_ids_list = []
                new_data_upload_log = self.db.get_new_data_upload_log()

                # if no more new data upload log in test mode
                # terminate after 3 empty rounds
                if self.is_test:
                    if not new_data_upload_log:
                        stale_count += 1
                        log.write(("[TEST MODE] Empty run "
                                   f"{stale_count} time(s)\n"))
                        print(stale_count)
                    if stale_count >= 3:
                        break

                log_ids_list = ' '.join([str(row['log_id'])
                                         for row in new_data_upload_log])
                if log_ids_list:
                    log.write(f"Run with list of log ids: {log_ids_list}\n")

                upload_tokens = []
                tasks = {}
                for row in new_data_upload_log:
                    upload_id = row['log_id']
                    site_id = row['site_id']
                    # timestamp = row['log_timestamp']
                    upload_tokens.append(row['upload_token'])

                    # get origin, zip, repair run_type
                    zip_process_id = None
                    prior_process_id = None
                    run_type = 'o'
                    if 'repair candidate for' in row['upload_comment']:
                        run_type = 'r'
                        prior_process_id = row['upload_comment'].split()[-1]
                    elif 'Archive upload for' in row['upload_comment']:
                        run_type = 'o'
                        zip_process_id = row['upload_comment'].split()[-1]
                    filename = row['data_file']
                    if zip_process_id:
                        filename = os.path.join(self.data_dir, site_id,
                                                zip_process_id, filename)
                    elif prior_process_id:
                        filename = os.path.join(self.data_dir, site_id,
                                                prior_process_id, filename)
                    else:
                        filename = os.path.join(self.data_dir, site_id,
                                                filename)

                    tasks[upload_id] = {'task': Task(filename, str(upload_id),
                                                     prior_process_id,
                                                     zip_process_id,
                                                     run_type, site_id),
                                        'retry': 0}
                    upload_ids_list = list(tasks.keys())

                # run tasks
                while tasks:
                    pool = mp.pool.ThreadPool(mp.cpu_count())
                    results = {}
                    for upload_id, v in tasks.items():
                        task, _ = v.values()
                        log.write((f"Start run: log id {upload_id}, "
                                   f"prior id: {prior_process_id}, "
                                   f"zip id: {zip_process_id}, "
                                   f"run type: {task.run_type}\n"))
                        cmd = ('python '
                               f'{self.upload_checks_path} '
                               f'{task.filename} '
                               f'{task.process_id} '
                               f'{task.run_type} '
                               f'{task.site_id} ')
                        if self.is_test:
                            cmd = cmd + '-t'
                        results[upload_id] = pool.apply_async(self.run_proc,
                                                              (cmd,))
                    pool.close()
                    pool.join()
                    for upload_id, result in results.items():
                        _, err = result.get()
                        retry = tasks[upload_id]['retry']
                        if err and retry < self.max_retries:
                            tasks[upload_id]['retry'] += 1
                            log.write((f"Log id: {upload_id}, "
                                       "retry for {retry} times and failed\n"))
                        else:
                            del tasks[upload_id]
                            if err:
                                log.write((f"Log id: {upload_id} "
                                           "ended unsucessfully\n"))
                            else:
                                log.write((f"Log id: {upload_id} "
                                           "ended sucessfully\n"))

                # send emails
                tokens = []
                for upload_id in upload_ids_list:
                    tokens.append(self.db.
                                  get_token_from_processing_log_id(upload_id))
                pool = mp.pool.ThreadPool(mp.cpu_count())
                results = []
                print(tokens)
                for token in tokens:
                    log.write(f"Email gen for token: {token}\n")
                    cmd = ('python '
                           f'{self.email_gen_path} '
                           f'{token} ')
                    if self.is_test:
                        cmd = cmd + '-t'
                    results.append(pool.apply_async(self.run_proc, (cmd,)))
                pool.close()
                pool.join()

                time.sleep(self.time_sleep)

            if self.is_test:
                return
            sys.exit(0)
