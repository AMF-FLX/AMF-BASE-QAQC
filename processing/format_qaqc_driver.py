import datetime as dt
import os
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
        # self.db = db
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
            log.write(f'Run with list of log ids: {log_ids_list} '
                      '[token: {uuid}]\n')

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

            tasks[upload_id] = {'task': Task(filename,
                                             upload_id,
                                             prior_process_id,
                                             zip_process_id,
                                             run_type,
                                             site_id,
                                             token),
                                'retry': 0}
        return tasks

    def run(self):
        with open(self.log_file_path, 'w+') as log:
            o_tasks = self.get_new_upload_data(log)
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
                    for upload_id, v in o_tasks.items():
                        task, _ = v.values()
                        log.write((f'Start run: log id {task.upload_id}, '
                                   f'prior id: {task.prior_process_id}, '
                                   f'zip id: {task.zip_process_id}, '
                                   f'run type: {task.run_type}\n'))
                        process_id, is_upload_sucessful, sub_uuid = \
                            upload_checks(task.filename,
                                          task.upload_id,
                                          task.run_type,
                                          task.site_id,
                                          task.prior_process_id,
                                          task.zip_process_id,
                                          self.is_test)
                        sub_uuids = []
                        if sub_uuid:
                            sub_uuids.append(sub_uuid)
                        while sub_uuids:
                            sub_uuid = sub_uuids[0]
                            s_tasks = self.get_new_upload_data(log, sub_uuid)
                            for v in s_tasks.values():
                                s_task, _ = v.values()
                                log.write(
                                    (f'Start run: log id {s_task.upload_id}, '
                                     f'prior id: {s_task.prior_process_id}, '
                                     f'zip id: {s_task.zip_process_id}, '
                                     f'run type: {s_task.run_type}, '
                                     f'uuid: {s_task.uuid}\n'))
                                process_id, is_upload_sucessful, ss_uuid = \
                                    upload_checks(s_task.filename,
                                                  s_task.upload_id,
                                                  s_task.run_type,
                                                  s_task.site_id,
                                                  s_task.prior_process_id,
                                                  s_task.zip_process_id,
                                                  self.is_test)
                                if ss_uuid:
                                    sub_uuids.append(ss_uuid)
                            sub_uuids.remove(sub_uuid)

                        if not self.is_test:
                            cmd = ('python '
                                   f'{self.email_gen_path} '
                                   f'{task.uuid} ')
                            # self.run_proc(cmd)
                        else:
                            print(f'send email to token {task.uuid}')
                    o_tasks = self.get_new_upload_data(log)
                time.sleep(self.time_sleep)
            if self.is_test:
                return
            sys.exit(0)
