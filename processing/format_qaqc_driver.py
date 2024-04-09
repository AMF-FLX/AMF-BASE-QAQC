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
from logger import Logger
from mail_handler import Mailer
from pathlib import Path
from upload_checks import upload_checks


__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'upload_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id', 'uuid'])

_log_name_prefix = 'FormatQAQCDriver'
_log = Logger(True, None, None,
              _log_name_prefix).getLogger(_log_name_prefix)


class FormatQAQCDriver:
    def __init__(self, lookback_h=None, test=False):
        config = ConfigParser()
        with open(os.path.join(os.getcwd(), 'qaqc.cfg'), 'r') as cfg:
            cfg_section = 'FORMAT_QAQC_DRIVER'
            self.lookback_h = lookback_h
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.log_dir = config.get(cfg_section, 'log_dir')
                self.time_sleep = config.getfloat(cfg_section, 'time_sleep_s')
                self.max_retries = config.getint(cfg_section, 'max_retries')
                self.max_timeout = config.getint(cfg_section, 'max_timeout_s')
                self.timeout = self.max_timeout / 10.0
                if not lookback_h:
                    self.lookback_h = config.getfloat(cfg_section, 'lookback_h')

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
                self.qaqc_processor_email = config.get(cfg_section,
                                                       'qaqc_processor_email')
                self.amp_team_email = config.get(cfg_section,
                                                 'amp_team_email')

            cfg_section = 'JIRA'
            if config.has_section(cfg_section):
                self.email_prefix = config.get(cfg_section,
                                               'project')

            cfg_section = 'PHASE_I'
            if config.has_section(cfg_section):
                self.data_directory = config.get(cfg_section,
                                                 'data_dir')
        # Initialize logger
        _log.info('Initialized')
        self.is_test = test
        self.email_gen = EmailGen()
        self.email_amp = Mailer(_log)
        self.stale_count = 0

    def send_email_to_amp(self, msg):
        sender = self.qaqc_processor_email
        if isinstance(self.amp_team_email, str):
            receipient = list(self.amp_team_email)
        subject = 'FormatQAQCDriver Abnormal Report'
        msg = self.email_amp.build_multipart_text_msg(sender,
                                                      receipient,
                                                      subject,
                                                      msg)
        self.email_amp.send_mail(sender, receipient, msg)

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
        ac_data_upload = \
            self.db.get_undone_data_upload_log_ac(
                self.conn,
                self.qaqc_processor_source,
                self.lookback_h)
        for row in ac_data_upload:
            comment = row.get('upload_comment')
            timestamp = row.get('log_timestamp')
            uuid = None
            if ('Archive upload for' in comment
                    or 'repair candidate for' in comment):
                process_id = comment.split()[-1]
                while process_id:
                    d = self.db.trace_o_data_upload(
                        self.conn,
                        process_id)
                    if (d.get('prior_process_id')
                            or d.get('zip_process_id')):
                        process_id = d.get('log_id')
                    else:
                        uuid = d.get('upload_token')
                        o_run_data = \
                            self.db.get_latest_run_with_uuid(self.conn,
                                                             uuid)
                        if (o_run_data 
                                and 
                                (o_run_data
                                 .get('process_timestamp') 
                                 > timestamp)):
                            uuid = None
                        break
            if uuid and uuid not in rerun_uuids:
                rerun_uuids.append(uuid)
        return rerun_uuids

    def get_new_upload_data(self,
                            is_qaqc_processor=True,
                            uuid=None,
                            is_recovery=False,
                            blacklist_uuid=[]):
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
        log_ids_list = ' '.join([str(row.get('log_id'))
                                 for row in new_data_upload_log])
        if log_ids_list:
            log_msg = f'Run with list of upload log ids: {log_ids_list} '
            if uuid:
                log_msg += f'with token: {uuid}'
            _log.info(log_msg)
        tasks = {}
        for row in new_data_upload_log:
            upload_id = row.get('log_id')
            site_id = row.get('site_id')
            token = row.get('upload_token')
            if token not in blacklist_uuid:
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

    def run_upload_checks_proc(self, task, queue):
        result = upload_checks(task.filename,
                               task.upload_id,
                               task.run_type,
                               task.site_id,
                               task.prior_process_id,
                               task.zip_process_id,
                               self.is_test)
        queue.put(result)

    def run(self):
        mp_queue = mp.Queue()
        processes = []
        blacklist_uuid = []
        # run recovery process
        rerun_uuids = self.recovery_process()
        if rerun_uuids:
            rerun_uuids_str = ', '.join(rerun_uuids)
            _log.info(f'[RECOVERY MODE] Rerun for these uuids: {rerun_uuids_str}')
        else:
            _log.info(f"[RECOVERY MODE] No UUID needed to rerun")
        o_tasks = {}
        o_grouped_tasks = []
        for uuid in rerun_uuids:
            tasks, grouped_tasks = \
                self.get_new_upload_data(False,
                                         uuid=uuid,
                                         is_recovery=True)
            o_tasks.update(tasks)
            o_grouped_tasks.extend(grouped_tasks)
        # get new task
        tasks, grouped_tasks = \
            self.get_new_upload_data(False)
        o_tasks.update(tasks)
        o_grouped_tasks.extend(grouped_tasks)
        stop_run = False
        while True:
            if self.is_test:
                if not o_tasks:
                    self.stale_count += 1
                    _log.debug(('[TEST MODE] Empty run '
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
                for upload_id in upload_ids:
                    task = o_tasks.get(upload_id)
                    token = task.uuid
                    is_zip = '.zip' in task.filename
                    _log.info(
                        ('Start upload_checks with parameters:\n'
                         f'   - Upload_log log_id: {task.upload_id}\n'
                         f'   - Prior id: {task.prior_process_id}\n'
                         f'   - Zip id: {task.zip_process_id}\n'
                         f'   - Run type: {task.run_type}\n'
                         f'   - UUID: {task.uuid}'))
                    p = mp.Process(target=self.run_upload_checks_proc,
                                   args=(task,
                                         mp_queue))
                    p.start()
                    processes.append(
                        {'process': p,
                         'runtime': 0,
                         'retry': 0,
                         'task': task,
                         'run_status': True})
                while processes:
                    time.sleep(self.time_sleep)
                    s_processes = []
                    for p in processes:
                        if not p.get('process').is_alive() and p.get('run_status'):
                            p['run_status'] = False
                            result = mp_queue.get()
                            (process_id, is_upload_successful, uuid) = result
                            _log.info(result)
                            s_tasks = {}
                            if uuid and is_upload_successful:
                                s_tasks, _ = self.get_new_upload_data(True, uuid)
                                if is_zip and len(s_tasks) > 1:
                                    token = uuid
                            for task in s_tasks.values():
                                _log.info(
                                    ('Start upload_checks with parameters:\n'
                                    f'   - Upload_log log_id: {task.upload_id}\n'
                                    f'   - Prior id: {task.prior_process_id}\n'
                                    f'   - Zip id: {task.zip_process_id}\n'
                                    f'   - Run type: {task.run_type}\n'
                                    f'   - UUID: {task.uuid}'))
                                s_p = mp.Process(target=self.run_upload_checks_proc,
                                                    args=(task,
                                                        mp_queue))
                                s_p.start()
                                s_processes.append(
                                    {'process': s_p,
                                    'runtime': 0,
                                    'retry': 0,
                                    'task': task,
                                    'run_status': True})
                            # elif not is_upload_successful:
                            #     pass
                        elif p.get('process').is_alive():
                            p['runtime'] += self.time_sleep
                            if p.get('runtime') > self.max_timeout:
                                _log.info(f"Process {p.get('task').uuid} is not done after {self.max_timeout}s...")
                                p.get('process').terminate()
                                p.get('process').join()
                                _log.info(f"Process {p.get('task').uuid} is terminated")
                                retry = p.get('retry')
                                if retry >= self.max_retries:
                                    is_qaqc_successful = False
                                    blacklist_uuid.append(p.get('task').uuid)
                                    _log.info(f"Process {p.get('task').uuid} {self.max_retries} retries reached, add to blacklist")
                                else:
                                    p['retry'] = retry + 1
                                    p['runtime'] = 0
                                    task = p.get('task')
                                    s_p = mp.Process(target=self.run_upload_checks_proc,
                                                    args=(task,
                                                        mp_queue))
                                    s_p.start()
                                    p['process'] = s_p
                                    s_processes.append(p)
                                    _log.info(f"Process {p.get('task').uuid} retry number: {p['retry']}/{self.max_retries}")
                            else:
                                s_processes.append(p)
                        else:
                            s_processes.append(p)
                    processes = s_processes
                # it will get here if all good, send out email to token
                if is_qaqc_successful and token:
                    _log.info(f'UUID {token} is executed sucessfully, now sending email to the team...')
                    try:
                        _log.info(f'[EMAIL] Running email gen for token {token}...')
                        msg = self.email_gen.driver(token)
                        if msg.startswith(self.email_prefix):
                            _log.info(f'[EMAIL] Email gen for token: {token}  - Success!\n'
                                      f'   - Message: {msg}')
                        else:
                            _log.info(f'[EMAIL] Email gen for token: {token}  - Failed!\n'
                                      f'   - Message: {msg}')
                            _log.debug('[EMAIL AMP] Sending email to AMP for token: '
                                    f'{token}')
                            self.send_email_to_amp(msg)
                            _log.debug('[EMAIL AMP] Sent email to AMP')
                    except EmailGenError:
                        # send email to AMP
                        _log.info(f'[EMAIL] Email gen for token: {token}  - Throw error!\n'
                                  f'   - Message: {msg}')
                        _log.debug('[EMAIL AMP] Sending email to AMP for token: '
                                   f'{token}')
                        self.send_email_to_amp(msg)
                        _log.debug('[EMAIL AMP] Sent email to AMP')
                else:
                    # send email to AMP
                    _log.info(f'UUID {token} is failed to execute, sending email to AMP...')
                    _log.debug('[EMAIL AMP] Sending email to AMP for token: '
                               f'{token}')
                    self.send_email_to_amp(msg)
                    _log.debug('[EMAIL AMP] Sent email to AMP')
            time.sleep(self.time_sleep)
            o_tasks, o_grouped_tasks = \
                self.get_new_upload_data(False, blacklist_uuid=blacklist_uuid)
        if self.is_test:
            return
        sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Format QAQC Driver')
    parser.add_argument(
        '--lookback_h', type=float, help='Look back x hours to check '
                                     'for any unfinished work')
    parser.add_argument(
        '-t', '--test', action='store_true', default=False,
        help='Sets flag for local run that does not write'
             ' to database')
    args = parser.parse_args()
    driver = FormatQAQCDriver(lookback_h=args.lookback_h,
                              test=args.test)
    driver.run()
