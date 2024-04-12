import ast
import argparse
import datetime
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
from utils import TimestampUtil


__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'

Task = namedtuple('Task', ['filename', 'upload_id',
                           'prior_process_id', 'zip_process_id',
                           'run_type', 'site_id', 'uuid'])

AMP_EMAIL_TEMPLATE = (
                '- Datetime: {timestamp}\n'
                '- Token: {token}\n'
                '- Site id: {site_id}\n'
                '- Upload id: {upload_id}\n'
                '- Log path: {log_path}\n\n'
                'Has this error message:\n'
                '   - {msg}')
ARCHIVE_COMMENT = 'Archive upload for'
REPAIR_COMMENT = 'repair candidate for'

_log_name_prefix = 'FormatQAQCDriver'
_log = Logger(True, None, None,
              _log_name_prefix).getLogger(_log_name_prefix)


class FormatQAQCDriver:
    def __init__(self, lookback_h=None, test=False):
        self.ts_util = TimestampUtil()
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
                    self.lookback_h = \
                        config.getfloat(cfg_section, 'lookback_h')
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
                try:
                    self.amp_team_email = \
                        ast.literal_eval(config.get(cfg_section,
                                                    'amp_team_email'))
                except Exception:
                    self.amp_team_email = []

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
        self.error_uuid = []

    def send_email_to_amp(self, msg, token, site_id, upload_id=None):
        sender = self.qaqc_processor_email
        receipient = self.amp_team_email
        timestamp = datetime.datetime.now().strftime(
            self.ts_util.JIRA_TS_FORMAT)
        if not upload_id:
            upload_id = 'Not Available'
        if receipient:
            subject = '[AMP] FormatQAQCDriver Abnormal Report'
            body_content = AMP_EMAIL_TEMPLATE.format(
                timestamp=timestamp,
                token=token,
                site_id=site_id,
                upload_id=upload_id,
                log_path=_log.default_log,
                msg=msg)
            content = \
                self.email_amp.build_multipart_text_msg(sender,
                                                        receipient,
                                                        subject,
                                                        body_content)
            self.email_amp.send_mail(sender, receipient, content)
        else:
            _log.warning('[EMAIL] AMP receipient is not configured')

    def recovery_process(self):
        rerun_uuids = []
        user_data_upload = \
            self.db.get_incomplete_user_data_upload(
                self.conn,
                self.qaqc_processor_source,
                self.lookback_h)
        for row in user_data_upload:
            uuid = row.get('upload_token')
            if uuid not in rerun_uuids:
                rerun_uuids.append(uuid)
        ac_data_upload = \
            self.db.get_incomplete_system_data_upload(
                self.conn,
                self.qaqc_processor_source,
                self.lookback_h)
        for row in ac_data_upload:
            comment = row.get('upload_comment')
            timestamp = row.get('log_timestamp')
            uuid = None
            if (ARCHIVE_COMMENT in comment
                    or REPAIR_COMMENT in comment):
                process_id = comment.split()[-1]
                while process_id:
                    d = self.db.trace_user_data_upload(
                        self.conn,
                        process_id)
                    prior_process_id = d.get('prior_process_id')
                    zip_process_id = d.get('zip_process_id')
                    if prior_process_id:
                        process_id = prior_process_id
                    elif zip_process_id:
                        process_id = zip_process_id
                    else:
                        uuid = d.get('upload_token')
                        user_run_data = \
                            self.db.get_latest_run_with_uuid(self.conn,
                                                             uuid)
                        if (user_run_data
                                and
                                (user_run_data
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
                            is_recovery=False):
        if is_recovery:
            new_data_upload_log = \
                self.db.get_data_upload_with_uuid(
                    self.conn,
                    uuid)
        else:
            new_data_upload_log = \
                self.db.get_new_data_upload(self.conn,
                                            self.qaqc_processor_source,
                                            is_qaqc_processor,
                                            uuid)
        log_ids_list = ' '.join([str(row.get('log_id'))
                                 for row in new_data_upload_log])
        if log_ids_list:
            log_msg = f'Run with list of upload log ids: {log_ids_list} '
            if uuid:
                log_msg += f'with token: {uuid}'
            if is_recovery:
                log_msg = f'[RECOVERY MODE] {log_msg}'
            else:
                log_msg = f'[REGULAR MODE] {log_msg}'
            _log.info(log_msg)
        tasks = {}
        for row in new_data_upload_log:
            upload_id = row.get('log_id')
            site_id = row.get('site_id')
            token = row.get('upload_token')
            if token not in self.error_uuid:
                zip_process_id = None
                prior_process_id = None
                run_type = 'o'
                upload_comment = row.get('upload_comment', '')
                if REPAIR_COMMENT in upload_comment:
                    run_type = 'r'
                    prior_process_id = upload_comment.split()[-1]
                elif ARCHIVE_COMMENT in upload_comment:
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
            grouped_tasks.setdefault(token, []).append(upload_id)
        grouped_upload_id = list(grouped_tasks.values())
        return tasks, grouped_upload_id

    def run_upload_checks_proc(self, task, queue):
        try:
            result = upload_checks(task.filename,
                                   task.upload_id,
                                   task.run_type,
                                   task.site_id,
                                   task.prior_process_id,
                                   task.zip_process_id,
                                   self.is_test)
            queue.put(result)
        except Exception as e:
            queue.put(str(e))

    def run(self):
        mp_queue = mp.Queue()
        processes = []
        # run recovery process
        rerun_uuids = self.recovery_process()
        if rerun_uuids:
            rerun_uuids_str = ', '.join(rerun_uuids)
            _log.info('[RECOVERY MODE] Rerun for these uuids: '
                      f'{rerun_uuids_str}')
        else:
            _log.info('[RECOVERY MODE] No UUID needed to rerun')
        user_tasks = {}
        user_tasks_group_by_uuid = []
        for uuid in rerun_uuids:
            tasks, tasks_group_by_uuid = \
                self.get_new_upload_data(False,
                                         uuid=uuid,
                                         is_recovery=True)
            user_tasks.update(tasks)
            user_tasks_group_by_uuid.extend(tasks_group_by_uuid)
        # get new task
        tasks, tasks_group_by_uuid = \
            self.get_new_upload_data(False)
        user_tasks.update(tasks)
        user_tasks_group_by_uuid.extend(tasks_group_by_uuid)
        stop_run = False
        while True:
            if self.is_test:
                if not user_tasks:
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
            for upload_ids in user_tasks_group_by_uuid:
                is_qaqc_successful = True
                # placeholder for error token and error msg
                # if upload_checks throws and error
                error_msg = None
                error_task = None
                for upload_id in upload_ids:
                    task = user_tasks.get(upload_id)
                    token = task.uuid
                    site_id = task.site_id
                    is_zip = '.zip' in task.filename
                    _log.info(
                        ('Start upload_checks with parameters:\n'
                         f'   - Site id: {site_id}\n'
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
                    processes_to_monitor = []
                    for p in processes:
                        if (not p.get('process').is_alive()
                                and p.get('run_status')):
                            p['run_status'] = False
                            result = mp_queue.get()
                            # a normal result from upload_checks will have:
                            # process_id, is_upload_successful, uuid
                            # otherwise there will be only the msg error
                            if len(result) == 3:
                                (process_id,
                                 is_upload_successful,
                                 uuid) = result
                                s_tasks = {}
                                if uuid and is_upload_successful:
                                    s_tasks, _ = \
                                        self.get_new_upload_data(True, uuid)
                                    if is_zip and len(s_tasks) > 1:
                                        token = uuid
                                for task in s_tasks.values():
                                    _log.info(
                                        'Start upload_checks '
                                        'with parameters:\n'
                                        f'   - Site id: {site_id}\n'
                                        '   - Upload_log log_id: '
                                        f'{task.upload_id}\n'
                                        '   - Prior id: '
                                        f'{task.prior_process_id}\n'
                                        f'   - Zip id: {task.zip_process_id}\n'
                                        f'   - Run type: {task.run_type}\n'
                                        f'   - UUID: {task.uuid}')
                                    new_p = mp.Process(
                                        target=self.run_upload_checks_proc,
                                        args=(task, mp_queue))
                                    new_p.start()
                                    processes_to_monitor.append(
                                        {'process': new_p,
                                         'runtime': 0,
                                         'retry': 0,
                                         'task': task,
                                         'run_status': True})
                            else:
                                # some upload_checks throw error
                                # send message to AMP
                                is_qaqc_successful = False
                                error_msg = result
                                error_task = p.get('task')
                                self.error_uuid.append(error_task.uuid)
                        elif p.get('process').is_alive():
                            p['runtime'] += self.time_sleep
                            if p.get('runtime') > self.max_timeout:
                                _log.info(f"Process {p.get('task').uuid} "
                                          'is not done after '
                                          f"{self.max_timeout}s...")
                                p.get('process').terminate()
                                p.get('process').join()
                                _log.info(f"Process {p.get('task').uuid} "
                                          'is terminated')
                                retry = p.get('retry')
                                if retry >= self.max_retries:
                                    is_qaqc_successful = False
                                    error_task = p.get('task')
                                    error_msg = (f'Process {error_task.uuid} '
                                                 f'{self.max_retries} '
                                                 'retries reached. '
                                                 'Stop running this process')
                                    _log.info(error_msg)
                                    self.error_uuid.append(error_task.uuid)
                                else:
                                    retry_count = retry + 1
                                    p['retry'] = retry_count
                                    p['runtime'] = 0
                                    task = p.get('task')
                                    new_p = mp.Process(
                                        target=self.run_upload_checks_proc,
                                        args=(task, mp_queue))
                                    new_p.start()
                                    p['process'] = new_p
                                    processes_to_monitor.append(p)
                                    _log.info(
                                        f'Process {task.uuid} '
                                        'retry number: '
                                        f'{retry_count}/{self.max_retries}')
                            else:
                                processes_to_monitor.append(p)
                        else:
                            processes_to_monitor.append(p)
                    processes = processes_to_monitor
                # it will get here if all good, send out email to token
                email_gen_status = None
                if is_qaqc_successful and token:
                    _log.info(f'[STATUS] UUID {token} '
                              'is executed sucessfully, '
                              'sending email to the team...')
                    _log.info('[EMAIL] Running email gen '
                              f'for token {token}...')
                    try:
                        msg = self.email_gen.driver(token)
                        if msg.startswith(self.email_prefix):
                            email_gen_status = 'Success'
                        else:
                            email_gen_status = 'Failed'
                    except EmailGenError as e:
                        msg = str(e)
                        email_gen_status = 'Error'
                else:
                    if not error_msg:
                        msg = 'Unknown Error'
                    token = error_task.uuid
                    upload_id = error_task.upload_id
                if email_gen_status:
                    _log.info('[EMAIL] Email gen for token: '
                              f'{token} - {email_gen_status}!\n'
                              f'   - Message: {msg}')
                else:
                    _log.info(f'[STATUS] UUID {token} is failed to execute, '
                              'sending email to AMP...')
                if email_gen_status != 'Success':
                    _log.debug('[EMAIL AMP] Sending email '
                               f'to AMP for token: {token}')
                    self.send_email_to_amp(msg, token, site_id)
                    _log.debug('[EMAIL AMP] Sent email to AMP')

            time.sleep(self.time_sleep)
            user_tasks, user_tasks_group_by_uuid = \
                self.get_new_upload_data(False)
            if user_tasks_group_by_uuid:
                _log.info('***Found new tasks!***')
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
