#!/usr/bin/env python

import argparse
import datetime as dt
import os
import sys

from configparser import ConfigParser
from typing import Union

from db_handler import DBHandler

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


def configure_dates(
        current_date_arg: str, days_filter: int) -> (str, str, str):
    current_date = dt.datetime.now()
    if current_date_arg:
        current_date = dt.datetime.strptime(current_date_arg, '%Y-%m-%d')
    query_date = current_date - dt.timedelta(days=days_filter)
    query_date = query_date.strftime('%Y-%m-%d')
    log_file_date = current_date.strftime('%Y-%m-%d')
    return current_date, query_date, log_file_date


def init_from_config(
        cfg_path: str, is_test: bool, log) -> (
        Union[DBHandler, None], Union[DBHandler, None], Union[str, None]):
    jira_db_handler, qaqc_db_handler, jira_project = None, None, None
    config = ConfigParser()
    with open(cfg_path, 'r') as cfg:
        cfg_section = 'DB'
        config.read_file(cfg)
        if config.has_section(cfg_section):
            db_hostname = config.get(cfg_section, 'flux_hostname')
            db_user = config.get(cfg_section, 'flux_user')
            db_auth = config.get(cfg_section, 'flux_auth')
            jira_db_name = config.get(cfg_section, 'jira_db_name')
            qaqc_db_name = config.get(cfg_section, 'flux_db_name')
            jira_db_handler = DBHandler(
                hostname=db_hostname, user=db_user,
                password=db_auth, db_name=jira_db_name)
            qaqc_db_handler = DBHandler(
                hostname=db_hostname, user=db_user,
                password=db_auth, db_name=qaqc_db_name)
        else:
            log.write('ERROR: DB configuration failed.')
            if not is_test:
                sys.exit(-1)
        cfg_section = 'JIRA'
        config.read_file(cfg)
        if config.has_section(cfg_section):
            jira_project = config.get(cfg_section, 'project')
        else:
            log.write('ERROR: JIRA configuration failed.')
            if not is_test:
                sys.exit(-1)

    return jira_db_handler, qaqc_db_handler, jira_project


def check_processed_uploads(process_info_store: dict, upload_file_store: dict,
                            log) -> (dict, list):
    processed_upload_id_lookup = {}
    for process_id, process_info in process_info_store.items():
        upload_id = process_info.get('upload_id')
        # uploaded files can be processed more than once. this is rare.
        processed_upload_id_lookup.setdefault(
            upload_id, []).append(process_id)

    uploads_not_processed = []
    processed_lookup = {}
    for upload_id in upload_file_store.keys():
        if upload_id not in processed_upload_id_lookup.keys():
            uploads_not_processed.append(upload_id)
            continue
        for process_id in processed_upload_id_lookup[upload_id]:
            processed_lookup[process_id] = upload_id

    # write message to log file
    if uploads_not_processed:
        num_uploads_not_processed = len(uploads_not_processed)
        log.write(f'ERROR: {num_uploads_not_processed} upload(s) '
                  'not processed through Format QA/QC\n'
                  '***********************************\n')
        for upload_id in uploads_not_processed:
            upload_info = upload_file_store[upload_id]
            site_id = upload_info['site_id']
            uploader = upload_info['uploader_name']
            upload_date = upload_info['upload_date']
            log.write(f'Upload id {upload_id}: {site_id} on {upload_date} '
                      f'by {uploader}\n')
        log.write('***********************************\n'
                  '***********************************\n\n')

    return processed_lookup, uploads_not_processed


def check_jira_issues(processed_lookup: dict, jira_process_ids: list,
                      upload_file_store: dict, log) -> dict:
    upload_tokens_missing_jira_issue = {}
    for process_id, upload_id in processed_lookup.items():
        if process_id not in jira_process_ids:
            upload_token = upload_file_store[upload_id]['upload_token']
            upload_tokens_missing_jira_issue.setdefault(
                upload_token, []).append(process_id)

    if upload_tokens_missing_jira_issue:
        num_uploads_missing_jira_issue = len(
            upload_tokens_missing_jira_issue)
        log.write(f'ERROR: {num_uploads_missing_jira_issue} upload(s) '
                  'processed through Format QA/QC '
                  'but missing JIRA Format issues\n'
                  '***********************************\n')
        for upload_token in upload_tokens_missing_jira_issue.keys():
            process_ids = ', '.join(
                upload_tokens_missing_jira_issue[upload_token])
            log.write(f'Upload token {upload_token} with process_id(s): '
                      f'{process_ids}\n')

    return upload_tokens_missing_jira_issue


def main():
    """
    Compare JIRA Format QA/QC issues against uploads and processing runs
    :return:
    """
    parser = argparse.ArgumentParser(
        description='Check uploads and Format QA/QC process runs for '
                    'dropped uploads and process runs. Execute from ')
    parser.add_argument('cfg_path', type=str, help='Configuration file path')
    parser.add_argument('log_dir', type=str, help='Log file directory')
    parser.add_argument('--days', '-d', type=int, default=3,
                        help='Number of days prior to check')
    parser.add_argument('--current_date', '-c', type=str,
                        help='Date to use as current date: YYYY-MM-DD')
    parser.add_argument('--test', '-t', action='store_true',
                        help='testing flag')
    args = parser.parse_args()
    is_test = args.test

    current_date, query_date, log_file_date = configure_dates(
        args.current_date, args.days)
    log_file_path = os.path.join(
        args.log_dir, f'qaqc_process_check_log_{log_file_date}.log')

    with open(log_file_path, 'w') as log:
        if not os.path.isfile(args.cfg_path):
            log.write('ERROR: Config file path not found.')
            if is_test:
                return
            sys.exit(-1)

        # configure database and JIRA connections
        jira_db_handler, qaqc_db_handler, jira_project = init_from_config(
            args.cfg_path, is_test, log)

        if not all([jira_project, qaqc_db_handler, jira_project]) and is_test:
            return

        # get list of Half Hour files uploaded in last x days
        upload_file_store = qaqc_db_handler.get_fp_in_uploads(
            date_created_after=query_date)

        # check for format processing in last x days
        process_info_store = qaqc_db_handler.get_format_qaqc_process_attempts(
            date_created_after=query_date)

        processed_lookup, uploads_not_processed = check_processed_uploads(
            process_info_store, upload_file_store, log)

        # check JIRA issues for last x days
        jira_process_ids = \
            jira_db_handler.get_process_ids_from_jira_format_issues(
                date_created_after=query_date, jira_project=jira_project)

        upload_tokens_missing_jira_issue = check_jira_issues(
            processed_lookup, jira_process_ids, upload_file_store, log)

        if not uploads_not_processed and not upload_tokens_missing_jira_issue:
            log.write('Super! All uploads processed. '
                      'All process attempts reported in a JIRA issue.')
    if is_test:
        return
    sys.exit(0)


if __name__ == '__main__':
    main()
