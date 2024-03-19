import os
import datetime as dt
import sys
import pytest
from collections import OrderedDict
from db_handler import NewDBHandler
from format_qaqc_driver import FormatQAQCDriver
from shutil import copyfile
import csv
from format_qaqc_driver import UploadChecks
import hashlib

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'


DATA_DIR = './test/testdata/format_qaqc_driver'
DATA_UPLOAD_LOG_DEFAULT = './test/testdata/format_qaqc_driver/data_upload_log_default_{}.csv'
PROCESSING_LOG_DEFAULT = './test/testdata/format_qaqc_driver/processing_log_default_{}.csv'
PROCESS_SUMMARIZED_OUTPUT_DEFAULT = './test/testdata/format_qaqc_driver/process_summarized_output_default_{}.csv'
UPLOAD_CHECKS_PARAMS_DEFAULT = './test/testdata/format_qaqc_driver/upload_checks_params_default.csv'

DATA_UPLOAD_LOG = './test/testdata/format_qaqc_driver/data_upload_log.csv'
PROCESS_SUMMARIZED_OUTPUT = './test/testdata/format_qaqc_driver/process_summarized_output.csv'
PROCESSING_LOG = './test/testdata/format_qaqc_driver/processing_log.csv'
UPLOAD_CHECKS_PARAMS = './test/testdata/format_qaqc_driver/upload_checks_params.csv'

def mock_init_db_conn(self):
    return

def mock_get_new_data_upload_log(self):
    print(os.getcwd())
    new_data_upload_log = []
    with open(PROCESSING_LOG, 'r') as f:
        next(f)
        reader = csv.reader(f)
        last_row = None
        for row in reader:
            last_row = row
        current_log_id = int(last_row[0])
    print(f'current log id is: {current_log_id}', file=sys.stdout)
    with open(DATA_UPLOAD_LOG, 'r') as f:
        next(f)
        reader = csv.reader(f)
        max_current_log_id = current_log_id
        for row in reader:
            if int(row[0]) > max_current_log_id:
                new_data_upload_log.append(OrderedDict({
                    "log_id": int(row[0]),
                    "site_id": row[1],
                    "user_id": row[2],
                    "user_name": row[3],
                    "user_email": row[4],
                    "upload_type_id": int(row[5]),
                    "upload_comment": row[6],
                    "upload_source_id": int(row[7]),
                    "upload_token": row[8],
                    "data_file": row[9],
                    "metadata_file": row[10],
                    "data_format": row[11],
                    "year": int(row[12]) if row[12] else None,
                    "log_timestamp": dt.datetime.fromtimestamp(float(row[13]))
                }))
                max_current_log_id = int(row[0])
        #     print(row)
        # print('##################')
    return new_data_upload_log

def mock_upload_checks_run_1(self,
                             file_name,
                             upload_id,
                             run_type,
                             site_id,
                             prior_process_id,
                             zip_process_id):
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_3(self,
                             file_name,
                             upload_id,
                             run_type,
                             site_id,
                             prior_process_id,
                             zip_process_id):
    if not prior_process_id:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'repair candidate for {upload_id}',
                    1,
                    'test_token',
                    file_name, None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_4(self,
                             file_name,
                             upload_id,
                             run_type,
                             site_id,
                             prior_process_id,
                             zip_process_id):
    if not prior_process_id and '.csv' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'repair candidate for {upload_id}',
                    1,
                    'test_token',
                    file_name, None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_5(self,
                             file_name,
                             upload_id,
                             run_type,
                             site_id,
                             prior_process_id,
                             zip_process_id):
    if not prior_process_id and '_scinot.csv' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'repair candidate for {upload_id}',
                    1,
                    'test_token',
                    file_name, None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_11(self,
                              file_name,
                              upload_id,
                              run_type,
                              site_id,
                              prior_process_id,
                              zip_process_id):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_12(self,
                              file_name,
                              upload_id,
                              run_type,
                              site_id,
                              prior_process_id,
                              zip_process_id):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
            data = [log_id+1, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011100_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_13(self,
                              file_name,
                              upload_id,
                              run_type,
                              site_id,
                              prior_process_id,
                              zip_process_id):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
            data = [log_id+1, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001010000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    elif zip_process_id and '.csv' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "toanngo", "Sy-Toan Ngo",
                    "sytoanngo@lbl.gov", 1,
                    f'repair candidate for {upload_id}',
                    1,
                    'test_token',
                    file_name,
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def mock_upload_checks_run_15(self,
                              file_name,
                              upload_id,
                              run_type,
                              site_id,
                              prior_process_id,
                              zip_process_id):
    with open(PROCESSING_LOG, 'r') as log:
        log_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESSING_LOG, 'a+') as log:
        writer = csv.writer(log)
        data = [log_id,
                1,
                dt.datetime.now().timestamp(),
                upload_id,
                site_id,
                None,
                'dummy id',
                None,
                prior_process_id if prior_process_id else None,
                zip_process_id if zip_process_id else None,
                0]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(PROCESS_SUMMARIZED_OUTPUT, 'r') as log:
        output_id = int(log.readlines()[-1].split(',')[0]) + 1
    with open(PROCESS_SUMMARIZED_OUTPUT, 'a+') as log:
        writer = csv.writer(log)
        data = [output_id,
                1,
                [],
                '',
                {},
                None,
                None,
                {}]
        writer.writerow(data)
    with open(UPLOAD_CHECKS_PARAMS, 'a+') as log:
        writer = csv.writer(log)
        data = [file_name,
                upload_id,
                run_type,
                site_id,
                prior_process_id,
                zip_process_id]
        writer.writerow(data)
    return None

def are_files_identical(file1_path, file2_path):
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
        # Read the content of both files
        content1 = f1.read().replace('\r', '')
        content2 = f2.read().replace('\r', '')
        # Compare the content
        return content1 == content2

@pytest.fixture
def qaqc_driver_1(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(1), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(1), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(1), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_1)
    driver = FormatQAQCDriver(test=True)
    return driver


def test_format_qaqc_driver(qaqc_driver_1):
    qaqc_driver_1.run()
    upload_check_params = './test/testdata/format_qaqc_driver/upload_checks_params_1.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)


@pytest.fixture
def qaqc_driver_2(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(2), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(2), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(2), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_1)
    driver = FormatQAQCDriver(test=True)
    return driver


def test_format_qaqc_driver(qaqc_driver_2):
    qaqc_driver_2.run()
    upload_check_params = './test/testdata/format_qaqc_driver/upload_checks_params_2.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

@pytest.fixture
def qaqc_driver_3(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(3), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(3), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(3), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_3)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_4(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(4), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(4), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(4), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_4)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_5(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(5), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(5), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(5), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_5)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_11(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(11), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(11), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(11), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_11)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_12(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(12), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(12), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(12), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_12)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_13(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(13), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(13), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(13), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_13)
    driver = FormatQAQCDriver(test=True)
    return driver

@pytest.fixture
def qaqc_driver_15(monkeypatch):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(15), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(15), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(15), PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr(UploadChecks, 'run',
                        mock_upload_checks_run_15)
    driver = FormatQAQCDriver(test=True)
    return driver

# @pytest.mark.parametrize("case,  driver", [(13, qaqc_driver_13), (13, qaqc_driver_15)])
def test_format_qaqc_driver_1(qaqc_driver_1):
    qaqc_driver_1.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_1.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_2(qaqc_driver_2):
    qaqc_driver_2.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_2.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_3(qaqc_driver_3):
    qaqc_driver_3.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_3.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_4(qaqc_driver_4):
    qaqc_driver_4.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_4.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_5(qaqc_driver_5):
    qaqc_driver_5.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_5.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_11(qaqc_driver_11):
    qaqc_driver_11.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_11.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_12(qaqc_driver_12):
    qaqc_driver_12.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_12.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_13(qaqc_driver_13):
    qaqc_driver_13.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_13.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)

def test_format_qaqc_driver_15(qaqc_driver_15):
    qaqc_driver_15.run()
    upload_check_params = f'./test/testdata/format_qaqc_driver/upload_checks_params_15.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)
