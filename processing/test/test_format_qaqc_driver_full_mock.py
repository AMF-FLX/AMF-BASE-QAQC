import datetime as dt
import pytest
from collections import OrderedDict
from db_handler import NewDBHandler
from format_qaqc_driver import FormatQAQCDriver
from shutil import copyfile
import csv

__author__ = 'Sy-Toan Ngo'
__email__ = 'sytoanngo@lbl.gov'


DATA_DIR = './test/testdata/format_qaqc_driver'
DATA_UPLOAD_LOG_DEFAULT = ('./test/testdata/format_qaqc_driver'
                           '/data_upload_log_default_{}.csv')
PROCESSING_LOG_DEFAULT = ('./test/testdata/format_qaqc_driver'
                          '/processing_log_default_{}.csv')
PROCESS_SUMMARIZED_OUTPUT_DEFAULT = \
    ('./test/testdata/format_qaqc_driver'
     '/process_summarized_output_default_{}.csv')
UPLOAD_CHECKS_PARAMS_DEFAULT = ('./test/testdata/format_qaqc_driver'
                                '/upload_checks_params_default.csv')

DATA_UPLOAD_LOG = './test/testdata/format_qaqc_driver/data_upload_log.csv'
PROCESS_SUMMARIZED_OUTPUT = \
    './test/testdata/format_qaqc_driver/process_summarized_output.csv'
PROCESSING_LOG = './test/testdata/format_qaqc_driver/processing_log.csv'
UPLOAD_CHECKS_PARAMS = \
    './test/testdata/format_qaqc_driver/upload_checks_params.csv'


def mock_init_db_conn(self):
    return


def mock_get_new_data_upload_log(self, uuid=None):
    new_data_upload_log = []
    if not uuid:
        with open(PROCESSING_LOG, 'r') as f:
            next(f)
            reader = csv.reader(f)
            last_row = None
            for row in reader:
                last_row = row
            current_log_id = int(last_row[0])
        with open(DATA_UPLOAD_LOG, 'r') as f:
            next(f)
            reader = csv.reader(f)
            max_current_log_id = current_log_id
            for row in reader:
                if (int(row[0]) > max_current_log_id
                        and 'repair candidate' not in row[6]):

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
                        "log_timestamp":
                            dt.datetime.fromtimestamp(float(row[13]))
                    }))
                    max_current_log_id = int(row[0])
    else:
        with open(PROCESSING_LOG, 'r') as f:
            next(f)
            reader = csv.reader(f)
            last_row = None
            for row in reader:
                last_row = row
            current_log_id = int(last_row[0])
        with open(DATA_UPLOAD_LOG, 'r') as f:
            next(f)
            reader = csv.reader(f)
            for row in reader:
                max_current_log_id = current_log_id
                if (int(row[0]) > max_current_log_id
                        and row[8] == uuid
                        and 'repair candidate' not in row[6]):
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
                        "log_timestamp":
                            dt.datetime.fromtimestamp(float(row[13]))
                    }))
                    max_current_log_id = int(row[0])
    return new_data_upload_log


def mock_upload_checks_1(file_name,
                         upload_id,
                         run_type,
                         site_id,
                         prior_process_id,
                         zip_process_id,
                         local_run):
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
    return '123', True, None


def mock_upload_checks_3(file_name,
                         upload_id,
                         run_type,
                         site_id,
                         prior_process_id,
                         zip_process_id,
                         local_run):
    if not prior_process_id:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
    if not prior_process_id:
        return '123', True, 'test_token'
    else:
        return '123', True, None


def mock_upload_checks_4(file_name,
                         upload_id,
                         run_type,
                         site_id,
                         prior_process_id,
                         zip_process_id,
                         local_run):
    if not prior_process_id and '.csv' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
    if not prior_process_id and '.csv' in file_name:
        return '123', True, 'test_token'
    else:
        return '123', True, None


def mock_upload_checks_5(file_name,
                         upload_id,
                         run_type,
                         site_id,
                         prior_process_id,
                         zip_process_id,
                         local_run):
    if not prior_process_id and '_scinot.csv' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
    if not prior_process_id and '_scinot.csv' in file_name:
        return '123', True, 'test_token'
    else:
        return '123', True, None


def mock_upload_checks_11(file_name,
                          upload_id,
                          run_type,
                          site_id,
                          prior_process_id,
                          zip_process_id,
                          local_run):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
    elif run_type != 'r':
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
                    f'repair candidate for {upload_id}',
                    1,
                    'test_token_2',
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
    if not prior_process_id and '.zip' in file_name:
        return '123', True, 'test_token'
    else:
        return '123', True, 'test_token_2'


def mock_upload_checks_12(file_name,
                          upload_id,
                          run_type,
                          site_id,
                          prior_process_id,
                          zip_process_id,
                          local_run):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
            data = [log_id+1, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
    if not prior_process_id:
        return '123', True, 'test_token'
    else:
        return '123', True, None


def mock_upload_checks_13(file_name,
                          upload_id,
                          run_type,
                          site_id,
                          prior_process_id,
                          zip_process_id,
                          local_run):
    if not prior_process_id and '.zip' in file_name:
        with open(DATA_UPLOAD_LOG, 'r') as log:
            log_id = int(log.readlines()[-1].split(',')[0]) + 1
        with open(DATA_UPLOAD_LOG, 'a+') as log:
            writer = csv.writer(log)
            data = [log_id, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
                    f'Archive upload for {upload_id}',
                    1,
                    'test_token',
                    'US-UMB_HR_200001011000_200001012000.csv',
                    None, None, None,
                    dt.datetime.now().timestamp()]
            writer.writerow(data)
            data = [log_id+1, site_id,
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
                    "dummy_userid", "dummy_username",
                    "dummy_email", 1,
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
    if not zip_process_id:
        return '123', True, 'test_token'
    else:
        return '123', True, None


def mock_upload_checks_15(file_name,
                          upload_id,
                          run_type,
                          site_id,
                          prior_process_id,
                          zip_process_id,
                          local_run):
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
    return '123', True, None


def are_files_identical(file1_path, file2_path):
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
        # Read the content of both files
        content1 = f1.read().replace('\r', '')
        content2 = f2.read().replace('\r', '')
        # Compare the content
        return content1 == content2


@pytest.mark.parametrize("case, mock_upload_checks",
                         [
                          (1, mock_upload_checks_1),
                          (2, mock_upload_checks_1),
                          (3, mock_upload_checks_3),
                          (4, mock_upload_checks_4),
                          (5, mock_upload_checks_5),
                          (11, mock_upload_checks_11),
                          (12, mock_upload_checks_12),
                          (13, mock_upload_checks_13),
                          (15, mock_upload_checks_15)
                         ])
def test_format_qaqc_driver(monkeypatch, case, mock_upload_checks):
    copyfile(DATA_UPLOAD_LOG_DEFAULT.format(case), DATA_UPLOAD_LOG)
    copyfile(PROCESSING_LOG_DEFAULT.format(case), PROCESSING_LOG)
    copyfile(PROCESS_SUMMARIZED_OUTPUT_DEFAULT.format(case),
             PROCESS_SUMMARIZED_OUTPUT)
    copyfile(UPLOAD_CHECKS_PARAMS_DEFAULT, UPLOAD_CHECKS_PARAMS)
    monkeypatch.setattr(NewDBHandler, 'init_db_conn',
                        mock_init_db_conn)
    monkeypatch.setattr(NewDBHandler, 'get_new_data_upload_log',
                        mock_get_new_data_upload_log)
    monkeypatch.setattr('format_qaqc_driver.upload_checks',
                        mock_upload_checks)
    driver = FormatQAQCDriver(test=True)
    driver.run()
    upload_check_params = \
        f'./test/testdata/format_qaqc_driver/upload_checks_params_{case}.csv'
    assert are_files_identical(upload_check_params, UPLOAD_CHECKS_PARAMS)
