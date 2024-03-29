import datetime as dt
import psycopg2

import pymssql
import socket

from jira_names import JIRANames

from configparser import ConfigParser
from io import StringIO
from logger import Logger
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from psycopg2.sql import Composed, SQL
from typing import NamedTuple, Optional, Union

__author__ = 'You-Wei Cheah, Danielle Christianson'
__email__ = 'ycheah@lbl.gov, dschristianson@lbl.gov'

_log = Logger().getLogger(__name__)


class DBConfig(NamedTuple):
    host: str
    user: str
    auth: str
    db_name: str


class DBHandlerError(Exception):
    pass


class NewDBHandler:
    def __init__(self):
        self.default_null = 'NONE'
        self.default_sep = '|'
        self._cwd = Path.cwd()
        self.conn = None

    def __del__(self):
        if self.conn:
            self.conn.close()

    def _read_config(self, cfg_filename='qaqc.cfg'):
        code_ver = None
        with open(self._cwd / cfg_filename) as cfg:
            config = ConfigParser()
            config.read_file(cfg)
            cfg_section = 'VERSION'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'code_version'):
                    code_ver = config.get(
                        cfg_section, 'code_version')
            cfg_section = 'DB'
            if config.has_section(cfg_section):
                if config.has_option(cfg_section, 'hostname'):
                    hostname = config.get(cfg_section, 'hostname')
                if config.has_option(cfg_section, 'user'):
                    user = config.get(cfg_section, 'user')
                if config.has_option(cfg_section, 'auth'):
                    auth = config.get(cfg_section, 'auth')
                if config.has_option(cfg_section, 'db_name'):
                    db_name = config.get(cfg_section, 'db_name')
            db_config = DBConfig(hostname, user, auth, db_name)
        return code_ver, db_config

    def init_db_conn(self, db_cfg, use_autocommit=True):
        if db_cfg is None:
            return None
        try:
            conn = psycopg2.connect(
                host=db_cfg.host, user=db_cfg.user,
                password=db_cfg.auth, dbname=db_cfg.db_name)
            if use_autocommit:
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            return conn

        except Exception as e:
            _log.error(f'Error occurred in initializing DB connetion: {e}')
            return None

    def insert_BASE_entries(self, conn, entry_ls):
        if entry_ls is None:
            return ''
        table_name = 'base_product_log'
        insert_fields = (
            'filename', 'start_year', 'end_year', 'file_size',
            'file_checksum', 'file_path', 'create_timestamp')
        return self._insert_entries(conn, table_name, insert_fields, entry_ls)

    def insert_BASE_BADM_entries(self, conn, entry_ls):
        if entry_ls is None:
            return ''
        table_name = 'base_badm_product_log'
        insert_fields = (
            'filename', 'base_hh_version', 'base_hr_version',
            'badm_version', 'file_size', 'file_checksum', 'file_path',
            'create_timestamp')
        return self._insert_entries(conn, table_name, insert_fields, entry_ls)

    def _insert_entries(self, conn, table_name, insert_fields, entry_ls):
        insert_rows = None
        if entry_ls is None:
            return insert_rows

        _entry_ls = []
        for idx, e in enumerate(entry_ls):
            row_values = (self.default_null if v is None else v for v in e)
            _entry_ls.append(self.default_sep.join((map(str, row_values))))

        with conn.cursor() as cursor:
            try:
                fmt_values = '\n'.join(map(str, _entry_ls))
                cursor.copy_from(
                    StringIO(fmt_values), table_name,
                    null=self.default_null, sep=self.default_sep,
                    columns=insert_fields)
                insert_rows = cursor.rowcount
                info_msg = f'Rows affected using copy_from: {insert_rows}'
                _log.info(info_msg)
            except Exception as e:
                _log.error('Error occurred in inserting entries')
                _log.error(e)
                raise DBHandlerError(e)
        return insert_rows

    def get_base_badm_update_zip_count(self, conn):
        count = 0
        query = SQL('SELECT count(*) FROM base_badm_update_auto '
                    'WHERE need_update IS True')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            count = cursor.fetchone().get('count')
        return count

    def get_badm_map(self, conn):
        badm_map = {}
        query = SQL('SELECT flux_id, filename, badm_version '
                    'FROM base_badm_update_auto ba '
                    'INNER JOIN badm_product_log bl '
                    'ON ba.badm_log_id = bl.log_id')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            for r in cursor:
                flux_id = r.get('flux_id')
                badm_map[flux_id] = (r.get('filename'), r.get('badm_version'))
        return badm_map

    def get_sites_with_updates(self, conn, is_historic=False):
        lookup = {}
        args = {'cdiac': '%cdiac%'}

        if is_historic:
            query = SQL('SELECT flux_id, base_hh_version, base_hr_version '
                        'FROM base_badm_update_auto u '
                        'INNER JOIN base_product_log bl '
                        'ON (bl.log_id = u.base_hh_log_id '
                        'OR bl.log_id = u.base_hr_log_id) '
                        'AND file_path LIKE %(cdiac)s')
        else:
            query = SQL('SELECT flux_id, base_hh_version, base_hr_version '
                        'FROM base_badm_update_auto '
                        'WHERE need_update IS True')

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, args)
            for r in cursor:
                flux_id = r.get('flux_id')
                hh_version = r.get('base_hh_version')
                hr_version = r.get('base_hr_version')
                lookup[flux_id] = (hh_version, hr_version)
        return lookup

    def get_sites_with_embargo(self, conn, embargo_years):
        site_ids = []

        query = SQL('SELECT flux_id from site_embargo_log '
                    'WHERE retire_timestamp IS NULL '
                    'AND EXTRACT(YEARS FROM AGE('
                    'NOW(), request_timestamp)) < %s')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (embargo_years,))
            for r in cursor:
                site_ids.append(r.get('flux_id'))
        return site_ids

    def get_filename_checksum_lookup(self, conn):
        checksums = {}
        query = SQL('SELECT filename, file_checksum '
                    'FROM base_product_auto')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            for r in cursor:
                fname = r.get('filename')
                checksums[fname] = r.get('file_checksum')
        return checksums

    def get_new_data_upload_log(self,
                                conn,
                                qaqc_processor_email,
                                is_qaqc_processor,
                                uuid=None):
        query_str = ('SELECT u.log_id, u.site_id, '
                     'u.data_file, u.upload_token, '
                     'u.upload_comment, u.upload_type_id '
                     'FROM input_interface.data_upload_log u '
                     'LEFT JOIN qaqc.processing_log p '
                     'ON u.log_id = p.upload_id '
                     'LEFT JOIN '
                     'input_interface.data_upload_file_xfer_log x '
                     'ON u.log_id = x.upload_log_id '
                     'WHERE p.log_id IS NULL '
                     'AND u.upload_type_id IN (4, 7) '
                     'AND x.xfer_end_log_timestamp IS NOT NULL ')
        if is_qaqc_processor:
            query += \
                'AND u.user_email == \'{q}\''.format(q=qaqc_processor_email)
        else:
            query += \
                'AND u.user_email != \'{q}\''.format(q=qaqc_processor_email)
        if uuid:
            query_str += 'AND u.upload_token = \'{u}\''.format(u=uuid)
        query = SQL(query_str)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            new_data_upload = cursor.fetchall()
        return new_data_upload

    def get_unfinished_data_upload_log(self, conn):
        query_str = ('SELECT u.log_id, u.site_id, '
                     'u.data_file, u.upload_token, '
                     'u.upload_comment, u.upload_type_id '
                     'FROM input_interface.data_upload_log u '
                     'LEFT JOIN qaqc.processing_log p '
                     'ON u.log_id = p.upload_id '
                     'LEFT JOIN input_interface.process_summarized_output o'
                     'ON p.log_id = o.process_id '
                     'WHERE o.process_id IS NULL '
                     'AND u.upload_type_id IN (4, 7) ')


    def is_all_task_done(self, conn):
        is_all_done = False
        query = SQL('SELECT COUNT(DISTINCT p.log_id) AS count_log_id '
                    'FROM qaqc.processing_log p '
                    'LEFT JOIN qaqc.process_summarized_output as s '
                    'ON p.log_id = s.process_id '
                    'WHERE s.process_id IS NULL')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            status = cursor.one()
        if status == 0:
            is_all_done = True
        return is_all_done

    def check_status_of_process_id(self, conn, process_id):
        is_success = False
        query = SQL('SELECT report AS count_log_id '
                    'FROM qaqc.process_summarized_output p '
                    'where process_id = {p}'.format(p=process_id))
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            report = cursor.one()
        if report:
            is_success = True
        return is_success

    def get_upload_file_info(self, conn, upload_id):
        upload_file_info = {}
        query = SQL('SELECT * FROM input_interface.data_upload_log '
                    'WHERE log_id = %s')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (upload_id,))
            for r in cursor:
                upload_file_info['upload_id'] = r.get('log_id')
                upload_file_info['site_id'] = r.get('site_id')
                upload_file_info['upload_comment'] = r.get('upload_comment')
                upload_file_info['data_file'] = r.get('data_file')
        return upload_file_info

    def _get_type_cv(self, query, name_field, id_field='type_id') -> dict:
        _, db_config = self._read_config()
        conn = self.init_db_conn(db_config)

        cv_lookup = {}

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            for r in cursor:
                cv_name = r.get(name_field)
                cv_id = r.get(id_field)
                cv_lookup.update({cv_name: cv_id})

        return cv_lookup

    def get_qaqc_state_types(self) -> dict:
        query = SQL('SELECT * from qaqc.state_cv_type_auto;')
        return self._get_type_cv(query, 'shortname')

    def get_qaqc_process_types(self) -> dict:
        query = SQL('SELECT * from qaqc.process_type_auto;')
        return self._get_type_cv(query, 'name')

    def register_format_qaqc(self, upload_id: int, process_timestamp: str,
                             site_id: str,
                             prior_process_id: Optional[int] = None,
                             zip_process_id: Optional[int] = None) -> int:

        process_code_version, db_config = self._read_config()
        processor_user_id = socket.gethostname()

        qaqc_process_type_lookup = self.get_qaqc_process_types()
        format_qaqc_process_type_id = qaqc_process_type_lookup.get(
            'Format QAQC')

        field_names = ('process_type_id, process_timestamp, upload_id, '
                       'site_id, processor_user_id, processing_code_version')
        values = [format_qaqc_process_type_id, process_timestamp, upload_id,
                  site_id, processor_user_id, process_code_version]

        if prior_process_id:
            field_names += ', prior_process_id'
            values.append(prior_process_id)
        if zip_process_id:
            field_names += ', zip_process_id'
            values.append(zip_process_id)

        values = tuple(values)

        conn = self.init_db_conn(db_config)
        process_id = self._register_qaqc_process(conn, field_names, values)

        return process_id

    def _register_qaqc_process(self, conn, query_fields: str,
                               process_values: tuple) -> int:
        query_pre = SQL('INSERT INTO qaqc.processing_log (')
        query_post = SQL(') VALUES %(process_values)s returning log_id;')
        query = Composed([query_pre, SQL(query_fields), query_post])
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {'process_values': process_values})
            count = 0
            for r in cursor:
                process_id = r.get('log_id')
                count += 1
            if count > 1:
                raise DBHandlerError('More than 1 process_id (log_id) '
                                     'returned upon process run entry.')
            return process_id


class DBHandler:
    def __init__(self, hostname, user, password, db_name):
        self.__hostname = hostname
        self.__user = user
        self.__password = password
        self.__db_name = db_name

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def insert_BASE_entries(self, entry_ls):
        if entry_ls is None:
            return ''
        query = ("INSERT INTO BASEProductLog "
                 "VALUES(%s, %d, %d, %d, %s, %s, %s, %s, %s)")
        return self._insert_entries(query, entry_ls)

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def insert_BASE_BADM_entries(self, entry_ls):
        if entry_ls is None:
            return ''
        query = ("INSERT INTO BASEBADMProductLog "
                 "VALUES(%s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s)")
        return self._insert_entries(query, entry_ls)

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def insert_qaqc_process_entry(self, fields, entry):
        if entry is None:
            return ''
        insert_entry = ("INSERT INTO qaqcProcessingLog ({f}) VALUES ({e})"
                        .format(f=fields, e=entry))
        return self._insert_entry(entry=insert_entry)

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def insert_qaqc_file_in_base_entries(self, entry_ls):
        if entry_ls is None:
            return ''
        query = ("INSERT INTO filesInBase "
                 "(processIDSiteRes, processIDfile) "
                 "VALUES (%d, %d)")
        return self._insert_entries(query, entry_ls)

    # ToDo: update eventually
    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def insert_qaqc_data_in_base_entries(self, entry_ls):
        if entry_ls is None:
            return ''
        query = ("INSERT INTO dataInBase "
                 "(fileID, startTime, endTime) "
                 "VALUES (%d, %s, %s)")
        return self._insert_entries(query, entry_ls)

    def _insert_entries(self, query, entry_ls):
        if entry_ls is None:
            return ''
            # entry_ls = self.entry_ls
            # there is no self.entry_ls so removing it
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.executemany(query, entry_ls)
            except Exception as e:
                _log.error("Error occurred in inserting entries")
                _log.error(e)
                return 'ERROR OCCURRED'
        return ''

    def _insert_entry(self, entry):
        if entry is None:
            return ''
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(entry)
            except Exception as e:
                _log.error("Error occurred in inserting entry")
                _log.error(e)
                return 'ERROR OCCURRED'
        return ''

    # ToDo: update for publish
    def get_sites_with_updates(self):
        site_ids = {}
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:

            query = ("SELECT b.site_id, b.processID, b.baseVersion "
                     "FROM BASEPublishLog b "
                     "INNER JOIN "
                     "(SELECT site_id, max(publishTimestamp) as max_ts "
                     "FROM BASEPublishLog "
                     "GROUP BY site_id) d "
                     "ON d.site_id = b.site_id "
                     "AND d.max_ts = b.publishTimestamp")
            cursor.execute(query)
            for row in cursor:
                site_id = row.get("site_id")
                site_ids[site_id] = (
                    row.get("baseVersion"), row.get("processID"))
        return site_ids

    # ToDo: update for publish
    def get_input_files(self, processID):
        input_files = set()
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            query = ("SELECT processIDFile "
                     "FROM filesInBase "
                     "WHERE processIDSiteRes = {i}")
            cursor.execute(query.format(i=processID))
            for row in cursor:
                pid = row.get("processIDFile")
                input_files.add(pid)
        return input_files

    def define_BASE_candidate_query(
            self, pre_query, post_query, state_ids):
        query_criteria = f's.status = {state_ids[0]}'
        for state_id in state_ids[1:]:
            query_criteria += f' OR s.status = {state_id}'
        query = pre_query + query_criteria + post_query
        return query

    # ToDo: update for publish
    def get_BASE_candidates(self, state_ids):
        preBASE_files = {}
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            pre_query = ("SELECT l.basename, l.processID, l.codeVersion "
                         "FROM qaqcProcessingLog l "
                         "INNER JOIN "
                         "(SELECT s.processID, s.status "
                         "FROM qaqcState s "
                         "INNER JOIN "
                         "(SELECT processID, MAX(stateDateTime) AS dateTime "
                         "FROM qaqcState "
                         "GROUP BY processID) maxDate "
                         "ON s.stateDateTime = maxDate.dateTime "
                         "AND s.processID = maxDate.processID "
                         "WHERE ")
            post_query = ") s1 ON s1.processID = l.processID"
            query = self.define_BASE_candidate_query(pre_query=pre_query,
                                                     post_query=post_query,
                                                     state_ids=state_ids)
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    candidate_filepath = row.get("basename")
                    code_version = row.get("codeVersion")
                    process_id = row.get("processID")
                    # Remap paths if codeVersion is prior to version 1.1.0
                    if code_version < "1.1.0":
                        path = Path(candidate_filepath)
                        filename = path.name
                        parent_path_parts = path.parent.parts
                        # strip top two level directories
                        # as well as immediate parent directory
                        # and rebuild immediate parent path
                        candidate_filepath = str(
                            Path(parent_path_parts[0],
                                 *parent_path_parts[3:-1],
                                 *("outputs", "qaqc_combined"),
                                 filename))
                    preBASE_files[candidate_filepath] = process_id

            except Exception as e:
                _log.error("Error occurred in get_BASE_candidates")
                _log.error(e)
                return "ERROR OCCURRED"
        return preBASE_files

    # ToDo: rework for new tables (reset_states in PreBASERegenerator)
    def get_preBASE_regen_candidates(self, query_type='latest',
                                     process_id_ls=None):
        process_info = {}
        row_key_ls = None
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            if query_type == 'latest':
                # latest publish for each site published in new pipeline
                # This query only grabs the latest for each site; not each
                # site-res combo.
                query = ("SELECT b.* "
                         "FROM qaqcProcessingLog as b "
                         "INNER JOIN "
                         "(SELECT SITE_ID, MAX(processDatetime) as max_ts "
                         "FROM qaqcProcessingLog "
                         "WHERE baseVersion is not NULL "
                         "GROUP BY SITE_ID) as d "
                         "ON d.SITE_ID = b.SITE_ID "
                         "AND d.max_ts = b.processDatetime "
                         "WHERE b.baseVersion is not NULL")
            else:
                # all publishes in new pipeline
                query = ("SELECT * "
                         "FROM AMFDataQAQC.dbo.qaqcProcessingLog "
                         "WHERE baseVersion is not NULL")
            conn.autocommit(True)
            try:
                cursor.execute(query)
                count = 0
                for row in cursor:
                    process_id = row.get("processID")
                    if process_id_ls:
                        if process_id not in process_id_ls:
                            continue
                    process_info[process_id] = {}
                    for key in row.keys():
                        process_info[process_id][key] = row.get(key)
                    version = row.get('baseVersion')
                    version = version.replace('-', '.')
                    process_info[process_id]['version_num'] = float(version)
                    if count < 1:
                        row_key_ls = list(row.keys())
                    count += 1
            except Exception as e:
                _log.error("Error occurred in get_preBASE_regen_candidates")
                _log.error(e)
        return process_info, row_key_ls

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def get_preBASE_duplicated_process_ids(self, process_id_ls):
        republish_map = {}
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            # this query could be more specific if specify that
            # baseVersion is NULL (i.e., unpublished republished rows)
            query = ("SELECT processID, priorProcessID "
                     "FROM qaqcProcessingLog "
                     "WHERE priorProcessID is not NULL "
                     "AND processType = 'BASE'")
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    old_process_id = row.get("priorProcessID")
                    if old_process_id in process_id_ls:
                        republish_map[old_process_id] = row.get("processID")
            except Exception as e:
                _log.error("Error occurred in get_qaqc_file_in_base")
                _log.error(e)
        return republish_map

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def get_qaqc_file_in_base(self, process_id, new_process_id=None):
        file_id_list = []  # a list of fileIDs for querying dataInBase
        file_in_base_ls = []  # a list of the tuples to be inserted
        file_process_id_dict = {}  # dict for dealing with dataInBase
        # get the files in the old base pid
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            query = ("SELECT * FROM filesInBase "
                     "WHERE processIDSiteRes = {pid}"
                     .format(pid=process_id))
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    file_pid = row.get("processIDFile")
                    if file_pid not in file_process_id_dict.keys():
                        file_process_id_dict[file_pid] = []
                    file_process_id_dict[file_pid].append(row.get("fileID"))
                    file_id_list.append(row.get("fileID"))
                    if new_process_id:
                        file_in_base_ls.append((new_process_id, file_pid))
            except Exception as e:
                _log.error("Error occurred in get_qaqc_file_in_base")
                _log.error(e)
        return file_id_list, file_in_base_ls, file_process_id_dict

    # ToDo: evaluate if needs update -- could be obsolete
    #       used in PreBASERegenerator
    def get_qaqc_data_in_base(self, file_ids):
        data_in_base_ls = {}
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            query = ("SELECT * FROM dataInBase "
                     "WHERE fileID in ({f})".format(f=file_ids))
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    data_in_base_ls[row.get("fileID")] = (
                        row.get("fileID"),
                        row.get("startTime"), row.get("endTime"))
            except Exception as e:
                _log.error("Error occurred in get_qaqc_data_in_base")
                _log.error(e)
        return data_in_base_ls

    # ToDo: rework for new tables (reset_states in PreBASERegenerator)
    def get_BASE_candidates_for_preBASE_regen(self, state_ids):
        process_info = {}
        site_list = []
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            pre_query = ("SELECT a.processID, a.SITE_ID, a.max_ts, s.status "
                         "FROM qaqcState as s "
                         "INNER JOIN "
                         "(SELECT b.processID, b.SITE_ID, d.max_ts "
                         "FROM qaqcProcessingLog as b "
                         "INNER JOIN "
                         "(SELECT processID, MAX(stateDatetime) as max_ts "
                         "FROM qaqcState "
                         "GROUP by processID) as d "
                         "ON d.processID = b.processID "
                         "WHERE b.processType = 'BASE') as a "
                         "ON a.processID = s.processID "
                         "AND a.max_ts = s.stateDateTime "
                         "WHERE ")
            post_query = ""
            query = self.define_BASE_candidate_query(pre_query=pre_query,
                                                     post_query=post_query,
                                                     state_ids=state_ids)
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    site_id = row.get("SITE_ID")
                    process_id = row.get("processID")
                    if site_id not in site_list:
                        site_list.append(site_id)
                    else:
                        _log.warning('BASE candidate list contains more than '
                                     'one processID for site {s}. Not '
                                     'including processID {p} in list to '
                                     'exclude from recreateBASE processing.'
                                     .format(s=site_id, p=process_id))
                        continue
                    process_info[process_id] = {}
                    for key in row.keys():
                        process_info[process_id][key] = row.get(key)
            except Exception as e:
                _log.error("Error occurred in "
                           "get_BASE_candidates_with_preBASE_details")
                _log.error(e)
        return process_info, site_list

    # ToDo: rework for new tables (reset_states in PreBASERegenerator)
    def get_qaqc_process_ids(self, query, key='processID'):
        process_id_ls = []
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query)
                for row in cursor:
                    process_id_ls.append(row.get(key))
            except Exception as e:
                _log.error("Error occurred in get_qaqc_data_in_base")
                _log.error(e)
        return process_id_ls

    # ToDo: rework for new tables (reset_states in PreBASERegenerator)
    def get_incomplete_phase3_process_ids(self, state_ids):
        process_id_ls = []
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            query = ("SELECT b.processID "
                     "FROM (SELECT processID, MAX(stateDateTime) as max_ts "
                     "FROM qaqcState GROUP by processID) as d "
                     "INNER JOIN qaqcState as b "
                     "ON d.processID = b.processID "
                     "AND d.max_ts = b.stateDateTime WHERE b.status in "
                     "(")
            for state_id in state_ids[:-1]:
                query += f'{state_id}, '
            query += f'{state_ids[-1]})'

            try:
                cursor.execute(query)
                for row in cursor:
                    process_id_ls.append(row.get('processID'))
            except Exception as e:
                _log.error("Error occurred in get_qaqc_data_in_base")
                _log.error(e)
        return process_id_ls

    # ToDo: rework for new tables
    # HERE
    def get_qaqc_status_history(self, process_id):
        status_history = []
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            query = ("SELECT * FROM qaqcState "
                     "WHERE processID = {pid} "
                     "ORDER by stateDateTime desc"
                     .format(pid=process_id))
            try:
                cursor.execute(query)
                for row in cursor:
                    status_history.append((row.get('stateID'),
                                           row.get('status'),
                                           row.get('action'),
                                           row.get('stateDateTime')))
            except Exception as e:
                _log.error("Error occurred in get_qaqc_data_in_base")
                _log.error(e)
        return status_history

    # Used in TranslateEarlyBase -- probably obsolete
    def get_site_status_BASE(self):
        site_list = []
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            query = ("SELECT * FROM AMFSiteStatusDisplay "
                     "WHERE curateDataSource = 'BASE'")
            try:
                cursor.execute(query)
                for row in cursor:
                    site_list.append(row.get('SITE_ID'))
            except Exception as e:
                _log.error("Error occurred in get_site_status_BASE")
                _log.error(e)
        return site_list

    def get_var_info_map(self, var_info_date):
        var_info_map = {}
        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            query = ("SELECT * FROM TowerVarDisplay_{d} "
                     "WHERE AMF_VAR_BASE_V1_VAR is not NULL"
                     .format(d=var_info_date))
            try:
                cursor.execute(query)
                for row in cursor:
                    site_id = row.get('SITE_ID')
                    if site_id not in var_info_map.keys():
                        var_info_map[site_id] = {}
                    v1_var = row.get('AMF_VAR_BASE_V1_VAR')
                    var_info_map[site_id][v1_var] = row.get('TOWER_VAR')
            except Exception as e:
                _log.error("Error occurred in get_var_info_map")
                _log.error(e)
        return var_info_map

    def get_timeout_issues(self, jira_project: str, issue_status_id: str,
                           reminder_field_values: tuple, change_field: str,
                           change_new_string: str, days_passed: int,
                           has_labels=None):
        """
        Get JIRA issues based on filters for timeout reminders.
        :param jira_project: str, JIRA project
        :param issue_status_id: str, issue status id
        :param reminder_field_values: tuple, reminder schdedule values
        :param change_field: str, issue field in which to detect a change
        :param change_new_string: str, value of the change_field to look for
        :param days_passed: str, the number of days passed to filter issues
        :param has_labels: tuple, labels to filter on
        :return: list of dict objects for each of the issues that meet the
                 criteria
        """

        query_args = dict(
            jira_project=jira_project,
            issue_status_id=issue_status_id,
            reminder_sched_values=reminder_field_values,
            change_field=change_field,
            change_new_string=change_new_string,
            format_qaqc_issue_name=JIRANames.format_QAQC_issue_name,
            reminder_sched_field_id=(
                JIRANames.reminder_schedule.replace('customfield_', '')),
            upload_token_field_id=(
                JIRANames.upload_token.replace('customfield_', '')),
            process_id_field_id=(
                JIRANames.process_ids.replace('customfield_', '')),
            cutoff_date=(
                dt.datetime.today() - dt.timedelta(days=days_passed)
                ).strftime("%Y-%m-%d"))

        query = (
            'SELECT z.issuenum, cfv2.STRINGVALUE AS uploadToken, '
            'cfv3.TEXTVALUE AS processID, y.last_change_date '
            'FROM (SELECT x.ID, MAX(x.status_change_date) AS last_change_date '
            'FROM (SELECT ji.ID, cg.CREATED AS status_change_date '
            'FROM [jiraissue] AS ji '
            'INNER JOIN [customfieldvalue] AS cfv ON ji.ID = cfv.ISSUE '
            'FULL OUTER JOIN [changegroup] AS cg ON ji.ID = cg.issueid '
            'INNER JOIN [changeitem] AS stat ON cg.ID = stat.groupid '
            'FULL OUTER JOIN [label] AS lbl ON ji.ID = lbl.ISSUE '
            'WHERE ji.issuestatus = %(issue_status_id)s '
            'AND ji.issuetype = (SELECT [ID] FROM [issuetype] '
            'WHERE pname = %(format_qaqc_issue_name)s) '
            'AND ji.PROJECT = (SELECT [ID] FROM [project] '
            'WHERE pkey = %(jira_project)s) '
            'AND cfv.CUSTOMFIELD = %(reminder_sched_field_id)s '
            'AND cfv.STRINGVALUE IN %(reminder_sched_values)s '
            'AND stat.FIELD = %(change_field)s '
            'AND CAST(stat.NEWSTRING AS nvarchar) = %(change_new_string)s '
            'AND cg.CREATED >= %(cutoff_date)s ')

        if has_labels:
            query += ('AND EXISTS ('
                      'SELECT lbl.LABEL WHERE lbl.LABEL IN %(has_labels)s) ')
            query_args['has_labels'] = has_labels

        query += (
            ') x '
            'GROUP BY x.ID) y '
            'INNER JOIN [jiraissue] AS z ON y.ID = z.ID '
            'INNER JOIN [customfieldvalue] AS cfv2 ON y.ID = cfv2.ISSUE '
            'INNER JOIN [customfieldvalue] AS cfv3 ON y.ID = cfv3.ISSUE '
            'WHERE cfv2.CUSTOMFIELD = %(upload_token_field_id)s '
            'AND cfv3.CUSTOMFIELD = %(process_id_field_id)s')

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query, query_args)
                return cursor.fetchall()

            except Exception as e:
                _log.error('Error occurred in get_timeout_issues')
                _log.error(e)

    def get_issue_labels(self, issue_num_list: list) -> dict:
        """
        return the JIRA issue labels for the specified issue numbers
        :param issue_num_list: list, issue numbers
        :return: dict, key = issue number, values = list of labels
        """
        label_lookup = {}
        query_args = {'issue_numbers': tuple(issue_num_list)}
        query = ('SELECT y.issuenum, x.label '
                 'FROM [label] x '
                 'LEFT OUTER JOIN [jiraissue] y ON y.ID = x.ISSUE '
                 'WHERE y.issuenum IN %(issue_numbers)s')

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    issue_num = str(r.get('issuenum'))
                    label_lookup.setdefault(
                        issue_num, []).append(r.get('label'))
                return label_lookup

            except Exception as e:
                _log.error('Error occurred in get_issue_labels')
                _log.error(e)

    # ToDo: update soon
    def get_process_code_version(self, process_ids: tuple):
        """
        Get process code version for specified process_ids from
        BASE processing log
        :param process_ids: tuple, process_ids
        :return: list of dicts with processID and codeVersion keys
        """
        query_params = {'process_ids': process_ids}
        query = ('SELECT processID, codeVersion '
                 'FROM qaqcProcessingLog '
                 'WHERE processID IN %(process_ids)s')

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query, query_params)
                return cursor.fetchall()

            except Exception as e:
                _log.error('Error occurred in get_process_code_version')
                _log.error(e)

    # ToDo: update eventually
    def get_fp_in_uploads(self, date_created_after: str) -> dict:
        """
        Get the uploaded files after specified date
        :param date_created_after: str, date after which to include uploads
        :return: dict, key = upload_id (UploadID),
                       value = dict with upload details
        """
        fp_in_uploads = {}

        query = ('SELECT * FROM FluxDataUploadLogAuto '
                 'WHERE uploadDate > %(query_date)s')
        query_args = dict(query_date=date_created_after)

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    upload_info = fp_in_uploads.setdefault(
                        r.get('UpdateID'), {})
                    upload_info['data_file'] = r.get('dataFile')
                    upload_info['site_id'] = r.get('SITE_ID')
                    upload_info['uploader_id'] = r.get('userID')
                    upload_info['uploader_email'] = r.get('email')
                    upload_info['uploader_name'] = r.get('name')
                    upload_info['upload_date'] = r.get('uploadDate')
            except Exception as e:
                _log.error('Error occurred in get_fp_in_uploads.')
                _log.error(e)

        return fp_in_uploads

    def get_process_ids_from_jira_format_issues(self, date_created_after: str,
                                                jira_project: str) -> list:
        """
        Get a list of process ids from JIRA format issues processed since
            specified date
        :param date_created_after: str, the date after which to include issues
        :param jira_project: str, the JIRA project name
        :return: list of process ids from the JIRA format issues
        """
        process_ids = []
        query = ('SELECT x.issuenum, x.reporter, x.CREATED, x.UPDATED, '
                 'cfv1.STRINGVALUE as site_id, '
                 'cfv2.TEXTVALUE as process_ids, '
                 'cfv3.STRINGVALUE as upload_token '
                 'FROM jiraissue x '
                 'INNER JOIN customfieldvalue cfv1 '
                 'ON x.ID = cfv1.ISSUE '
                 'INNER JOIN customfieldvalue cfv2 '
                 'ON x.ID = cfv2.ISSUE '
                 'INNER JOIN customfieldvalue cfv3 '
                 'ON x.ID = cfv3.ISSUE '
                 'WHERE x.PROJECT = '
                 '(SELECT ID FROM project '
                 'WHERE pkey = %(jira_project)s) '
                 'AND x.issuetype = ('
                 'SELECT ID FROM issuetype '
                 'WHERE pname = %(format_qaqc_issue_name)s)  '
                 'AND x.CREATED > %(query_date)s '
                 'AND cfv1.CUSTOMFIELD = %(site_id_field_id)s '
                 'AND cfv2.CUSTOMFIELD = %(process_ids_field_id)s '
                 'AND cfv3.CUSTOMFIELD = %(upload_token_field_id)s')

        query_args = dict(
            jira_project=jira_project, query_date=date_created_after,
            format_qaqc_issue_name=JIRANames.format_QAQC_issue_name,
            site_id_field_id=JIRANames.site_id.split('_')[-1],
            process_ids_field_id=JIRANames.process_ids.split('_')[-1],
            upload_token_field_id=JIRANames.upload_token.split('_')[-1])

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)
            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    process_ids_str = r.get('process_ids')
                    process_ids.extend(process_ids_str.split(' '))
            except Exception as e:
                _log.error('Error occurred in '
                           'get_process_ids_from_jira_format_issues')
                _log.error(e)

        return process_ids

    # ToDo update eventually
    def get_format_qaqc_process_attempts(self, date_created_after: str,
                                         process_types: tuple = ('File',),
                                         site_ids: tuple = ()) -> dict:
        """
        Get Format QAQC process attempts
        :param date_created_after: str, date after which to include issues
        :param process_types: tuple, the process types: default File = Format
        :param site_ids: tuple, site_IDs if fitlering by site_ids
        :return: dict, key = process_id, value = dict of process details
        """
        process_info_store = {}
        query = ('SELECT * FROM qaqcProcessingLog '
                 'WHERE processDate > %(query_date)s '
                 'AND processType in %(process_type)s ')
        query_args = dict(query_date=date_created_after,
                          process_types=process_types)
        if site_ids:
            query = (f'{query}'
                     'AND SITE_ID in %(site_ids)s')
            query_args['site_ids'] = site_ids

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)

            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    process_info = process_info_store.setdefault(
                        r.get('processID'), {})
                    process_info['process_type'] = r.get('processType')
                    process_info['site_id'] = r.get('SITE_ID')
                    process_info['upload_id'] = r.get('updateID')
                    process_info['process_datetime'] = r.get('processDatetime')
                    process_info['prior_process_id'] = r.get('priorProcessID')
                    process_info['zip_process_id'] = r.get('zipProcessID')
                    process_info['retry_count'] = r.get('retryCount')
                    process_info['file_timestamp_start'] = r.get('startTime')
                    process_info['file_timestamp_end'] = r.get('endTime')

            except Exception as e:
                _log.error('Error occurred in '
                           'get_format_qaqc_process_attempts')
                _log.error(e)

        return process_info_store

    # ToDo: update eventually
    def get_upload_file_info_for_site(self, site_id: str) -> list:
        """
        Get upload file info for the specified site for processing runs
        This query joins the QAQC processing log with the upload file log
        :param site_id: str
        :return: list of dict objects in descending order
        """
        uploaded_files = []
        query = SQL(
            'SELECT x.processID, x.updateID, x.SITE_ID, '
            'x.startTime, x.endTime, '
            'y.startTime AS autocorrected_start_time, '
            'y.endTime AS autocorrected_end_time, '
            'z.uploadToken, z.uploadDate, z.dataFile, z.userID, z.name '
            'FROM qaqcProcessingLog x '
            'LEFT OUTER JOIN qaqcProcessingLog y '
            'ON x.processID = y.priorProcessID '
            'LEFT OUTER JOIN FluxDataUploadLogAuto z '
            'ON x.updateID = z.updateID '
            'WHERE x.processType = %(process_type)s '
            'AND x.site_id = %(site_id)s '
            'AND x.priorProcessID IS NULL '
            'AND NOT ((x.processID > 14865 AND x.processID < 26642) '
            'AND x.site_id IN %(exclude_sites)s) '
            'ORDER BY x.processID DESC ')

        query_args = dict(site_id=site_id, process_type='File',
                          exclude_sites=('US-Pnp', 'US-Men'))

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)

            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    temp_dict = dict()
                    temp_dict['process_id'] = r.get('processID')
                    temp_dict['upload_id'] = r.get('uploadID')
                    temp_dict['site_id'] = r.get('SITE_ID')
                    temp_dict['start_time'] = r.get('start_time')
                    temp_dict['end_time'] = r.get('end_time')
                    temp_dict['autocorrected_start_time'] = r.get(
                        'autocorrected_start_time')
                    temp_dict['autocorrected_end_time'] = r.get(
                        'autocorrected_end_time')
                    temp_dict['upload_token'] = r.get('uploadToken')
                    temp_dict['upload_timestamp'] = r.get('uploadDate')
                    temp_dict['data_filename'] = r.get('dataFile')
                    temp_dict['uploader_id'] = r.get('userID')
                    temp_dict['uploader_name'] = r.get('name')
                    uploaded_files.append(temp_dict)

            except Exception as e:
                _log.error('Error occurred in get_uploaded_file_info_for_site')
                _log.error(e)

        return uploaded_files

    def get_format_issues(
            self, jira_project: str, site_id: Union[None, str] = None,
            issue_key_list: Union[None, list] = None,
            with_details: bool = False) -> dict:
        """

        :param jira_project:
        :param site_id:
        :param issue_key_list:
        :param with_details:
        :return:
        """
        jira_names = JIRANames()
        format_issues = {}
        if not site_id or issue_key_list:
            _log.error('Site ID or a list of issue keys must be specified.')
            return format_issues

        # NOTE: fields may be modified in next PR depending
        #    on resolve_issues method
        # ToDo: consider changing all to inner joins
        query_pieces = [(
            'SELECT x.issuenum, x.reporter, x.CREATED as created, '
            'a.STRINGVALUE as site_id, '
            'b.TEXTVALUE as process_ids, c.STRINGVALUE as upload_token, '
            'd.TEXTVALUE as file_date_range, e.TEXTVALUE as upload_comment '
            'FROM jiraissue x '
            'INNER JOIN customfieldvalue a ON x.ID = a.ISSUE '
            'INNER JOIN customfieldvalue b ON x.ID = b.ISSUE '
            'INNER JOIN customfieldvalue c ON x.ID = c.ISSUE '
            'INNER JOIN customfieldvalue d ON x.ID = d.ISSUE '
            'INNER JOIN customfieldvalue e ON x.ID = e.ISSUE '
            'WHERE x.PROJECT = ('
            'SELECT [ID] FROM [project] '
            'WHERE pkey = %(jira_project)s ) '
            'AND x.issuetype = ('
            'SELECT ID FROM issuetype '
            'WHERE pname = %(format_qaqc_issue_name)s)  '
            'AND a.CUSTOMFIELD = %(site_id_field)s '
            'AND b.CUSTOMFIELD = %(process_ids_field)s '
            'AND c.CUSTOMFIELD = %(upload_token_field)s '
            'AND d.CUSTOMFIELD = %(file_date_range_field)s '
            'AND e.CUSTOMFIELD = %(upload_comment_field)s ')]

        query_args = dict(
            jira_project=jira_project,
            format_qaqc_issue_name=JIRANames.format_QAQC_issue_name,
            site_id_field=jira_names.strip_customfield('site_id'),
            process_ids_field=jira_names.strip_customfield('process_ids'),
            upload_token_field=jira_names.strip_customfield('upload_token'),
            file_date_range_field=jira_names.strip_customfield(
                'start_end_dates'),
            upload_comment_field=jira_names.strip_customfield(
                'upload_comment'))

        if site_id:
            query_pieces.append('AND a.STRINGVALUE = %(site_id)s ')
            query_args['site_id'] = site_id

        if issue_key_list:
            query_pieces.append('AND x.issuenum in %(issue_key_list)s ')
            query_args['issue_key_list'] = issue_key_list

        query_pieces.append('ORDER BY x.CREATED DESC')

        query = ''.join(query_pieces)

        with pymssql.connect(
                server=self.__hostname,
                user=self.__user,
                password=self.__password,
                database=self.__db_name) as conn, \
                conn.cursor(as_dict=True) as cursor:
            conn.autocommit(True)

            try:
                cursor.execute(query, query_args)
                for r in cursor:
                    key = r.get('issuenum')
                    issue_info = format_issues.setdefault(key, {})
                    issue_info['upload_token'] = r.get('upload_token')

                    if with_details:
                        # ToDo: exact fields to be determined in next PR
                        issue_info['created_date'] = r.get('created')
                        issue_info['reporter'] = r.get('reporter')
                        issue_info['site_id'] = r.get('site_id')
                        process_ids = r.get('process_ids')
                        issue_info['process_ids'] = process_ids.split(' ')
                        issue_info['file_date_range'] = r.get(
                            'file_date_range')
                        issue_info['upload_comment'] = r.get('upload_comment')

            except Exception as e:
                _log.error('Error occurred in get_format_issues')
                _log.error(e)

        return format_issues
