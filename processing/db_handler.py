import datetime as dt
import psycopg2

from jira_names import JIRANames

from configparser import ConfigParser
from io import StringIO
from logger import Logger
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor
from psycopg2.sql import Composed, SQL
from typing import NamedTuple, Union

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

    def get_input_files(self, process_id):
        input_files = set()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = SQL('SELECT format_qaqc_id '
                        'FROM qaqc.aggregate_processing_log '
                        'WHERE data_qaqc_id = %s')
            cursor.execute(query, (process_id,))
            for row in cursor:
                pid = row.get('format_qaqc_id')
                input_files.add(pid)
        return input_files

    def define_base_candidates_query(
            self, pre_query, post_query, state_ids):
        full_query_components = [pre_query]
        query_criteria = SQL('scv.shortname = %s ')
        for idx, state_id in enumerate(state_ids):
            if idx > 0:
                full_query_components.append(SQL('OR '))
            full_query_components.append(query_criteria)
        full_query_components.append(post_query)
        return Composed(full_query_components)

    # ToDo: update for publish
    def get_base_candidates(self, state_ids):
        preBASE_files = {}

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            pre_query = SQL(
                'SELECT o.aggregate_file_path, s.process_id, '
                'p.publishing_code_version '
                'FROM qaqc.processing_log l '
                'INNER JOIN ('
                'SELECT s.process_id, s.state_id '
                'FROM qaqc.state_log s '
                'INNER JOIN ('
                'SELECT process_id, MAX(log_timestamp) AS '
                'latest_state_timestamp '
                'FROM qaqc.state_log '
                'GROUP BY process_id) latest_state '
                'ON latest_state.latest_state_timestamp = s.log_timestamp '
                'AND s.process_id = latest_state.process_id '
                'INNER JOIN qaqc.state_cv_type_auto scv '
                'ON scv.type_id = s.state_id '
                'WHERE ')
            post_query = SQL(
                ') s ON s.process_id = l.log_id '
                'INNER JOIN qaqc.process_summarized_output o '
                'ON o.process_id = l.log_id '
                'LEFT JOIN qaqc.publishing_log p '
                'ON p.process_id = l.log_id ')
            query = self.define_base_candidates_query(
                pre_query=pre_query,
                post_query=post_query,
                state_ids=state_ids)
            try:
                cursor.execute(query, state_ids)
                for row in cursor:
                    candidate_filepath = row.get('aggregate_file_path')
                    code_version = row.get('publishing_code_version')
                    process_id = row.get('process_id')
                    # Remap paths if codeVersion is prior to version 1.1.0
                    if code_version < '1.1.0':
                        path = Path(candidate_filepath)
                        filename = path.name
                        parent_path_parts = path.parent.parts
                        # strip top two level directories
                        # as well as immediate parent directory
                        # and rebuild immediate parent path
                        candidate_filepath = str(
                            Path(parent_path_parts[0],
                                 *parent_path_parts[3:-1],
                                 *('outputs', 'qaqc_combined'),
                                 filename))
                    preBASE_files[candidate_filepath] = process_id

            except Exception as e:
                _log.error("Error occurred in get_base_candidates")
                _log.error(e)
                return "ERROR OCCURRED"
        return preBASE_files

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

    def get_data_upload_with_uuid(self, conn, uuid):
        query = SQL('SELECT u.log_id, u.site_id, '
                    'u.data_file, u.upload_token, '
                    'u.upload_comment, u.upload_type_id '
                    'FROM input_interface.data_upload_log u '
                    'LEFT JOIN '
                    'input_interface.data_upload_file_xfer_log x '
                    'ON u.log_id = x.upload_log_id '
                    'LEFT JOIN '
                    'input_interface.data_upload_type t '
                    'ON u.upload_type_id = t.type_id '
                    'WHERE '
                    't.description IN (\'Half hourly data\', '
                    '\'Half-hourly gap-filled data\') '
                    'AND x.xfer_end_log_timestamp IS NOT NULL '
                    'AND u.upload_token = %(uuid)s')
        params = {'uuid': uuid}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            new_data_upload = cursor.fetchall()
        return new_data_upload

    def get_new_data_upload(self,
                            conn,
                            qaqc_processor_source,
                            is_qaqc_processor,
                            uuid=None):
        query_base = SQL('SELECT u.log_id, u.site_id, '
                         'u.data_file, u.upload_token, '
                         'u.upload_comment, u.upload_type_id '
                         'FROM input_interface.data_upload_log u '
                         'LEFT JOIN qaqc.processing_log p '
                         'ON u.log_id = p.upload_id '
                         'LEFT JOIN '
                         'input_interface.data_upload_file_xfer_log x '
                         'ON u.log_id = x.upload_log_id '
                         'LEFT JOIN input_interface.data_source_type s '
                         'ON u.upload_source_id = s.source_id '
                         'LEFT JOIN '
                         'input_interface.data_upload_type t '
                         'ON u.upload_type_id = t.type_id '
                         'WHERE p.log_id IS NULL '
                         'AND t.description IN (\'Half hourly data\', '
                         '\'Half-hourly gap-filled data\') '
                         'AND x.xfer_end_log_timestamp IS NOT NULL ')
        composed_query_list = [query_base]
        if is_qaqc_processor:
            composed_query_list.append(
                SQL('AND s.source = %(qaqc_processor_source)s'))
        else:
            composed_query_list.append(
                SQL('AND s.source <> %(qaqc_processor_source)s'))
        params = {'qaqc_processor_source': qaqc_processor_source}
        if uuid:
            composed_query_list.append(
                SQL('AND u.upload_token = %(uuid)s'))
            params['uuid'] = uuid
        query = Composed(composed_query_list)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            new_data_upload = cursor.fetchall()
        return new_data_upload

    def get_latest_run_with_uuid(self, conn, uuid):
        query = SQL('SELECT u.log_id, latest_upload.process_timestamp '
                    'FROM input_interface.data_upload_log u '
                    'INNER JOIN (SELECT upload_id, '
                    'max(process_timestamp) AS process_timestamp '
                    'FROM qaqc.processing_log p '
                    'GROUP BY upload_id) latest_upload '
                    'ON u.log_id = latest_upload.upload_id '
                    'WHERE u.upload_token = %(uuid)s')
        params = {'uuid': uuid}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            run_data = cursor.fetchone()
        return run_data

    def get_incomplete_user_data_upload(self,
                                        conn,
                                        qaqc_processor_source,
                                        lookback_h):
        # case:
        # user file is the file that is uploaded by users
        # user file has entry in upload_log and processing_log
        # but not in summarized_output_log
        # solution:
        # return this data_upload
        query = SQL('SELECT u.log_id, u.site_id, '
                    'u.data_file, u.upload_token, '
                    'u.upload_comment, u.upload_type_id '
                    'FROM input_interface.data_upload_log u '
                    'LEFT JOIN '
                    '(SELECT * FROM ('
                    'SELECT process_timestamp, upload_id, log_id, '
                    'ROW_NUMBER() OVER (PARTITION BY upload_id '
                    'ORDER BY process_timestamp DESC) '
                    'AS row_num FROM qaqc.processing_log) ps '
                    'WHERE row_num = 1) p '
                    'ON u.log_id = p.upload_id '
                    'LEFT JOIN qaqc.process_summarized_output o '
                    'ON p.log_id = o.process_id '
                    'LEFT JOIN '
                    'input_interface.data_upload_file_xfer_log x '
                    'ON u.log_id = x.upload_log_id '
                    'LEFT JOIN input_interface.data_source_type s '
                    'ON u.upload_source_id = s.source_id '
                    'LEFT JOIN '
                    'input_interface.data_upload_type t '
                    'ON u.upload_type_id = t.type_id '
                    'WHERE p.log_id IS NOT NULL '
                    'AND o.output_id IS NULL '
                    'AND t.description IN (\'Half hourly data\', '
                    '\'Half-hourly gap-filled data\') '
                    'AND x.xfer_end_log_timestamp IS NOT NULL '
                    'AND s.source <> %(qaqc_processor_source)s '
                    'AND log_timestamp >= CURRENT_TIMESTAMP '
                    '- INTERVAL \'%(lookback_h)s hours\'')
        params = {'qaqc_processor_source': qaqc_processor_source,
                  'lookback_h': lookback_h}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data_upload = cursor.fetchall()
        return data_upload

    def get_incomplete_system_data_upload(self,
                                          conn,
                                          qaqc_processor_source,
                                          lookback_h):
        # case:
        # system file is the file that is uploaded by QAQCProcessor
        # system file has entry in upload_log
        # system file can be in/not in processing_log
        # but not in summarized_output_log
        # solution:
        # traceback and return user file of this data_upload
        query = SQL('SELECT u.log_id, u.site_id, '
                    'u.data_file, u.upload_token, '
                    'u.upload_comment, u.upload_type_id, '
                    'u.log_timestamp '
                    'FROM input_interface.data_upload_log u '
                    'LEFT JOIN qaqc.processing_log p '
                    'ON u.log_id = p.upload_id '
                    'LEFT JOIN qaqc.process_summarized_output o '
                    'ON p.log_id = o.process_id '
                    'LEFT JOIN input_interface.data_source_type s '
                    'ON u.upload_source_id = s.source_id '
                    'LEFT JOIN '
                    'input_interface.data_upload_type t '
                    'ON u.upload_type_id = t.type_id '
                    'WHERE o.output_id IS NULL '
                    'AND t.description IN (\'Half hourly data\', '
                    '\'Half-hourly gap-filled data\') '
                    'AND s.source = %(qaqc_processor_source)s '
                    'AND log_timestamp >= CURRENT_TIMESTAMP '
                    '- INTERVAL \'%(lookback_h)s hours\'')
        params = {'qaqc_processor_source': qaqc_processor_source,
                  'lookback_h': lookback_h}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data_upload = cursor.fetchall()
        return data_upload

    def trace_user_data_upload(self, conn, process_id):
        # case:
        # system file is not finished
        # trace up 1 level given process_id
        query = SQL('SELECT u.log_id, u.site_id, '
                    'u.data_file, u.upload_token, '
                    'u.upload_comment, u.upload_type_id, '
                    'p.prior_process_id, p.zip_process_id '
                    'FROM input_interface.data_upload_log u '
                    'LEFT JOIN qaqc.processing_log p '
                    'ON u.log_id = p.upload_id '
                    'WHERE p.log_id = %(process_id)s')
        params = {'process_id': process_id}
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data_upload = cursor.fetchone()
        return data_upload

    def check_status_of_process_id(self, conn, process_id):
        is_success = False
        query = SQL('SELECT report AS count_log_id '
                    'FROM qaqc.process_summarized_output p '
                    'WHERE process_id = %(process_id)s')
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {'process_id': process_id})
            report = cursor.fetchone()
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
        self.conn = self.init_db_conn(db_config)

        cv_lookup = {}

        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            for r in cursor:
                cv_name = r.get(name_field)
                cv_id = r.get(id_field)
                cv_lookup.update({cv_name: cv_id})

        return cv_lookup

    def get_qaqc_state_types(self) -> dict:
        query = SQL('SELECT * from qaqc.state_cv_type_auto;')
        return self._get_type_cv(query, 'shortname')

    def get_incomplete_phase3_process_ids(self, conn, state_ids):
        process_ids = []
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = SQL(
                'SELECT b.process_id '
                'FROM (SELECT process_id, MAX(log_timestamp) as max_ts '
                'FROM qaqc.state_log GROUP by process_id) as d '
                'INNER JOIN qaqc.state_log as b '
                'ON d.process_id = b.process_id '
                'AND d.max_ts = b.log_timestamp WHERE b.state_id in '
                '%(state_ids)s')

            try:
                cursor.execute(query, {'state_ids': state_ids})
                for row in cursor:
                    process_ids.append(row.get('process_id'))
            except Exception as e:
                _log.error('Error occurred in '
                           'get_incomplete_phase3_process_ids')
                _log.error(e)
        return process_ids

    def get_qaqc_state_history(self, conn, process_id):
        status_history = []
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = SQL('SELECT * FROM qaqc.state_log '
                        'WHERE process_id = %(pid)s '
                        'ORDER by log_timestamp desc')
            try:
                cursor.execute(query, {'pid': process_id})
                for row in cursor:
                    status_history.append((row.get('log_id'),
                                           row.get('state_id'),
                                           row.get('log_timestamp')))
            except Exception as e:
                _log.error('Error occurred in get_qaqc_data_state_history')
                _log.error(e)
        return status_history
