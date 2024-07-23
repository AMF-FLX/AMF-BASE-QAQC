#!/usr/bin/env python

import json
import urllib.request
from configparser import ConfigParser
from http import HTTPStatus
from logger import Logger
from urllib.request import HTTPError

_log = Logger().getLogger(__name__)


class FPVariables():
    def __init__(self):
        self.fp_vars = {}

        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            cfg_section = 'WEBSERVICES'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.fp_vars_ws = config.get(cfg_section, 'fp_vars')
                self._load_fp_vars_dict()
            else:
                self.fp_vars_ws = None
                _log.error('Cannot find web service from config.')

    def _load_fp_vars_dict(self):
        _log.info('Loading variable dictionary.')

        # Until VarUtils is redesigned, WSUtil can not be used here
        status = None
        fail_reason = 'unknown'
        try:
            with urllib.request.urlopen(self.fp_vars_ws) as c:
                status = c.code
                content = c.read().decode('utf-8')
        except HTTPError as e:
            status = e.code
            fail_reason = e.reason
        finally:
            if status != HTTPStatus.OK:
                status_msg = (
                    f'{self.fp_vars_ws} returned status code: '
                    f'{status}\n Failure reason: {fail_reason}')
                _log.fatal(status_msg)
                raise Exception(status_msg)

        data = json.loads(content)
        for var in data:
            self.fp_vars[var.get('Name')] = var.get('Units')
        return self.fp_vars

    def get_fp_vars_dict(self):
        return self.fp_vars
