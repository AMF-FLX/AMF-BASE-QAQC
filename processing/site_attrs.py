#!/usr/bin/env python

import json
from configparser import ConfigParser
from logger import Logger
from urllib.request import urlopen
from urllib.error import HTTPError
from future.standard_library import install_aliases
install_aliases()

_log = Logger().getLogger(__name__)


class SiteAttributes():
    def __init__(self):
        self.sites = {}

        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            cfg_section = 'WEBSERVICES'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.site_attrs_ws = config.get(cfg_section, 'site_attrs')
                self._load_site_dict()
            else:
                self.site_attrs_ws = None
                _log.error('Cannot find web service from config.')

    def _load_site_dict(self):
        _log.info('Loading site dictionary.')
        try:
            resp = urlopen(self.site_attrs_ws)
        except HTTPError as e:
            raise Exception('{ws} returned status code {s}\n{r}'.format(
                ws=self.site_attrs_ws, s=e.code, r=e.read().decode('utf-8')))
        resp = json.loads(resp.read().decode('utf-8'))
        for var in resp:
            self.sites[var.get('SITE_ID')] = var.get('SITE_NAME')
        return self.sites

    def get_site_dict(self):
        return self.sites
