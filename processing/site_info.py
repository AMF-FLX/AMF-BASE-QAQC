#!/usr/bin/env python

import json
from configparser import ConfigParser
from logger import Logger
from utils import WSUtil

_log = Logger().getLogger(__name__)


class SiteInfo:
    def __init__(self, site_id):
        self.site_info = {}
        self.ws_util = WSUtil(_log)

        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            cfg_section = 'WEBSERVICES'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.site_info_ws = config.get(cfg_section, 'site_info')
                self._load_site_dict(site_id)
            else:
                self.site_info_ws = None
                _log.error('Cannot find web service from config.')

    def _load_site_dict(self, site_id):
        url = self.site_info_ws.format(site_id=site_id)
        content = self.ws_util.get_content(url)

        resp = json.loads(content)
        grp_header = list(resp.get('HEADER', {'': {}}).values())[0]
        self.site_info['SITE_ID'] = grp_header.get('SITE_ID', None)
        grp_location = list(resp.get('LOCATION', {'': {}}).values())[0]
        self.site_info['LOCATION_LAT'] = grp_location.get('LOCATION_LAT', None)
        self.site_info['LOCATION_LONG'] = grp_location.get(
            'LOCATION_LONG', None)
        self.site_info['LOCATION_ELEV'] = grp_location.get(
            'LOCATION_ELEV', None)
        grp_igbp = list(resp.get('IGBP', {'': {}}).values())[0]
        self.site_info['IGBP'] = grp_igbp.get('IGBP', None)
        grp_utc_offset = list(resp.get('UTC_OFFSET', {'': {}}).values())[0]
        self.site_info['UTC_OFFSET'] = grp_utc_offset.get('UTC_OFFSET', None)
        return self.site_info

    def get_site_dict(self):
        return self.site_info
