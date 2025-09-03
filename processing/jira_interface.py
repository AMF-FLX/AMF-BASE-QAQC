#!/usr/bin/env python

from configparser import ConfigParser
from future.standard_library import install_aliases
from http import HTTPStatus
from jira_names import JIRANames
from logger import Logger
from report_status import ReportStatus
from typing import Union, Optional
from urllib.error import HTTPError

import json
import site_attrs
import urllib.request
install_aliases()

__author__ = 'Norm Beekwilder, Danielle Christianson, Fianna O\'Brien'
__email__ = 'norm.beekwilder@gmail.com, dschristianson@lbl.gov, ' \
            'flobrien@lbl.gov'
_log = Logger().getLogger(__name__)


class JIRAInterface:
    def __init__(self, configure_organizations: bool = True):
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            cfg_section = 'JIRA'
            config.read_file(cfg)
            if config.has_section(cfg_section):
                self.jira_host = config.get(cfg_section, 'jira_host')
                self.jira_base_path = config.get(cfg_section, 'jira_base_path')
                self.jira_project = config.get(cfg_section, 'project')
                self.sd_id = config.get(cfg_section, 'sd_id')
                self.data_issue_query = config.get(
                    cfg_section, 'data_issue_query')
                self.format_issue_query = config.get(
                    cfg_section, 'format_issue_query')
                # create user_token with
                #    base64.standard_b64encode('<user>:<pass>'.encode('utf-8'))
                self.user_token = config.get(cfg_section, 'user_token')
            else:
                self.jira_host = None
                self.jira_base_path = None
                self.jira_project = None
                self.user_token = None
            is_test = config.get('VERSION', 'test')
            cfg_section = 'TEST_INFO'
            if is_test and config.has_section(cfg_section):
                self.test_site = config.get(cfg_section, 'test_site')
            else:
                self.test_site = 'test-Site'
            self.jira_ws_base = f'{self.jira_host}{self.jira_base_path}'
            if configure_organizations:
                self.org_dict = self.get_organizations()

    def _get_default_auth(self):
        return f'Basic {self.user_token}'

    def _get_default_http_error_msg_code(self, ws, error):
        err_code = error.code
        err_msg = error.read().decode('utf-8')
        return f'{ws} returned status code {err_code}\n{err_msg}', err_code

    def _get_default_http_exception(self, ws, error):
        exception_msg, _ = self._get_default_http_error_msg_code(ws, error)
        return Exception(exception_msg)

    def create_format_issue(self, site_id, process_id, start_end_times,
                            upload_token, uploader, summary, description,
                            upload_comment, reminder_schedule_id=None):
        if site_id not in self.org_dict:
            self.ensure_org_exists(site_id)
        # there doesn't seem to be a way to set the request type
        #     from this api. workaround with automation
        msg = {'fields': {'project': {'key': self.jira_project},
                          'summary': summary,
                          'description': description,
                          'issuetype': {'name':
                                        JIRANames.format_QAQC_issue_name},
                          'reporter': {'name': uploader},
                          JIRANames.site_id: site_id,
                          JIRANames.process_ids: process_id,
                          JIRANames.start_end_dates: start_end_times,
                          JIRANames.upload_token: upload_token,
                          JIRANames.upload_comment: upload_comment,
                          JIRANames.organizations:
                              [int(self.org_dict[site_id])]}}
        if reminder_schedule_id:
            msg['fields'][JIRANames.reminder_schedule] = {
                'id': reminder_schedule_id}
        return self._create_issue(msg)

    def create_data_issue(self, site_id, process_id, time_resolution,
                          contact, summary, description,
                          participants, ftp_link, report_link):
        if site_id != self.test_site:
            self.update_org_members(site_id)
        msg = {'fields': {'project': {'key': self.jira_project},
                          'summary': summary,
                          'description': description,
                          'issuetype': {
                              'name': JIRANames.data_QAQC_issue_name},
                          'reporter': {'name': contact},
                          JIRANames.site_id: site_id,
                          JIRANames.process_ids: process_id,
                          JIRANames.organizations:
                              [int(self.org_dict[site_id])],
                          JIRANames.report_link: report_link,
                          JIRANames.ftp_link: ftp_link,
                          JIRANames.sandbox: '*[DATA QA/QC]*',
                          JIRANames.time_res: {'value': time_resolution},
                          JIRANames.participants:
                              [{'name': p} for p in participants]}}
        result_msg, result = self._create_issue(msg)
        if result == HTTPStatus.CREATED:
            return result_msg
        raise Exception(result_msg)

    def create_site_issue(
            self, site_id, reporter, summary, description, participants):
        issue_name = JIRANames.site_QAQC_issue_name
        msg = {
            'fields': {
                'project': {'key': self.jira_project},
                'summary': summary,
                'description': description,
                'issuetype': {'name': issue_name},
                'reporter': {'name': reporter},
                JIRANames.site_id: site_id
            }}
        result_msg, result = self._create_issue(msg)
        if result == HTTPStatus.CREATED:
            return result_msg
        raise Exception(result_msg)

    def _create_issue(self, msg):
        url = f'{self.jira_ws_base}/issue'
        try:
            req = urllib.request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            req.add_header('Authorization', self._get_default_auth())
            response = urllib.request.urlopen(req, msg_bytes)
            issue = json.loads(response.read().decode('utf-8'))
            return issue['key'], response.getcode()
        except HTTPError as e:
            err_msg, err_code = self._get_default_http_error_msg_code(url, e)
            return err_msg, err_code

    def add_comment(self, issue_key, message, public=False):
        msg = {'body': message, 'public': public}
        url = (f'{self.jira_host}/rest/servicedeskapi/request/'
               f'{issue_key}/comment')
        response = self._basic_post_request(msg, url)
        return response.getcode()

    def _basic_get_request(self, url):
        try:
            req = urllib.request.Request(url)
            req.add_header('X-ExperimentalApi', 'opt-in')
            req.add_header('Authorization', self._get_default_auth())
            response = urllib.request.urlopen(req)
            return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            self._get_default_http_exception(ws=url, error=e)

    def _basic_post_request(self, msg, url, is_experimental=False):
        try:
            req = urllib.request.Request(url)
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            req.add_header('Authorization', self._get_default_auth())
            if is_experimental:
                req.add_header('X-ExperimentalApi', 'opt-in')
            post_request = urllib.request.urlopen(req, msg_bytes)
            if is_experimental:
                return post_request.getcode(), post_request
            return post_request
        except HTTPError as e:
            if is_experimental:
                err_msg, err_code = self._get_default_http_error_msg_code(
                    url, e)
                _log.error(err_msg)
                return err_code, err_msg
            self._get_default_http_exception(ws=url, error=e)

    def _experimental_post_request(self, msg, url):
        return self._basic_post_request(msg, url, is_experimental=True)

    def _delete_request(self, msg, url):
        try:
            req = urllib.request.Request(url, method='DELETE')
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            req.add_header('X-ExperimentalApi', 'opt-in')
            req.add_header('Authorization', self._get_default_auth())
            return urllib.request.urlopen(req, msg_bytes)
        except HTTPError as e:
            self._get_default_http_exception(ws=url, error=e)

    def _basic_put_request(self, msg, url):
        try:
            req = urllib.request.Request(url, method='PUT')
            req.add_header('Content-Type', 'application/json; charset=utf-8')
            json_data = json.dumps(msg)
            msg_bytes = json_data.encode('utf-8')  # needs to be bytes
            req.add_header('Content-Length', len(msg_bytes))
            req.add_header('Authorization', self._get_default_auth())
            return urllib.request.urlopen(req, msg_bytes)
        except HTTPError as e:
            self._get_default_http_exception(ws=url, error=e)

    def _update_issue_fields(self, issue_key, msg):
        msg = {'update': msg}
        url = f'{self.jira_ws_base}/issue/{issue_key}'
        self._basic_put_request(msg, url)

    def get_jira_issue(self, issue_key):
        return self._basic_get_request(
            f'{self.jira_ws_base}/issue/{issue_key}')

    def add_label(self, issue_key, labels):
        if not isinstance(labels, list):
            labels = [labels]
        add_labels_dict = [{'add': label} for label in labels]
        msg = {'labels': add_labels_dict}
        self._update_issue_fields(issue_key, msg)

    def remove_label(self, issue_key, labels):
        if not isinstance(labels, list):
            labels = [labels]
        remove_labels_dict = [{'remove': label} for label in labels]
        msg = {'labels': remove_labels_dict}
        self._update_issue_fields(issue_key, msg)

    def set_issue_state(self, issue_key, transition, resolution=None,
                        labels=None):
        msg = {'transition': {'id': transition}}
        if resolution is not None:
            msg['fields'] = {'resolution': {'name': resolution}}
        if labels is not None:
            if resolution is None:
                msg['fields'] = {}
            msg['fields']['labels'] = labels
        url = f'{self.jira_ws_base}/issue/{issue_key}/transitions'
        self._basic_post_request(msg, url)

    def add_related_link(self, parent_issue, child_issue,
                         link_type='Relates'):
        msg = {'type': {'name': link_type},
               'inwardIssue': {'key': parent_issue},
               'outwardIssue': {'key': child_issue}}
        url = f'{self.jira_ws_base}/issueLink'
        self._basic_post_request(msg, url)

    def get_organizations(self):
        count = 0
        org_dict = {}
        while True:
            orgs = self._get_org_block(count)
            for o in orgs['values']:
                org_dict[o['name']] = o['id']
            if orgs['isLastPage']:
                return org_dict
            count += 1

    def _get_org_block(self, block):
        s = str(block * 50)
        url = f'{self.jira_host}/rest/servicedeskapi/organization?start={s}'
        return self._basic_get_request(url)

    def get_prior_data_qaqc_key(self, site_id):
        q = self.data_issue_query.format(p=self.jira_project, s=site_id)
        url = f'{self.jira_ws_base}/search?{q}'
        issue_dict = self._basic_get_request(url)
        if len(issue_dict['issues']) == 0:
            return None
        return issue_dict['issues'][0]['key']

    def get_format_qaqc_key(self, process_id):
        q = self.format_issue_query.format(p=self.jira_project, i=process_id)
        url = f'{self.jira_ws_base}/search?{q}'
        issue_dict = self._basic_get_request(url)
        if len(issue_dict['issues']) == 0:
            return None
        return issue_dict['issues'][0]['key']

    def get_format_issues(
            self, site_id: Union[None, str] = None,
            issue_key_list: Union[None, list] = None,
            max_request_count: int = 20,
            max_results_per_try: int = 50,
            additional_fields: Optional[list] = None,
            sort_order: str = 'DESC') -> dict:
        """
        Get format issues via REST API search
        """

        format_issues = {}
        if not site_id and not issue_key_list:
            _log.error('Site ID or a list of issue keys must be specified.')
            return format_issues

        fields = [JIRANames.issue_created, JIRANames.issue_reporter,
                  JIRANames.site_id, JIRANames.process_ids,
                  JIRANames.upload_token, JIRANames.start_end_dates,
                  JIRANames.issue_status]

        if additional_fields:
            fields.extend(additional_fields)

        site_id_query = ''
        if site_id:
            site_id_query = f'AND "Site ID" ~ {site_id} '

        key_query = ''
        if issue_key_list:
            issue_key_str = "\", \"".join(issue_key_list)
            key_query = f'AND key IN (\"{issue_key_str}\") '

        jql = (f'project = {self.jira_project} AND issuetype = "Format QAQC Results" {site_id_query}{key_query}'
               f'ORDER BY created {sort_order}')

        url = f'{self.jira_ws_base}/search'
        total = 0
        request_count = 0
        max_results = max_results_per_try
        start_at = 0

        while total is not None and (total > request_count * max_results if request_count > 0 else True):
            msg = {"startAt": start_at, "maxResults": max_results, "fields": fields, "jql": jql}
            response = self._basic_post_request(msg, url)

            if response is None:
                return format_issues

            response_code = response.getcode()

            if not response_code or response_code != HTTPStatus.OK:
                _log.warning(f'bad response from jira search api with msg: {msg}')
                return format_issues

            response_data = json.loads(response.read().decode('utf-8'))

            issue_list = response_data.get('issues', [])
            for issue in issue_list:
                key = issue.get('key')
                issue_info = format_issues.setdefault(key, {})
                issue_fields = issue.get('fields', {})

                issue_info['site_id'] = issue_fields.get(JIRANames.site_id)
                issue_info['created'] = issue_fields.get(JIRANames.issue_created)
                issue_info['reporter'] = issue_fields.get(JIRANames.issue_reporter)
                issue_info['process_ids'] = issue_fields.get(JIRANames.process_ids)
                issue_info['upload_token'] = issue_fields.get(JIRANames.upload_token)
                issue_info['start_end_dates'] = issue_fields.get(JIRANames.start_end_dates)
                issue_info['upload_token'] = issue_fields.get(JIRANames.upload_token)
                issue_info['status'] = issue_fields.get(JIRANames.issue_status).get('name')

                for field_name in additional_fields:
                    issue_info[field_name] = issue_fields.get(field_name)

            total = response_data.get('total')
            start_at += max_results
            request_count += 1

            if request_count > max_request_count:
                _log.warning(f'Max requests {request_count} is more than the limit in '
                             'get_format_issues. Stopping request.')
                break

        return format_issues

    def get_sub_issues_to_link(self, key):
        url = f'{self.jira_ws_base}/issue/{key}'
        issue = self._basic_get_request(url)
        keys = []
        for s in issue['fields']['subtasks']:
            if s['fields']['status']['name'] not in ('Fixed', 'Canceled'):
                keys.append(s['key'])
        for issue_link in issue['fields']['issuelinks']:
            if 'inwardIssue' in issue_link.keys():
                continue
            status = issue_link['outwardIssue']['fields']['status']['name']
            if issue_link['type']['name'] == JIRANames.sub_issue_name \
                    and status not in (JIRANames.data_sub_issue_fixed,
                                       JIRANames.data_sub_issue_canceled):
                keys.append(issue_link['outwardIssue']['key'])
        return keys

    def create_organizations(self):
        site_dict = site_attrs.SiteAttributes().get_site_dict()
        for s in site_dict.keys():
            if s in self.org_dict:
                # site is already an organization in jira.
                continue
            self.create_org(s)

    def create_org(self, site_id):
        url = f'{self.jira_host}/rest/servicedeskapi/organization'
        msg = {'name': site_id}
        status_code, response = self._experimental_post_request(msg, url)
        if status_code == HTTPStatus.CREATED:
            orgs = json.loads(response.read().decode('utf-8'))
            if self.add_organization_to_service_desk(orgs['id']):
                return orgs['id']
        raise Exception(f'{url} returned status code {status_code}\n'
                        f'{response}')

    def add_organization_to_service_desk(self, org_id):
        url = (f'{self.jira_host}/rest/servicedeskapi/servicedesk/'
               f'{self.sd_id}/organization')
        msg = {'organizationId': org_id}
        status_code, response = self._experimental_post_request(msg, url)
        if status_code == HTTPStatus.NO_CONTENT:
            return True
        raise Exception(f'{url} returned status code {status_code}\n'
                        f'{response}')

    def add_users_to_organization(self, org_id, users):
        url = (f'{self.jira_host}/rest/servicedeskapi/organization/'
               f'{org_id}/user')
        msg = {'usernames': users}
        status_code, response = self._experimental_post_request(msg, url)
        if status_code != HTTPStatus.NO_CONTENT:
            raise Exception(f'{url} returned status code {status_code}\n'
                            f'{response}')

    def remove_users_from_organization(self, org_id, users):
        url = (f'{self.jira_host}/rest/servicedeskapi/organization/'
               f'{org_id}/user')
        msg = {'usernames': users}
        self._delete_request(msg, url)

    def run_query(self, jql, start_at=0):
        url = f'{self.jira_ws_base}/search?jql={jql}&startAt={start_at}'
        return self._basic_get_request(url)

    def ensure_org_exists(self, site_id):
        self.org_dict[site_id] = self.create_org(site_id)
        rs = ReportStatus()
        site_members = rs.get_site_users(site_id)
        # get the different types of members based on registration
        ad_users = site_members.get('AD_users')
        other_users = site_members.get('other_users')
        if ad_users:
            self.add_users_to_organization(
                self.org_dict[site_id], ad_users)
        if other_users:
            non_users = []
            for name, email in other_users.items():
                user = self.create_non_ad_user(name, email)
                if user is not None:
                    non_users.append(user)
            self.add_users_to_organization(self.org_dict[site_id], non_users)

    def create_non_ad_user(self, name, email):
        url = f'{self.jira_host}/rest/servicedeskapi/customer'
        msg = {'email': email, 'fullName': name}
        status_code, response = self._experimental_post_request(msg, url)
        if status_code == HTTPStatus.BAD_REQUEST:
            if 'A user with that username already exists' not in response:
                return None
            _log.info('User already exists')
        elif status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN,
                             HTTPStatus.NOT_FOUND):
            raise Exception('Error when trying to check organization for '
                            f'user {name}. Check error message.')
        return email

    def update_org_members(self, site_id):
        rs = ReportStatus()
        site_members = rs.get_site_users(site_id)
        # get the different types of members based on registration
        ad_users = site_members.get('AD_users', [])
        other_users = site_members.get('other_users', {})
        org_members = self.get_org_members(site_id)

        # initialize a few lists
        new_members = []
        other_users_email = []
        remove_members = []

        for m in ad_users:
            if m not in org_members:
                new_members.append(m)
        for name, email in other_users.items():
            # site_members[other_users] is a dict with
            #   key = site_team_member_name, value = site_team_member_email
            # We use the site_team_member_email to create non-ad users in JIRA
            #   when we automatically create the user, JIRA converts the email
            #   to all lowercase for the username
            if email.lower() not in org_members:
                user = self.create_non_ad_user(name, email)
                if user is not None:
                    new_members.append(user)
            # See comment above for why we check lowercase
            #   site_team_member_email for organization users
            other_users_email.append(email.lower())
        if len(new_members) > 0:
            self.add_users_to_organization(self.org_dict[site_id], new_members)
        # Check existing organization members. If they are no longer in
        #    ad_users or other_users (see above for why lowercase emails
        #    are used), gather the username for removal.
        for m in org_members:
            if m not in ad_users and m not in other_users_email:
                remove_members.append(m)
        if len(remove_members) > 0:
            self.remove_users_from_organization(
                self.org_dict[site_id], remove_members)

    def get_org_members(self, site_id):
        if site_id not in self.org_dict:
            return []
        url = (f'{self.jira_host}/rest/servicedeskapi/organization/'
               f'{self.org_dict[site_id]}/user')
        user_result = self._basic_get_request(url)
        site_members = []
        for user in user_result['values']:
            site_members.append(user['name'])
        return site_members

    def add_participants(self, issue_key, participants):
        msg = {'usernames': participants}
        url = (f'{self.jira_host}/rest/servicedeskapi/request/'
               f'{issue_key}/participant')
        self._basic_post_request(msg, url)


if __name__ == "__main__":
    _log = Logger(True, process_type='File Format').getLogger(__name__)
