#!/usr/bin/env python

from logger import Logger
from jira_interface import JIRAInterface
from jira_names import JIRANames
from report_status import ReportStatus
from datetime import datetime as dt
from configparser import ConfigParser

__author__ = 'Norm Beekwilder'
__email__ = 'norm.beekwilder@gmail.com'
_log = Logger().getLogger(__name__)


class RemindSites:
    def __init__(self):
        self.jira = JIRAInterface()
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            cfg_section = 'REMIND'
            if config.has_section(cfg_section):
                self.waiting_query = config.get(cfg_section, 'waiting_query')
        self.action_message = 'Dear {pi},\n  We received an upload from ' \
                              'your site team and processed your data ' \
                              'through the AmeriFlux  Format QA/QC on {d}. ' \
                              'As the results indicated, we cannot ' \
                              'process your data further. You can see the ' \
                              'full details by clicking on the “View ' \
                              'request” link below. Use your AmeriFlux ' \
                              'credentials to sign in.\nPlease correct the ' \
                              'issues identified in the results and ' \
                              're-upload this data so we can proceed with ' \
                              'Data QA/QC and publish the data. Please ' \
                              'reply to this email with any questions. ' \
                              '\nSincerely, ' \
                              '\nAMP Data Team'
        self.review_message = 'Dear {pi},\n We received an upload from your ' \
                              'site team and processed your data ' \
                              'through the AmeriFlux  Format QA/QC on {d}. ' \
                              'The results indicated that we can process ' \
                              'your data; however, there were significant ' \
                              'warnings. We would like your confirmation ' \
                              'before proceeding. You can see the full ' \
                              'details by clicking on the “View request” ' \
                              'link below. Use your AmeriFlux credentials ' \
                              'to sign in.\nPlease either confirm that ' \
                              'you would like us to proceed or correct the ' \
                              'issues identified in the results and ' \
                              're-upload this data so we can proceed with ' \
                              'Data QA/QC and publish the data. Please ' \
                              'reply to this email with any questions. ' \
                              '\nSincerely, ' \
                              '\nAMP Data Team'

    def send_reminders(self):
        issue_dict = self.get_issues()
        for site_id in issue_dict:
            self.jira.update_org_members(site_id)
            site_dict = ReportStatus().get_base_info(site_id, [])
            for key in issue_dict[site_id]:
                issue = issue_dict[site_id][key]
                if issue['summary'].startswith(
                        'Format Results - ACTION REQUIRED'):
                    msg = self.action_message.format(
                            pi=site_dict['reporter_name'],
                            d=dt.strftime(dt.strptime(
                                    issue['created'][:21],
                                    "%Y-%m-%dT%H:%M:%S.%f"), "%b %d, %Y"))
                elif issue['summary'].startswith(
                        'Format Results - Review requested'):
                    msg = self.review_message.format(
                            pi=site_dict['reporter_name'],
                            d=dt.strftime(dt.strptime(
                                    issue['created'][:21],
                                    "%Y-%m-%dT%H:%M:%S.%f"), "%b %d, %Y"))
                else:
                    continue
                new_participants = []
                if site_dict['reporter_id'] not in issue[
                        JIRANames.participants] \
                   and site_dict['reporter_id'] != issue[
                           'reporter']['name']:
                    new_participants.append(site_dict['reporter_id'])
                for p in site_dict['participant_ids']:
                    if p not in issue[JIRANames.participants] \
                       and p != issue['reporter']['name']:
                        new_participants.append(p)
                if len(new_participants) > 0:
                    self.jira.add_participants(key, new_participants)
                self.jira.add_comment(key, msg, True)

    def get_issues(self):
        count = 0
        issue_dict = {}
        while True:
            results = self.jira.run_query(self.waiting_query, count*50)
            for issue in results['issues']:
                site_id = issue['fields'][JIRANames.organizations][0]['name']
                if site_id not in issue_dict:
                    issue_dict[site_id] = {}
                issue_dict[site_id][issue["key"]] = issue["fields"]
            if len(results['issues']) < results['maxResults']:
                break
            count += 1
        return issue_dict

    def driver(self):
        return self.send_reminders()

    def main(self):
        return self.driver()


if __name__ == "__main__":
    # _log = Logger(True).getLogger(__name__)
    print(RemindSites().main())
