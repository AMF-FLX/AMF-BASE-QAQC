import argparse
import copy
import json

from configparser import ConfigParser
from datetime import datetime, timedelta
from db_handler import DBHandler
from email_gen import EmailGen
from jira_interface import JIRAInterface
from jira_names import JIRANames
from logger import Logger

_log = Logger(
    setup=True, process_type='timeout_format_issues').getLogger(
    'timeout_format_issues')


class TimeOutIssue:
    def __init__(self, is_test=False):
        _log.info('Starting timeout format issue job.')
        jira_timeout_schedule_path = None
        jira_messages_path = None
        config = ConfigParser()
        with open('qaqc.cfg') as cfg:
            config.read_file(cfg)
            cfg_section = 'DB'
            if config.has_section(cfg_section):
                hostname = config.get(cfg_section, 'flux_hostname')
                user = config.get(cfg_section, 'flux_user')
                auth = config.get(cfg_section, 'flux_auth')
                jira_db = config.get(cfg_section, 'jira_db_name')
                qaqc_db = config.get(cfg_section, 'flux_db_name')
            else:
                hostname = None
                user = None
                auth = None
                jira_db = None
                qaqc_db = None
                _log.error(f'Config section {cfg_section} not found.')
            cfg_section = 'JIRA'
            if config.has_section(cfg_section):
                self.jira_project = config.get(cfg_section, 'project')
                jira_timeout_schedule_path = config.get(cfg_section,
                                                        'timeout_schedule')
            else:
                self.jira_project = None
                _log.error(f'Config section {cfg_section} not found.')
            cfg_section = 'REPORT_EMAIL'
            if config.has_section(cfg_section):
                jira_messages_path = config.get(cfg_section, 'messages_json')
            cfg_section = 'WEBSERVICES'
            if config.has_section(cfg_section):
                self.upload_reports_ws = config.get(
                    cfg_section, 'upload_reports')
            else:
                self.upload_reports_ws = None
                _log.error(f'Config section {cfg_section} not found.')
        if all([hostname, user, auth, jira_db]):
            self.jira_db_handler = DBHandler(hostname=hostname, user=user,
                                             password=auth, db_name=jira_db)
        else:
            self.jira_db_handler = None
            _log.error('JIRA DB Handler not set.')
        if all([hostname, user, auth, qaqc_db]):
            self.qaqc_db_handler = DBHandler(hostname=hostname, user=user,
                                             password=auth, db_name=qaqc_db)
        else:
            self.qaqc_db_handler = None
            _log.error('QAQC DB Handler not set.')
        if jira_timeout_schedule_path is not None:
            with open(jira_timeout_schedule_path) as f:
                self.timeout_json = json.load(f)
        else:
            self.timeout_json = None
            _log.error(
                'Could not open resource file \'timeout_schedule\' '
                f'at {jira_timeout_schedule_path}')
        try:
            with open(jira_messages_path) as f:
                self.message_json = json.load(f)
        except Exception as e:
            self.message_json = None
            _log.error(
                'Could not open resource file \'message_json\' '
                f'at {jira_messages_path}. ERROR: {str(e)}')
        self.jira_interface = JIRAInterface()
        self.current_date = datetime.now()
        self.is_test = is_test

    def get_jira_issues(self, status: str, reminder_field_values: list,
                        max_days_passed: int, has_labels: str = '') -> list:
        """
        Gather timed-out issues from SQL based on status, schedule, age &
        existing labels.

        :param status: str, Issue status text
        :param reminder_field_values: list, reminder values
        :param max_days_passed: int, Max days since status change among the
                                     reminder_fields
        :param has_labels: str, Labels existing on ticket

        :return: List of jira issues containing issue number, upload token,
                 & code version
        """
        if status != JIRANames.Status.waiting_for_customer_name:
            raise Exception(
                'Status must be '
                f'\'{JIRANames.Status.waiting_for_customer_name}\'')

        issue_status_id = JIRANames.Status.waiting_for_customer_id
        change_field = JIRANames.Status.__field_name__
        change_new_string = JIRANames.Status.waiting_for_customer_name

        reminder_field_values = tuple(reminder_field_values)

        if not has_labels:
            has_labels = list()
        elif isinstance(has_labels, str):
            has_labels = list(has_labels)
        has_labels.append(JIRANames.label_results_sent)
        has_labels = tuple(has_labels)

        return self.jira_db_handler.get_timeout_issues(
            self.jira_project, issue_status_id,
            reminder_field_values, change_field, change_new_string,
            max_days_passed, has_labels=has_labels)

    def get_process_code_version(self, timeout_issues: list) -> dict:
        """
        Get code version used for the upload based on first process_id in issue
            The code version is used to get the details for the timeout email
        :param timeout_issues: list, timeout issues returned by the jira query
        :return: dict, key = process_id, value = code_version
        """
        if self.is_test:
            return {'63084': '0.4.19', '63096': '0.4.34', '63097': '0.4.34'}

        process_ids = set()
        for issue in timeout_issues:
            # only the first processID is needed
            process_ids.add(issue['processID'].split('\n')[0])

        process_code_version = self.qaqc_db_handler.get_process_code_version(
            tuple(process_ids))

        code_version_lookup = {str(row['processID']): row['codeVersion']
                               for row in process_code_version}

        return code_version_lookup

    def get_timeout_issue_store(self, timeout_issues: list) -> dict:
        """
        Transform timeout_issue list into diction with a few extra fields.

        :param timeout_issues: list, list of timeout issues
        :return: dict, key = issue_number, value = dict issue details
        """
        code_version_lookup = self.get_process_code_version(timeout_issues)

        timeout_issue_store = {}
        for issue in timeout_issues:
            issue_num = str(issue.get('issuenum'))
            if issue_num in timeout_issue_store.keys():
                _log.error(f'Duplicate issue {issue_num} found. '
                           f'Skipping duplicate.')
                continue

            try:
                # look up code version by the first processID
                process_id = issue.get('processID', '').split('\n')[0]
                code_version = code_version_lookup.get(process_id)
                if code_version is not None:
                    issue_store = timeout_issue_store.setdefault(issue_num, {})
                    issue_store['upload_token'] = issue.get('uploadToken')
                    issue_store['code_version'] = code_version
                    issue_store['last_change_date'] = issue.get(
                        'last_change_date')
                else:
                    _log.error('Code Version not found for '
                               f'Process ID {process_id}.')
            except Exception as e:
                _log.error('Error occurred in get_jira_issues')
                _log.error(e)

        return timeout_issue_store

    def get_issue_labels(self, timeout_issues_store: dict) -> dict:
        """
        Get the current labels on each timeout issue
        :param timeout_issues_store: dict of timeout issues.
                               dict keys are issue numbers
        :return: dict, key = issue num, value = list of labels
        """
        issue_nums = list(timeout_issues_store.keys())
        return self.jira_db_handler.get_issue_labels(issue_nums)

    def get_msg_text(self, msg_key: str, issue_number: str) -> str:
        """
        Get the message text from the json file
        :param issue_number: str, issue number
        :param msg_key: message key from the reminder schedule
        :return:
        """
        if self.message_json is None or \
                msg_key not in self.message_json.keys():
            error = ('Jira Messages file or message not found. Comment for '
                     f'{issue_number} not added.')
            _log.error(error)
            return ''
        return self.message_json.get(msg_key)

    def gen_timeout_comment(self, issue_number: str, upload_token: str,
                            code_version: str, msg_key: str):
        """
        Create text for timeout message on Jira issue

        :param issue_number: str, Jira issue number
        :param upload_token: str, upload token from relevant upload
        :param code_version: str, version number of code base on upload
        :param msg_key: str, identifier for reminder message stored in
                        jira_messages.json

        :return: Jira timeout comment text
        """
        msg_text = self.get_msg_text(msg_key, issue_number)
        if not msg_text:
            return None

        # Keep in case want to indlude in test case in future.
        # upload_info = {'uploader': 'tester', 'SITE_ID': 'test-Site'}
        # format_msg = 'Test msg'

        upload_info = EmailGen().get_upload_data(
            upload_token, code_version)
        EmailGen().add_original_file_process_id_to_report(upload_info)
        file_statuses = EmailGen().get_file_statuses(upload_info)
        format_msg = EmailGen().generate_auto_email_components(
            upload_info, file_statuses)

        msg = msg_text.format(
            uploader=upload_info['uploader'],
            site_id=upload_info['SITE_ID'],
            # Add timestamp format to utils TimeStamps
            date=datetime.strftime(
                datetime.strptime(upload_info['datetime'][:19],
                                  '%Y-%m-%dT%H:%M:%S'),
                '%b %d, %Y'),
            format_results=format_msg,
            jira_ticket_link=self.get_project_issue_key(issue_number))
        return msg

    # ToDo: test
    def gen_internal_support_msg(self, issue_number: str,
                                 days_since_status_change: int,
                                 label_lookup: dict):
        """
        Generate the standard internal message that alerts the Data Team that
            the issue needs attention due to non standard reminder situation.

        :param issue_number: str, the issue number
        :param days_since_status_change: int, days since the status change
        :param label_lookup: dict, key = issue number, value = list of labels
        :return: str or None, text for JIRA comment
        """
        msg_text = self.get_msg_text('internal_timeout_support', issue_number)
        if not msg_text:
            return None
        existing_labels = '<none>'
        if issue_number in label_lookup.keys():
            existing_labels = label_lookup.get(issue_number)
            existing_labels = ', '.join(existing_labels)
        return msg_text.format(
            days_since_status_change=days_since_status_change,
            existing_labels=existing_labels)

    # ToDo: test
    def gen_internal_timeout_msg(self, issue_number: str,
                                 time_out_length: int):
        """
        Generate the standard internal message that alerts the Data Team that
            the issue needs attention due to no response in timeout period.

        :param issue_number: str, issue number
        :param time_out_length: int, number of days in timeout period
        :return:
        """
        msg_text = self.get_msg_text('internal_timeout', issue_number)
        if not msg_text:
            return None
        return msg_text.format(time_out_length=time_out_length)

    def get_max_day_passed(self, reminder_schedules: list) -> int:
        """
        Get the maximum days passed for the jira timeout issue query
        :param reminder_schedules: list, reminder schdules for a reminder type
        :return: int
        """
        days_passed = [r['days'] for r in reminder_schedules]
        return max(days_passed) + 10

    def get_query_params(self, reminder_id: str, status: str,
                         reminder_schedules: list) -> dict:
        """
        Create parameter dictionary to input into query

        :param reminder_id: str, Jira ID number of reminder field value
        :param status: str, Jira status name
        :param reminder_schedules: list, reminder schedules
        :return: Dictionary of query parameters
        """
        query_params = {
            'status': status,
            'reminder_field_values': [reminder_id],
            'max_days_passed': self.get_max_day_passed(reminder_schedules)
        }

        return query_params

    def has_unexpected_label(self, label_lookup: dict, issue_number: str,
                             expected_labels: list) -> bool:
        """
        Check issue labels for unexpected reminder labels. These include labels
            that are applied when time period is longer than status change.

            For example if the issue has only "Auto Reminder 2"
            (applied at day 14) and it is 8 days since the status changed,
            the issue has an unexpected reminder label.

            However, if the issue has no reminder labels and it is 10 days
            since the status change ("Auto Reminder 1" would be expected),
            then no unexpected labels are found.
            I.e., missing labels are not considered a problem.

        :param label_lookup: dict, key = issue num, value = list of labels
        :param issue_number: str, issue number
        :param expected_labels:, list of expected labels.
        :return: bool, True if unexpected labels, False if no unexpected labels
        """
        if issue_number not in label_lookup.keys():
            return False

        issue_labels = label_lookup.get(issue_number)
        reminder_labels = copy.deepcopy(
            self.timeout_json.get('reminder_labels'))

        if expected_labels:
            for expected_label in expected_labels:
                reminder_labels.remove(expected_label)

        for issue_label in issue_labels:
            if issue_label in reminder_labels:
                return True

        return False

    def has_new_label(self, issue_number: str, new_label: str,
                      label_lookup: dict) -> bool:
        """
        Check if the issue has the new label that is applied in
            the given time period
        :param issue_number: str, issue number
        :param new_label: str, new reminder label
        :param label_lookup: dict, key = issue number, value = list of labels
        :return: bool, True if issue has new label, False if issue does not
        """
        if issue_number not in label_lookup.keys():
            return False
        if new_label in label_lookup.get(issue_number):
            return True
        return False

    # ToDo: test
    def get_action_params(self, reminder_schedule: dict, issue_number: str,
                          issue_details: dict, days_since_status_change: int,
                          max_reminder_day: int, label_lookup: dict) -> dict:
        """
        Performs action for issue number, such as send message & add label or
        run transition

        :param reminder_schedule: dict, the reminder parameters
        :param issue_number: str, Jira issue number
        :param issue_details: dict, issue details
        :param days_since_status_change: int, number of days
                                              since the status changed
        :param max_reminder_day: int, max reminder day in the schedules
                                      for the give schedule type
                                      (auto, weekly, etc)
        :param label_lookup: dict, key = issue num, value = list of labels
        :return: dict, parameters for action
        """
        msg_type = reminder_schedule.get('msg')
        new_label = reminder_schedule.get('new_label')
        transition = reminder_schedule.get('transition')
        expected_labels = reminder_schedule.get('expected_labels')

        acceptable_labels = list()
        if expected_labels:
            acceptable_labels.extend(expected_labels)
        if new_label:
            acceptable_labels.append(new_label)

        if transition:
            transition = getattr(JIRANames, transition)

        msg = None
        is_public_msg = False
        add_label = None

        if self.has_unexpected_label(label_lookup, issue_number,
                                     acceptable_labels):
            msg_type = 'internal_timeout_support'
            msg = self.gen_internal_support_msg(
                issue_number, days_since_status_change, label_lookup)
            transition = JIRANames.format_waiting_for_support

        elif days_since_status_change >= max_reminder_day:
            msg_type = 'internal_timeout'
            msg = self.gen_internal_timeout_msg(
                issue_number, reminder_schedule.get('days'))

        elif (new_label and not self.has_new_label(
                issue_number, new_label, label_lookup)):
            add_label = new_label
            msg = self.gen_timeout_comment(
                issue_number, issue_details.get('upload_token'),
                issue_details.get('code_version'), msg_type)
            is_public_msg = True

        action_params = {
            'msg_type': msg_type,
            'msg': msg,
            'is_public_msg': is_public_msg,
            'add_label': add_label,
            'transition': transition}

        return action_params

    def get_project_issue_key(self, issue_number: str) -> str:
        """
        Get project key issue
        :param issue_number: str, issue number
        :return: str, project issue key
        """
        return f'{self.jira_project}-{issue_number}'

    def run_action(self, issue_number, msg_type=None, msg=None,
                   is_public_msg=False, add_label=None,
                   transition=None) -> bool:
        """
        Execute the actions needed
        :param issue_number: str, issue number
        :param msg_type: str, msg type (key in the jira_messages.json)
        :param msg: str, message text
        :param is_public_msg: bool, True if message sent to customer,
                                    False if internal message
        :param add_label: str, label to add to issue
        :param transition: str, status code of the state to transition to
        :return: bool
        """
        issue_key = self.get_project_issue_key(issue_number)
        if not msg:
            _log.error('Reminder action cannot be attempted '
                       'without a message.')
            return False

        try:
            self.jira_interface.add_comment(
                issue_key, msg, public=is_public_msg)
            _log.info(f'{issue_key} timeout comment {msg_type} added.')
            if add_label:
                self.jira_interface.add_label(issue_key, add_label)
                _log.info(f'{issue_key} timeout label \'{add_label}\' added.')
            if transition:
                self.jira_interface.set_issue_state(issue_key, transition)
                _log.info(f'{issue_key} transition {transition} performed.')
            return True

        except Exception as e:
            _log.error('An error occurred in performing the reminder action '
                       f'for issue {issue_key}. {e}')
            return False

    def calculate_time_diff(self, status_change_date: datetime) -> int:
        """
        Calculate the number of days since the status change and current date
        :param status_change_date: dt, the status change date
        :return: int, difference in status_change_date and current date
        """
        time_diff = self.current_date - status_change_date
        return time_diff.days

    def get_reminder_schedule_idx(self, days_since_status_change: int,
                                  reminder_schedules: list) -> int:
        """
        Get the index of the reminder schedule to use based on
            the days since status change
        :param days_since_status_change: int, days since status change
        :param reminder_schedules: list, reminder schedules
        :return: index of the reminder schedule to use (from the list)
        """
        # Work backwards (max day to min day) in the schedules. If the
        #    time since the status change is greater than the schedule's
        #    cutoff time, use that schedule.
        for idx in reversed(range(len(reminder_schedules))):
            if days_since_status_change >= reminder_schedules[idx]['days']:
                return idx
        return -1

    def process_timeout_issue(self, issue_number: str, issue_details: dict,
                              reminder_schedules: list, label_lookup: dict,
                              max_reminder_day: int) -> (bool, bool):
        """
        Process the timeout issue
        :param issue_number: str, issue number
        :param issue_details: dict, issue details
        :param reminder_schedules: list, reminder schedules
        :param label_lookup: dict, key = issue num, value = list of labels
        :param max_reminder_day: int, max day in reminder schedules
        :return: tuple(bool, bool), action required, action successful
        """

        days_since_status_change = self.calculate_time_diff(
            issue_details['last_change_date'])

        reminder_idx = self.get_reminder_schedule_idx(
            days_since_status_change, reminder_schedules)

        if reminder_idx >= 0:
            action_params = self.get_action_params(
                reminder_schedules[reminder_idx], issue_number, issue_details,
                days_since_status_change, max_reminder_day, label_lookup)

            # if there is a message, that means there needs to be action
            if action_params.get('msg'):
                return True, self.run_action(issue_number, **action_params)

            # if no message, no action is needed
            _log.info(f'No action needed for {issue_number}')
            return False, False
        # If the time since the status change is less than the minimum
        #    schedule cutoff time, check if there is an unexpected label.
        if self.has_unexpected_label(
                label_lookup, issue_number, list()):
            msg = self.gen_internal_support_msg(
                issue_number, days_since_status_change, label_lookup)
            return True, self.run_action(
                issue_number, msg_type='internal_timeout_support', msg=msg,
                transition=JIRANames.format_waiting_for_support)

        _log.info(f'No action needed for {issue_number}')
        return False, False

    def execute_reminder_schedule(self, reminder_schedules: list,
                                  timeout_issues: list) -> tuple:
        """
        Finds JIRA tickets that have passed scheduled due date &
        performs required action prescribed to that due date.

        :param reminder_schedules: list, reminder schedules
        :param timeout_issues: list, timeout issues from JIRA query
        """
        timeout_issue_store = self.get_timeout_issue_store(timeout_issues)
        label_lookup = self.get_issue_labels(timeout_issue_store)

        max_reminder_day = max([r.get('days') for r in reminder_schedules])

        issues_with_action = []
        action_issue_with_errors = []
        issues_with_no_action = []

        for issue_number, issue_details in timeout_issue_store.items():
            issue_with_action, successful_action = self.process_timeout_issue(
                issue_number, issue_details, reminder_schedules,
                label_lookup, max_reminder_day)
            if issue_with_action:
                issues_with_action.append(issue_number)
                if not successful_action:
                    action_issue_with_errors.append(issue_number)
                continue
            if not issue_with_action and successful_action:
                _log.error(f'Checkout issue {issue_number}: Successful action '
                           f'was reported for an non-action issue.')
            issues_with_no_action.append(issue_number)

        num_issues_with_action = len(issues_with_action)
        issues_with_action = ', '.join(issues_with_action)
        if num_issues_with_action < 1:
            issues_with_action = 'no issues'
        _log.info(f'Executed action for {num_issues_with_action} issues: '
                  f'{issues_with_action}.')

        num_issues_with_no_action = len(issues_with_no_action)
        issues_with_no_action = ', '.join(issues_with_no_action)
        if num_issues_with_no_action < 1:
            issues_with_no_action = 'no issues'
        _log.info(f'{num_issues_with_no_action} issues did not need action: '
                  f'{issues_with_no_action}.')

        if num_issues_with_action + num_issues_with_no_action != len(
                timeout_issue_store):
            _log.error('Action and non-action issues do not add up.')

        num_action_issues_with_errors = len(action_issue_with_errors)
        if num_action_issues_with_errors > 0:
            action_issue_with_errors = ', '.join(action_issue_with_errors)
            _log.error(f'{num_action_issues_with_errors} issues had erroneous'
                       f'action. Check them out:\n{action_issue_with_errors}')

        return (num_issues_with_action, num_issues_with_no_action,
                num_action_issues_with_errors)

    def get_reminder_schedule_info(self, reminder_type: str) -> (list, str):
        """
        Get the reminder schedules for the type of reminder
         :param reminder_type: String value for reminder schedule

        :return reminder_schedules: list of dict
        :return reminder_id: str Jira ID for reminder schedule value
        """
        reminder_schedules = (
            self.timeout_json['reminder_schedules'][reminder_type])
        # Get Jira reminder ID
        reminder_id = getattr(JIRANames.ReminderOptions(), reminder_type)
        return reminder_schedules, reminder_id

    def driver(self, status_filter=None, reminder_schedule_filter=None):
        """
        Searches timed-out issues & performs action based on scheme in
        `timeout_schedule.json`
        """
        status_schedules = self.timeout_json['status_schedules']
        for status, reminder_labels in status_schedules.items():
            if status_filter:
                if status not in status_filter:
                    continue
            for reminder_label in reminder_labels:
                if reminder_schedule_filter:
                    if reminder_label not in reminder_schedule_filter:
                        continue
                reminder_schedules, reminder_id = \
                    self.get_reminder_schedule_info(reminder_label)
                query_params = self.get_query_params(
                    reminder_id, status, reminder_schedules)
                timeout_issues = self.get_jira_issues(**query_params)
                if timeout_issues:
                    self.execute_reminder_schedule(
                        reminder_schedules, timeout_issues)

    def main(self):
        """
        Add system args for command line execution
        """
        parser = argparse.ArgumentParser(
            description='Detect JIRA issues needing a reminder according to '
                        'the issue\'s reminder schedule. Execute reminder '
                        'action based on issue status, labels, and days '
                        'since the status change.')
        parser.add_argument('--days_passed', type=int,
                            help='Days past current date. Used for testing.')
        parser.add_argument('--statuses', type=str, nargs='*',
                            help='Issue statuses to include in the '
                                 'timeout processing. They must be a status '
                                 'in the status_schedules dictionary in '
                                 'timeout_schedule.json ')
        parser.add_argument('--reminder_schedules', type=str, nargs='*',
                            help='Reminder schedules to process: '
                                 'auto, one_week, one_month, no_reminder')
        parser.add_argument('--test', '-t', action='store_true',
                            help='Test mode')
        args = parser.parse_args()
        timeout_issue = TimeOutIssue(is_test=args.test)

        if args.days_passed:
            timeout_issue.current_date = \
                datetime.now() + timedelta(days=args.days_passed)

        timeout_issue.driver(status_filter=args.statuses,
                             reminder_schedule_filter=args.reminder_schedules)


if __name__ == '__main__':
    TimeOutIssue().main()
