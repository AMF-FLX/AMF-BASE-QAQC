from timeout_jira_issue import TimeOutIssue
from jira_interface import JIRAInterface
from jira_names import JIRANames
from datetime import datetime, timedelta
import re


class JiraIssue:
    def __init__(self, key):
        self.key = key
        self.load_ticket()

    def load_ticket(self):
        self.info = JIRAInterface().get_jira_issue(self.key)
        self.status = self.info.get('fields', {}).get('status', {}).get('name')
        self.labels = sorted(self.info.get('fields', {}).get('labels'))
        self.last_comment = self.info.get('fields', {}).get('comment'). \
            get('comments')[-1].get('body')
        self.updated = self.info.get('fields', {}).get('updated')
        self.reminder_schedule = self.info.get('fields', {}). \
            get(JIRANames.reminder_schedule, {}).get('id')


def run_timeout(status, reminder_schedule, days_passed):
    timeout_issue = TimeOutIssue(is_test=True)
    timeout_issue.current_date = datetime.now() + timedelta(days=days_passed)
    timeout_issue.driver(status_filter=[status],
                         reminder_schedule_filter=[reminder_schedule])


def comment_timeout1_match(comment):
    timeout_ptn = re.compile(r'Dear .*?,\n\nThanks again for uploading data')
    return bool(timeout_ptn.match(comment))


def matches_reminder_schedule(jira_issue, reminder_schedule):
    ticket_reminder_id = jira_issue.reminder_schedule
    if not ticket_reminder_id == getattr(JIRANames.ReminderOptions,
                                         reminder_schedule):
        raise Exception(
            f'Expected reminder schedule of "{reminder_schedule}" '
            f'on {jira_issue.key}')


def reset_labels(jira_issue, starting_labels=[]):
    remove_labels = [label for label in
                     jira_issue.labels
                     if label not in starting_labels]
    add_labels = [label for label in starting_labels if label not in
                  jira_issue.labels]
    if remove_labels:
        JIRAInterface().remove_label(jira_issue.key, remove_labels)
    if add_labels:
        JIRAInterface().add_label(jira_issue.key, add_labels)
    jira_issue.load_ticket()


def reset_status(jira_issue, goal_status):
    transitions = {
        'waiting for customer': {
            'to_support': '151',
            'from_support': '161'},
        'replaced with upload': {
            'to_support': '191',
            'from_support': '141'},
        'attempt data qaqc': {
            'to_support': '191',
            'from_support': '171'}
    }
    if jira_issue.status == 'Waiting for support':
        if goal_status == 'Waiting for support':
            # transition to waiting for customer
            JIRAInterface().set_issue_state(
                jira_issue.key,
                transitions['waiting for customer']['from_support'])
            # transition to waiting for support
            JIRAInterface().set_issue_state(
                jira_issue.key,
                transitions['waiting for customer']['to_support'])
            jira_issue.load_ticket()
        else:
            JIRAInterface().set_issue_state(
                jira_issue.key,
                transitions[goal_status.lower()]['from_support'])
            jira_issue.load_ticket()
    elif goal_status == 'Waiting for support':
        JIRAInterface().set_issue_state(
            jira_issue.key,
            transitions[jira_issue.status.lower()]['to_support'])
        jira_issue.load_ticket()
    else:
        JIRAInterface().set_issue_state(
            jira_issue.key,
            transitions[jira_issue.status.lower()]['to_support'])
        JIRAInterface().set_issue_state(
            jira_issue.key,
            transitions[goal_status.lower()]['from_support'])
        jira_issue.load_ticket()


def waiting_customer_auto_less_than_seven():
    days_passed = 2
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_auto_less_than_seven_label_error():
    days_passed = 2
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Auto_Reminder_1']):
        raise Exception('Expected labels ["Results_Sent", "Auto_Reminder_1"]')
    if not test_ticket.last_comment.startswith(
            'This issue had an unexpected Reminder label'):
        raise Exception(
            'Expected internal comment with Unexpected Label message')


def waiting_customer_auto_seven_to_fourteen_no_label():
    # TODO Deal with SQL error here by putting in no labels.
    pass


def waiting_customer_auto_seven_to_fourteen_new():
    days_passed = 8
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(
            ['Results_Sent', 'Auto_Reminder_1']):
        raise Exception('Expected labels ["Results_Sent", "Auto_Reminder_1"]')
    if not comment_timeout1_match(test_ticket.last_comment):
        raise Exception('Expected comment of "Reminder for data uploaded"')


def waiting_customer_auto_seven_to_fourteen_done():
    days_passed = 8
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Auto_Reminder_1']):
        raise Exception('Expected labels ["Results_Sent", "Auto_Reminder_1"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_auto_seven_to_fourteen_flux_label():
    days_passed = 8
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'FLX-CA']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(
            ['Results_Sent', 'Auto_Reminder_1', 'FLX-CA']):
        raise Exception(
            'Expected labels ["Results_Sent", "Auto_Reminder_1", "FLX-CA"]')
    if not comment_timeout1_match(test_ticket.last_comment):
        raise Exception('Expected comment of "Reminder for data uploaded"')


def waiting_customer_auto_seven_to_fourteen_weekly_label_error():
    days_passed = 8
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Weekly']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Weekly']):
        raise Exception('Expected labels ["Results_Sent", "Weekly"]')
    if not test_ticket.last_comment.startswith(
            'This issue had an unexpected Reminder label'):
        raise Exception(
            'Expected internal comment with Unexpected Label message')


def waiting_customer_auto_fourteen_to_thirty_new():
    days_passed = 16
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1']
    test_ticket = JiraIssue('TESTQAQC-3103')

    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(
            ['Results_Sent', 'Auto_Reminder_1', 'Auto_Reminder_2']):
        raise Exception(
            'Expected labels ["Results_Sent", "Auto_Reminder_1",'
            ' "Auto_Reminder_2"]')
    if not comment_timeout1_match(test_ticket.last_comment):
        raise Exception('Expected comment of "Reminder for data uploaded"')


def waiting_customer_auto_fourteen_to_thirty_done():
    days_passed = 16
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1', 'Auto_Reminder_2']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(
            ['Results_Sent', 'Auto_Reminder_1', 'Auto_Reminder_2']):
        raise Exception(
            'Expected labels ["Results_Sent", "Auto_Reminder_1", '
            '"Auto_Reminder_2"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_auto_over_thirty():
    days_passed = 31
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1', 'Auto_Reminder_2']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(
            ['Results_Sent', 'Auto_Reminder_1', 'Auto_Reminder_2']):
        raise Exception(
            'Expected labels ["Results_Sent", "Auto_Reminder_1", '
            '"Auto_Reminder_2"]')
    if not test_ticket.last_comment.startswith('This issue has timed out.'):
        raise Exception('Expected internal comment with Timeout message')


def waiting_customer_auto_over_thirty_missing_label_error():
    days_passed = 31
    reminder_schedule = 'auto'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Auto_Reminder_1']):
        raise Exception(
            'Expected labels ["Results_Sent", "Auto_Reminder_1", '
            '"Auto_Reminder_2"]')
    if not test_ticket.last_comment.startswith('This issue has timed out.'):
        raise Exception('Expected internal comment with Timeout message')


def waiting_customer_one_week_less_than_seven():
    days_passed = 2
    reminder_schedule = 'one_week'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3104')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_one_week_seven_to_thirty_new():
    days_passed = 8
    reminder_schedule = 'one_week'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3104')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Weekly']):
        raise Exception('Expected labels ["Results_Sent", "Weekly"]')
    if not comment_timeout1_match(test_ticket.last_comment):
        raise Exception('Expected comment of "Reminder for data uploaded"')


def waiting_customer_one_week_seven_to_thirty_done():
    days_passed = 18
    reminder_schedule = 'one_week'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Weekly']

    test_ticket = JiraIssue('TESTQAQC-3104')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Weekly']):
        raise Exception('Expected labels ["Results_Sent", "Weekly"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_one_week_over_thirty():
    days_passed = 31
    reminder_schedule = 'one_week'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Weekly']

    test_ticket = JiraIssue('TESTQAQC-3104')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Weekly']):
        raise Exception('Expected labels ["Results_Sent", "Weekly"]')
    if not test_ticket.last_comment.startswith('This issue has timed out.'):
        raise Exception('Expected internal comment with Timeout message')


def waiting_customer_one_month_less_than_thirty():
    days_passed = 18
    reminder_schedule = 'one_month'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3105')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_one_month_over_thirty():
    days_passed = 31
    reminder_schedule = 'one_month'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3105')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Monthly']):
        raise Exception('Expected labels ["Results_Sent", "Monthly"]')
    if not comment_timeout1_match(test_ticket.last_comment):
        raise Exception('Expected comment of "Reminder for data uploaded"')


def waiting_customer_one_month_thirty_to_thirty_eight():
    days_passed = 36
    reminder_schedule = 'one_month'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Monthly']

    test_ticket = JiraIssue('TESTQAQC-3105')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Monthly']):
        raise Exception('Expected labels ["Results_Sent", "Monthly"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_one_month_over_thrity_eight():
    days_passed = 40
    reminder_schedule = 'one_month'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Monthly']

    test_ticket = JiraIssue('TESTQAQC-3105')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == sorted(['Results_Sent', 'Monthly']):
        raise Exception('Expected labels ["Results_Sent", "Monthly"]')
    if not test_ticket.last_comment.startswith('This issue has timed out.'):
        raise Exception('Expected internal comment with Timeout message')


def waiting_customer_no_reminder_less_than_thirty():
    # TODO need code version for process ID 63055
    days_passed = 22
    reminder_schedule = 'no_reminder'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3106')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for customer':
        raise Exception('Expected status of "Waiting for customer"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_customer_no_reminder_less_than_thirty_error():
    # TODO need code version for process ID 63055
    days_passed = 22
    reminder_schedule = 'no_reminder'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent', 'Auto_Reminder_1']

    test_ticket = JiraIssue('TESTQAQC-3106')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.last_comment.startswith(
            'This issue had an unexpected Reminder label'):
        raise Exception(
            'Expected internal comment with Unexpected Label message')


def waiting_customer_no_reminder_over_thirty():
    # TODO need code version for process ID 63055
    days_passed = 32
    reminder_schedule = 'no_reminder'
    status = 'Waiting for Customer'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3106')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.last_comment.startswith('This issue has timed out.'):
        raise Exception('Expected internal comment with Timeout message')


def attempt_data_qaqc_auto_less_than_seven():
    days_passed = 5
    reminder_schedule = 'auto'
    status = 'Attempt Data QAQC'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()
    if not test_ticket.status == 'Attempt Data QAQC':
        raise Exception('Expected status of "Attempt Data QAQC"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def waiting_support_auto_less_than_seven():
    days_passed = 5
    reminder_schedule = 'auto'
    status = 'Waiting for support'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Waiting for support':
        raise Exception('Expected status of "Waiting for support"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')


def replacement_upload_auto_less_than_seven():
    days_passed = 5
    reminder_schedule = 'auto'
    status = 'Replaced with Upload'
    starting_labels = ['Results_Sent']

    test_ticket = JiraIssue('TESTQAQC-3103')
    matches_reminder_schedule(test_ticket, reminder_schedule)
    reset_labels(test_ticket, starting_labels)
    reset_status(test_ticket, status)
    start_ticket_updated = test_ticket.updated

    run_timeout(status, reminder_schedule, days_passed)

    test_ticket.load_ticket()

    if not test_ticket.status == 'Replaced with Upload':
        raise Exception('Expected status of "Replaced with Upload"')
    if not test_ticket.labels == ['Results_Sent']:
        raise Exception('Expected labels ["Results_Sent"]')
    if not test_ticket.updated == start_ticket_updated:
        raise Exception('Unexpected change was made to ticket')
