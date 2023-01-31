#!/usr/bin/env python

from logger import Logger

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'

_log = Logger().getLogger(__name__)


class Messages:
    def __init__(self):
        self.msgs = []
        self.checknames = {}
        self.read_messages()
        self.build_display_check_dict()

    def read_messages(self, fname='Check_messages.txt'):
        msg_list = []
        with open(fname) as f:
            for line in f.read().split('\n'):
                msg_list.append(line.split(';'))
        if all([len(msg_item) == 7 for msg_item in msg_list]):
            # print('all good')
            for testname, check, stat, action, msg, prefix, suffix in msg_list:
                dic = {
                    'test_name': testname,
                    'check_name': check,
                    'status': stat,
                    'type': action,
                    'message': msg,
                    'report_prefix': prefix,
                    'report_suffix': suffix
                }
                self.msgs.append(dic)
        else:
            _log.fatal("Message dictionary was not generated load properly.")
            print('we have a problem')

    def get_msg_dict(self):
        return self.msgs

    def get_msg(self, testname, stat, msg_type='report_prefix'):
        if msg_type not in ('message', 'report_prefix', 'report_suffix'):
            return '(get_msg) Message type {m} not valid'.format(m=msg_type)
        if testname not in self.checknames.keys():
            return '(get_msg) Check name {cn} not valid'.format(cn=testname)
        msg = [m for m in self.msgs
               if m['test_name'] == testname
               and m['status'] == stat][0]
        if msg:
            return msg[msg_type]
        else:
            return ('(get_msg) Message for test {t} '.format(t=testname)
                    + 'with status {s} not found'.format(s=stat))

    def build_display_check_dict(self):
        for m in self.msgs:
            if m['test_name'] not in self.checknames.keys():
                self.checknames[m['test_name']] = m['check_name']

    def get_display_check(self, testname):
        if testname not in self.checknames.keys():
            return 'Display name for QA/QC check not found.'
        else:
            return self.checknames[testname]
