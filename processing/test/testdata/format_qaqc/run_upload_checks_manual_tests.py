#!/usr/bin/env python

import subprocess
import string
import time

__author__ = 'Danielle Christianson'
__email__ = 'dschristianson@lbl.gov'


# This is a test utility script for running the test files locally.

# DO NOT run it on your local machine unless you know how to set up the
#    config file properly to write to the test system.

which_script = 'upload_checks.py'

which_python = '3.6'

# ************************************************************
if which_python == '3.6':
    subprocess.call(['which', 'python'])
    # subprocess.call(['/bin/source', 'activate', 'python3'])

wdir = '/Users/dsc/Documents/LBNL/ameriflux/AMF-FLX/AMF-QAQC'
fname = '{wdir}/tests/file_check_expected_issues_ts3.csv'.format(wdir=wdir)
with open(fname, 'r') as f:
    files = f.readline()
    notes = f.readline()

files = files.split(',')
files = [h.strip(string.whitespace) for h in files]
notes = notes.split(',')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(' ')
print(files)
start_index = 1
check_length = len(files)
print(check_length)
print(' ')
print(' ')
counters = start_index
for ifile, n in zip(files[start_index:check_length],
                    notes[start_index:check_length]):
    if str(counters) not in ('0'):   # 38 test not implemented yet
        print('************************************************')
        print('*****  {c} -- {note}'.format(note=n, c=counters))
        print('************************************************')
        time.sleep(1)
        # subprocess.call([which_python.format(f=f)])
        subprocess.call(
            ['python',
             '{wdir}/processing/{ws}'.format(wdir=wdir, ws=which_script),
             '{wdir}/tests/testdata/{ifile}'.format(wdir=wdir, ifile=ifile),
             '9999', 'o', 'US-UMB', '-t'
             ])
        print(' ')
        print(' ')
        print(' ')
    if 55 < counters < 62:
        # test cases 55 thru 62 have different outcomes based on whether it is
        #   an original file or autocorrected file. E.g., bad variable names
        #   in an original file trigger the fixer whereas they do not in
        #   an autocorrected file. So I run the autocorrected file for these
        #   issue as well as the original file.
        print('************************************************')
        print('*****  {c} AUTOREPAIR -- {note}'.format(note=n, c=counters))
        print('************************************************')
        # subprocess.call([which_python.format(f=f)])
        subprocess.call(
            ['python',
             '{wdir}/processing/{ws}'.format(wdir=wdir, ws=which_script),
             '{wdir}/tests/testdata/{ifile}'.format(wdir=wdir, ifile=ifile),
             '9999', 'r', 'US-UMB', '-t'
             ])
        print(' ')
        print(' ')
        print(' ')
    counters += 1

# if which_python == '3.6':
    # subprocess.call(['/bin/source', 'deactivate', 'python3'])
