## Format QA/QC module end-to-end test files.

**Do NOT use these test files as valid data.**

Original test files committed on 19Feb2017 by Danielle Christianson. 
They are modifications of a test file provided by Housen Chu and have
various errors introduced.

The modifications are described in: "file_check_expected_issues_ts3.csv"

The files can be run in bulk manually with the "run_upload_checks_manual_tests.py",
and the output report status object visually inspected. The report status object
contains the majority of information used to build the online Format QA/QC report.

Files updated 20Feb2017 by dsc:
- End timestamps did not match file names b/c mistakenly used time in TIMESTAMP_START. 
- Deleted last entry in affected files.   
