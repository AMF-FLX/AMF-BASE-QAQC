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

### Manual testing
20241121: The following test files are recommended for manual testing:
- US-UMB_HR_200001011000_200001012000.csv: All good. No autorepair (no reupload).
- US-UMB_HR_200001010000_200001012000.txt: Autorepair (Reupload)
- US-UMB_HR_201703150000_201703151200.pdf: Failed -- no autorepair try (no reupload)
- US-UMB_HR_200001010000_200001012000_bad2.csv: Failed autorepair (no reupload)
- US-UMB_HR_200001011000_200001012000_bad25.csv: Failed autorepair (reupload)
- US-UMB_HR_allgood.zip: Zip file with all good files (zip reupload only)
- US-UMB_HR.zip: Zip file with autorepair (zip reupload and autorepair upload)
- US-UMB_HR_one.zip: Zip file with single good file (no reupload)

Make sure you have a virtual environment with the requirements installs.
Double check the qaqc.cfg file that the correct test endpoints are configured.
Start the format_qaqc_driver.
Upload the test files separately. Then upload the first 3 together.
