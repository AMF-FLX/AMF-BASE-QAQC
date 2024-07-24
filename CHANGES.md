# AMF-BASE-QAQC Change Log

### QAQC v2.1.2
*2024 Jul 24*

New in this release:
- Removal of undeployed modules that have not been migrated to the new infrastructure. This allows removal of pymssql package.

### QAQC v2.1.1
*2024 Jul 23*

New in this release:
- Removal of no longer used modules and / or functionality within modules.
- Update reset state functionality for publishing to work with new infrastructure.

Notes:
- There are a few not yet implemented modules still referencing the old infrastructure. Additional clean up forthcoming.

### QAQC v2.1.0
*2024 May 24*

This release includes updates to Data QA/QC and Publish functionality that are not backwards compatible.

New in this release:
- Support for new webservices in the Data QA/QC modules.
- Updates to the Publish modules for new infrastructure
- Updates to historical ranges for each site (used in diurnal seasonal pattern check).

Notes:
- There are additional helper modules still referencing the old infrastructure. Additional clean up forthcoming.

### QAQC v2.0.0
*2024 Apr 29*

This major release includes updates to Format QA/QC functionality that are not backwards compatible.

New in this release:
- Support for new data upload webservices.
- Format QA/QC driver has been added. This change involves reworking how the upload_checks module is run and the information returned from it.

Notes:
- The Data QA/QC and Publish functionality have incomplete transitions to the new infrastructure included in this version. We recommend prior versions are used for these modes until further updates are made.

### QAQC v1.2.6
*2023 Nov 01*

New in this release:
- Add support for new site_info web service.

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id.
  The webservice generating the online Format reports will be changed to accept either report_id.

### QAQC v1.2.5
*2023 Sept 08*

Bug fixes:
- Change PosixPath so string for json serialization in Data QA/QC. #71

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id.
  The webservice generating the online Format reports will be changed to accept either report_id.


### QAQC v1.2.4
*2023 Aug 30*

New in this release:
- Add support for preventing embargoed sites from being published.
 
Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. 
The webservice generating the online Format reports will be changed to accept either report_id.


### QAQC v1.2.3
*2023 June 07*

New in this release:
- Clean up log files.
 
Bug fixes:
- Format QA/QC check for forward filled timestamps does not run if timestamps are malformed. #23

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. 
The webservice generating the online Format reports will be changed to accept either report_id.


### QAQC v1.2.2
*2023 May 18*

New in this release:
- Support for python 3.6.9
- Updated Copyright Notice and added License agreement in prep for making the repo public. Both cover all prior releases.

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. 
The webservice generating the online Format reports will be changed to accept either report_id.

### QAQC v1.2.1
*2023 Mar 30*

New in this release:
- Enable access to BASE candidate source file generated on different infrastructure.

Bug fixes:
- Correct misapplication of new tagging PI functionality.

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. Change forthcoming.

### QAQC v1.2.0
*2023 Mar 23*

Bug fixes:
- Correct issue with tagging PI variables in BASE publish.
- Enable timestamp_alignment to handle data records with less than 16 days in a single year.

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. Change forthcoming.

### QAQC v1.1.0
*2023 Mar 03*

New in this release:
- Updates to self-review Data QA/QC email.
- Synchronize language across Data QA/QC check modules
- Minor updates to documentation.

Bug fixes:
- Correctly handle migrated webservice: get_site_users.

Notes:
- In the new Data QA/QC email, links to Format QA/QC reports use the autocorrected file report_id number if it exists instead of the original file report_id. Change forthcoming.

### QAQC v1.0.0
*2023 Feb*

Initial open-source release of the Format QA/QC, Data QA/QC, and BASE Publish modules. The functionality has not changed from v0.4.52, 
rather the relevant modules extracted from a larger collection of code.

New in this release:
- Enable connection to migrated webservice: get_site_users.
- Removed unused and outdated modules / functions
- Added documentation for open-sourcing

---

Releases below were generated in the archived repository AMF-QAQC. Open a [new discussion topic](https://github.com/AMF-FLX/AMF-BASE-QAQC/discussions) with any questions.

### QAQC v0.4.52
*2022 Jul 26*

New in this release:
- Add self-review labels to Data QA/QC jira tickets
- Add argument to main for forcing AMP Data QA/QC review (versus self-review)

- Notes:
- This version has partial code for automating Data QAQC run initiation.

### QAQC v0.4.51
*2022 Jul 18*

New in this release:
- Automated Self-Review QA/QC email
- Updates Data QA/QC timeshift
  - Update FAIL / WARNING logic
  - Modify sunrise/sunset point selection
  - Change SW_IN_POT to use max value in window
- Updates to Data QAQC threshold
- Additional tests to multivariate comparison module
- Make UI link a config variable

Notes:
- This version has partial code for automating Data QAQC run initiation.

### QAQC v0.4.50
*2022 May 27*

New in this release:
- Ustar filter bug fix

Notes:
- This version has partial code for automating Data QAQC run initiation.

### QAQC v0.4.49
*2022 May 16*

New in this release:
- Data QAQC modules (diurnal_seasonal, timeshift, thresholds, ustar_filter, multivariate_intercomparison) upgraded:
  - Statistics updated / added (e.g., slope deviation in multivariate_intercomparison, corrected R2 calculation in multivariate_intercomparison)
  - Result code logic added
  - Statistics and result code summarized in csv file per module
  - Logging and status objects overhauled and made consistent
  - Unit tests added
  - End-to-end (e2e) tests added

Notes:
- This version has partial code for automating Data QAQC run initiation.

### QAQC v0.4.48
*2022 Mar 23*

New in this release:
- Variable coverage module added to Data QA/QC that generates figures.
- Check for ONEFlux variables added to Data QA/QC.
- Check for presence of root and root_1_1_1 added to Data QA/QC. No messaging added to report yet.
- Data records that begin on the last hour of a year are truncated rather than backfilling to beginning of the year.
- Automated reminder messages (needs a cronjob to work).
- Unit and e2e tests for thresholds and timeshift modules.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.
- This version has partial code for automating Data QAQC run initiation.

### QAQC v0.4.47
*2022 Feb 2*

New in this release:
- Remove dependency on httplib2
- Update historical ranges

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.46
*2021 Jul 19*

New in this release:
- Bug fixes for timeshift messaging
- Correct flagging of erroneous points in timeshift figure
- Update Data QA/QC email draft
- Update url in summarizer query string

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.45
*2021 Apr 15*

New in this release:
- Update historical data for diurnal_seasonal.py module.
- Update diurnal_seasonal.py module to use specific variable names. Skip check if no historical data.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.44
*2020 Nov 18*

New in this release:
- Remove database connections to older database in the publish module.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.43
*2020 Oct 6*

New in this release:
- Updates to automated Format QA/QC email per UX testing

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.42
*2020 Sep 3*

New in this release:
- Bug fixes in publish modules

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.41
*2020 Aug 14*

New in this release:
- Tweaks to JIRA communications

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.40
*2020 Aug 10*

New in this release:
- Change Summarizer to use production database
- Tweaks to publishing module

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.39
*2020 Aug 10*

New in this release:
- New database handling
- Automatic reminders for Format QA/QC
- Format QA/QC features:
  - Catch forward timestamp filling,
  - Catch / fix trailing underscore in filename,
  - Allow FCH4 to satisfy mandatory variables.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.38
*2020 May 1*

New in this release:
- Additional functionality for new database handling

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.37
*2020 Mar 23*

New in this release:
- Corrects initialization of new DB handler in BASE creator module

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.
- v0.4.36 tag was mistakenly applied to develop branch. There is no release v0.4.36.

### QAQC v0.4.35
*2020 Mar 18*

New in this release:
- New database handling and BASE-BADM updates

Bug fixes:
- Potential radiation generation off by one
- JIRA user creation error
- Incorrect handling of invalid variables that affected threshold test

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.34
*2019 Sep 4*

New in this release:
- Automated Format QA/QC email generation
- Data Availability Updates
- Several bug fixes and additional tests in Format QA/QC

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.33
*2019 Jul 10*

New in this release:
- Updated QAQC RegisterBase web service for GetBaseCandidates
- Minor fixes to test

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.32
*2019 Feb 1*

New in this release:
- Bug fix in base_creator

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.31
*2019 Jan 25*

New in this release:
- Bug fixes for missing data in timeshift text
- Upgrades to publish module malformed file fix
- Removal of unused web service GET call in email_gen
- Other minor code clean up

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.30
*2018 Dec 26*

New in this release:
- Bug fixes for missing value format in Format QA/QC
- Refactor missing value format to incorporate testing
- Comment out spike detection in Data QA/QC
- Fix for publish module file malformation and md5sum reporting bug (not to be used until further testing).
- Add yml for easier on-boarding

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.29
*2018 Oct 16*

New in this release:
- Data version determined with checksum comparison
- Translate earlyFP BASE updated to use VariableInfo mappings.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.28
*2018 Sep 14*

New in this release:
- Updated radiation variable selection for ustar filter check.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.27
*2018 Aug 17*

New in this release:
- Fix for uploaded zipped files that have subdirectories and / or files that aren't really files.

Notes:
- Code version is not updated in qaqc_template.cfg file.
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.26
*2018 Aug 9*

New in this release:
- More bug fixes for HH and HR edge case. There is still an outstanding issue for sites with both HH and HR resolutions if both resolutions are actively being updated.
- Bug fix for applying _PI to only the variables.
- Correction to qaqc_template to point to the correct BADM directory

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.25
*2018 Aug 8*

New in this release:
- Bug fix for null case of HH and HR in phase III (publish) when resolutions are from pre and post QA/QC pipeline.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.24
*2018 Aug 8*

New in this release:
- Bug fix for HH and HR resolutions in phase III (publish) when resolutions are from pre and post QA/QC pipeline.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.23
*2018 Aug 8*

New in this release:
- Bug fixes to phase III (publish) for rebase_regen functionality
- Fix for qaqc state change that does not overwrite codeVersion
- Test mode for publish_base_badm

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.22
*2018 Aug 3*

New in this release:
- Bug fixes for phase III (publish)
- Improved documentation

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.21
*2018 Aug 3*

New in this release:
- Phase III (publish) fix to handle sites with both HR and HH data
- Fixes for Phase III to work with prebase_regen logic
- Add reset function to prebase_regen
- Minor tweaks to duplicate function in prebase_regen

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.20
*2018 Aug 2*

New in this release:
- Major version increase to 5
- Adds _PI to aggregated variables
- Updates to Phase III (publish)
- Bug fixes for multivariate comparison
- Regenerate preBASE function for re-running Phase III on already published Data QA/QC runs.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.19
*2018 Jun 29*

New in this release:
- Bug fix for whitespace before / after variable name
- Report upload file start and end date in JIRA issue

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.18
*2018 Jun 21*

New in this release:
- Bug fix for gap-filled radiation in ustar filter check.
- Turn TA-T_SONIC and WS-USTAR multivariate comparisons back on.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.17
*2018 Jun 14*

New in this release:
- Refactoring of code to meet flake8 standard
- Combiner: bug fix for including wrong files
- multivariate_comparison: cross-level and multi-level comparison functionality
- New BASE-BADM publish code to work with new backend production machine
- Minor updates in the messaging and email texts
- Minor refactoring throughout the codebase

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.16
*2017 Nov 20*

New in this release:
- Fix infinite loop in file_fixer when binary file was uploaded as CSV.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.15
*2017 Nov 9*

New in this release:
- Enforcement of correct order of qualifiers in Phase III (publish).
- Catch in Phase III for known edge case of qualifiers in wrong order that slip thru Format QA/QC. Fix in Format QA/QC forthcoming.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.14
*2017 Nov 7*

New in this release:
- Bug fix for new code deployed in v0.4.13

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.13
*2017 Nov 7*

New in this release:
- Comparison of data sources for candidate BASE file in publish phase.
- Support to write publishing information to new database table. Note we still write the BASE version number to main QA/QC processing table to preserve function of existing JIRA queries.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.12
*2017 Nov 2*

New in this release:
- Fix _PI and _F mislabeling of variables
- Reminder emails for Format QA\QC issues in "Waiting for Customer".
- Fix for utf-8 files with Byte Order Marks.
- Support for adding non-AD users to JIRA organizations and issues.

Notes:
- The major code version is increased to 4. This change is not reflected in the qaqc_template.cfg file but will be in the next version.
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.11
*2017 Sep 29*

New in this release:
- Fix to deal with old and new BASE versions.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.10
*2017 Sep 28*

New in this release:
- Fix in format of published BASE file.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.9
*2017 Sep 20*

New in this release:
- Entire data record is generated for files with standard FP-In variables only. Generation of entire data record for non-standard files (_NS) will be developed in a later version.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.8
*2017 Sep 15*

New in this release:
- Format QA/QC handles Inf values
- Bug fixes in timeshift and USTAR checks

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.7
*2017 Sep 12*

New in this release:
- Made timestamp data types consistent in Format QA/QC
- Fixed bugs in timeshift Data QA/QC (handles partial years)

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.6
*2017 Aug 30*

New in this release:
- Mechanism to update and track CDIAC-L2 BASE files.
- Minor updates to Format QA/QC.
- Full implementation of Data QA/QC timeshift and diurnal seasonal checks.
- Preliminary messaging for Data QA/QC timeshift and diurnal seasonal checks.

Notes:
- This version has a known bug in diurnal seasonal messaging. Fix is still forthcoming.

### QAQC v0.4.5
*2017 Aug 15*

New in this release:
- Data record is back-filled to Jan 1 00:00 if the first timestamp occurs after Jan 1 00:00.
- Format QA/QC handles .tar files.
- Minor fixes to Format QA/QC.

Notes:
- This version has a known bug in the multivariate comparison Data QAQC check. Fix is forthcoming.

### QAQC v0.4.4
*2017 Aug 5*

New in this release:
- Bug fix in Data QA/QC.
- Updates to figures.

Notes:
- This version has a known issue. Data records that do not start on Jan 1 00:00 will throw an exception in Data QA/QC. Fix is forthcoming.

### QAQC v0.4.3
*2017 Aug 4*

New in this release:
- VPD unit fix for historical data.
- Updates to the format fixer.
- JIRA enhancement to automatically generate draft email from sub-tasks (via a web hook).
- Additional code hardening.

Notes:
- This version has a known issue. Data records that do not start on Jan 1 00:00 will throw an exception in Data QA/QC. Fix is forthcoming.

### QAQC v0.4.2
*2017 Jul 12*

New in this release:
- Better Excel handling in the fixer.
- Suppression of variable name messages in NS files.
- Better handling of certain corrupted timestamps.
- Removal of PI flag from certain variables.
- Removal of FH2O to LE rename.

### QAQC v0.4.1
*2017 Jun 29*

New in this release:
- Retry limit when process is interrupted or code fails
- Backwards capability for zip handling
- Draft email corrections
- Web services updates to support UI changes

### QAQC v0.4
*2017 Jun 26*

New in this release:
- Integration with JIRA Service desk for issue tracking
- Archive file support
- Improved reprocessing support
- Lots of minor fixes and enhancements

### QAQC v0.3
*2017 May 22*

New in this release:
- Initial production release of Phase III (BASE-BADM generation and publish)
- Code development for interface with JIRA issue tracking
- Minor fixes to Phase I (Format QA/QC) and Phase II (Data QA/QC).

### QAQC v0.2.1
*2017 May 2*

New in this release:
- Minor fixes for Data QA/QC and in development BASE publishing.

### QAQC v0.2
*2017 Apr 20*

New in this release:
- Data QA/QC (phase II) and BASE Publish (phase III) code updates for initial production runs

### QAQC v0.1.1
*2017 Apr 11*

A port of QAQC v0.1.1 release from initial SVN repo.

New in this release:
- Bug fixes

### QAQC v0.1
*2017 Apr 7*

A port of QAQC v0.1 release from initial SVN repo.
