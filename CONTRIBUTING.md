## Contributing to AMF-BASE-QAQC
Our preferred channels of communication are public following our [Code of Conduct](CODE_OF_CONDUCT.md). 
Please review existing issues and discussion topics before opening a [new discussion topic](https://github.com/AMF-FLX/AMF-BASE-QAQC/discussions) on Github discussions.

Currently, development is limited to the AmeriFlux Management Project (AMP) team.

## Development Practices
* AMP uses the [GitFlow model](https://datasift.github.io/gitflow/IntroducingGitFlow.html) of branching and code versioning in git. 
* Code development will be performed in a development branch of the repo or a fork. 
  * Reference the issue number in the branch name when possible.
  * Commits will *NOT* be made directly to the develop and main branches of the repo.
  * It's good practice to check in your branch as you are developing.
* Sensitive information (usernames, passwords, etc) and configuration data (e.g database host port) should *NOT* be checked into the repo. See additional information below.
* Unit tests should be created / modified with each feature or bug fix.
* Developers will submit a pull request to the develop branch that is then reviewed / merged by another team member.
  * Each pull request should contain only related modifications to a feature or bug fix. 
* A practice of rebasing with the main repo should be used rather than merge commits. 
* We follow PEP8 formatting.

### Sensitive information guidelines
Below are considered sensitive information -- make sure this information is removed from any code that you push to any branch of this repo.
* Any endpoints other than ameriflux.lbl.gov and amfcdn.lbl.gov.
* Any directory paths that include machine names and usernames.
* Any usernames and passwords for general AMP accounts, AMP members, AMP test accounts, and community member accounts.
  * To reference machine names in issue tickets, use "production" and "develop".
* Database names.

These types of sensitive information are configured in the qaqc.cfg file.
Use the qaqc_template.cfg file modified as follows:
* Fill in anything left blank.
* Replace "path" with the appropriate directory path.
* Replace "url" with the appropriate url endpoint.

In addition, check with AMP development leads to discuss the use of any data for testing purposes.

## Versioning
We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/AMF-FLX/AMF-BASE-QAQC/tags). For versions prior to 1.0.0, see the [Change Log](CHANGES.md).

## Questions
Our preferred channels of communication are public. Please open a [new discussion topic](https://github.com/AMF-FLX/AMF-BASE-QAQC/discussions) on Github discussions


