[![CircleCI](https://dl.circleci.com/status-badge/img/gh/AMF-FLX/AMF-BASE-QAQC/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/AMF-FLX/AMF-BASE-QAQC/tree/main)

# AMF-BASE-QAQC
## AmeriFlux BASE Flux/Met Data QA/QC and Processing

The AmeriFlux BASE Flux/Met Data QA/QC and Processing (AMF-BASE-QAQC) code provides tools to review and prepare continuous flux/met data submitted to the AmeriFlux Management Project (AMP) for publication as the AmeriFlux BASE data product.

The code provides 3 core functionalities:
1. Format QA/QC assesses submitted data files for compliance with the required submission format;
2. Data QA/QC assesses the data quality;
3. BASE Publish prepares the data for publication.

Find more details about the AmeriFlux BASE data product and AmeriFlux processing pipelines at [AmeriFlux Flux Data Products](https://ameriflux.lbl.gov/data/flux-data-products/).

## Getting Started

### Install

Python version 3.6.9 or greater is required. See requirements.txt for required packages.

Notes:
* Some modules do not yet work on python versions greater than 3.6.9.
* psycopg2 and pymssql packages are only required for Publish BASE module.
* codecov, flake8, pytest-cov, and pytest-flake8 packages are required for AMP testing.

### Running modules

Processing modules are in the ./processing directory:
* Format QA/QC: upload_checks.py
* Data QA/QC: main.py
* BASE Publish: base_badm_main.py, publish_base_badm.py

Format QA/QC and Data QA/QC have test modes that allow local assessment of data files. See modules above for more detail.

## Changelog

See the [Change Log](CHANGES.md) for a history of updates and changes to AMF-BASE-QAQC.

## Support

For more information about the AmeriFlux network and the AmeriFlux Management Project visit https://ameriflux.lbl.gov/.

Our preferred channels of communication are public. Please check out existing or add a new [discussion topic](https://github.com/AMF-FLX/AMF-BASE-QAQC/discussions) on Github discussions.

## Authors

### Code Development

* **Danielle S. Christianson** - [LBL](https://crd.lbl.gov/divisions/scidata/ids/staff/danielle-christianson/)
* **You-Wei Cheah** - [LBL](https://crd.lbl.gov/divisions/scidata/ids/staff/you-wei-cheah/)
* **Housen Chu** - [LBL](https://eesa.lbl.gov/profiles/housen-chu/)
* **Gilberto Pastorello** - [LBL](https://crd.lbl.gov/divisions/scidata/uds/staff/gilberto-pastorello/)
* **Joshua Geden** - LBL
* **Fianna O’Brien** - [LBL](https://crd.lbl.gov/divisions/scidata/ids/staff/fianna-obrien/)
* **Sy-Toan Ngo** - [LBL](https://crd.lbl.gov/divisions/scidata/ids/staff/sy-toan-ngo/)
* **Norman F. Beekwilder** - [University of Virginia](https://www.linkedin.com/in/norm-beekwilder-a3a24b127/)
* **Alessio Ribeca** - Fondazione Centro Euro-Mediterraneo sui Cambiamenti Climatici
* **Carlo Trotta** - [Division Impacts on Agriculture, Forests and Ecosystem Services (IAFES), Fondazione Centro Euro-Mediterraneo sui Cambiamenti Climatici](https://www.researchgate.net/profile/Carlo-Trotta)

### Evaluation

* **Stephen W. Chan** - [LBL](https://eesa.lbl.gov/profiles/wai-yin-stephen-chan/)
* **Sigrid Dengel** - [LBL](https://eesa.lbl.gov/profiles/sigrid-dengel/)
* **Dario Papale** - [University of Tuscia](https://www.researchgate.net/profile/Dario-Papale)
* **Sébastien C. Biraud** - [LBL](https://eesa.lbl.gov/profiles/sebastien-biraud/)
* **Deborah A. Agarwal** - [LBL](https://crd.lbl.gov/divisions/scidata/about-scidata/office-of-the-director/agarwal/)


## Copyright Notice

AmeriFlux BASE Flux/Met Data QA/QC and Processing (AMF-BASE-QAQC) Copyright (c) 2023, 
The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of
any required approvals from the U.S. Dept. of Energy), University of
Virginia, and University of Tuscia.  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Intellectual Property Office at
IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department
of Energy and the U.S. Government consequently retains certain rights.  As
such, the U.S. Government has been granted for itself and others acting on
its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
Software to reproduce, distribute copies to the public, prepare derivative 
works, and perform publicly and display publicly, and to permit others to do so.

## License

See [LICENSE](LICENSE) file for licensing details

## Acknowledgments

This material is based upon work supported by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research, Environmental System Science Program, under Award Number DE-AC02-05CH11231.

## References
*Note: Datasets have been modified for testing purposes. Visit [AmeriFlux](https://ameriflux.lbl.gov/) for the latest AmeriFlux BASE data products.*

Jiquan Chen, Housen Chu (2021), AmeriFlux BASE US-CRT Curtice Walter-Berger cropland, Ver. 5-5, AmeriFlux AMP, (Dataset). https://doi.org/10.17190/AMF/1246156

Gerald Flerchinger (2021), AmeriFlux BASE US-Rws Reynolds Creek Wyoming big sagebrush, Ver. 4-5, AmeriFlux AMP, (Dataset). https://doi.org/10.17190/AMF/1375201

Christopher Gough, Gil Bohrer, Peter Curtis (2022), AmeriFlux BASE US-UMB Univ. of Mich. Biological Station, Ver. 18-5, AmeriFlux AMP, (Dataset). https://doi.org/10.17190/AMF/1246107

Gilberto Pastorello, Carlo Trotta, Eleonora Canfora, et al. (2020), The FLUXNET2015 dataset and the ONEFlux processing pipeline for eddy covariance data. Sci Data 7, 225. https://doi.org/10.1038/s41597-020-0534-3