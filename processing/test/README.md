# AMF-QAQC Processing Tests

This `test` folder contains all unit and end-to-end (e2e) tests for the processing portion of the QAQC data pipeline.

## Organization

Most test files will be named after the module that they test with `test_` prepended to the beginning of the filename. Is is important that test files start with `test_` or end with `_test` for pytest to automatically collect the test file (the latter naming method is preferred). Any data needed to run the tests, such as example plots or sample input `.csv` files, should be included in the `testdata` subfolder in a folder that matches the module name that the data will be used to test.

To run the tests you can simply run `pytest` from the `processing` directory.

## Unit Tests

Individual unit tests that test a single function or closely related group of functions are typically included as single functions within a module's test file. This function must also start with `test_` or end with `_test`. For example, most files will need to test the init function, so they will have something similar to the following:

```python
def test_init():
    # insert testing logic here
```

## End-to-end Tests

End-to-end (e2e) tests are included for certain modules. These tests typically are found in the same test file that has the unit tests for a given module and are typically under a function called `test_e2e`. For some modules (namely multivariate_intercomparison), the e2e tests might be in a separate folder in order to allow for test splitting on the remote CI/CD deployment.

e2e tests will run its corresponding module through its driver function using a sample input `.csv` file. The general order of an e2e test is to start with a single `.csv` file -> setup loggers -> process data with the DataReader module -> obtain a list of Status objects from the module driver function -> assert the content of the Status objects.

Individual runs of the e2e tests are specified in JSON files that match the name of the module they test (e.g. diurnal_seasonal.py -> test_diurnal_seasonal.json). These JSON files also specifiy runtime arguments including the `.csv` file name, as well as things like `site_id` and any other variable needed for the e2e test to run.

The `expected_results` section of the JSON files included things like the expected logs that should be saved and the expected status objects that should be returned. These logs and status objects will be used to assert that the driver function of the module being tested is working as expected.

In order to reduce the runtime needed to process the data on every run, some e2e tests will attempt to load cached versions of the processed csv data instead of using the DataReader module to process it. These cached files end in `.npy` and represent a numpy binary data format. The csv headers are also cached in a file ending in `_headers.txt`. These `.npy` and `_headers.txt` files can typically be found in the same directory as the input `.csv` files that they correspond to. If you wish to regenerate the `.npy` and `_headers.txt` files, you can simply delete them and then run the e2e tests again and the DataReader will re-process the data.

_Important: If you modify one of the test `.csv` files then you must delete the corresponding `.npy` file. Otherwise the e2e test will still be using the cached data from the original test `.csv` file. If you change the headers of the `.csv` files then the `_headers.txt` file should also be deleted._
