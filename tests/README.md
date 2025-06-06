*Inspired by Integration testing in https://github.com/VITO-RS-Vegetation/lcfm-production.*

# Testing


This repository uses [pytest](https://docs.pytest.org/en/latest/) for testing. The tests are located in the `tests` directory.

There are two sets of tests that can be run: unit tests and integration tests. The unit tests are run on the local machine, while the integration tests are run on the OpenEO backend.

### Unit tests
The unit tests are run on the local machine and are used to test the functionality of the code. The unit tests are run using pytest and can be run using the following command:

```
pytest --disable-warnings -vv
```
The `-vv` flag is optional and can be used to increase verbosity. This is useful for debugging process graphs differences.

Running the unit tests will check (and create if non-existent) the process graphs for all the implemented products. For each product there is also a test that creates a benchmark scenario to be used by the integration tests. These benchmark scenarios contain the process_graphs, as well as job-options and extra information like a title and description. They follow the format layed out by the openeo-apex-tests repository.  

### Integration tests
The integration can be run form your own machine or from a GitHub action (required before a merge into main).  

NOTE: Running the integration tests will only start jobs for the process graphs that have been changed compared to the main branch. This is done to avoid running all integration tests every time you push a change.

#### Integration tests on your own machine
Running them from your own machine will start the openEO jobs using your own authentication.
You can run them using the following command:

```
pytest --integration --disable-warnings -vv
```
If you use vscode and want to use the testing functionality, you can add the following to your [vscode settings.json](.vscode/settings.json) file (pr create it if it does not exist):

```json
{
    "python.testing.pytestArgs": [
        "--disable-warnings",
        "-vv",
        "--integration"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

#### Integration tests from GitHub action
To run the integration tests from a GitHub action, follow these steps:
1. Push your changes to a branch on GitHub.
2. Go to the "Actions" tab of your repository on GitHub.
3. Select the "Integration tests" workflow from the list of workflows. (It is pinned)
4. Click on the "Run workflow" button and select the branch you want to run the tests on.
5. Click on the "Run workflow" button to start the workflow.

Running the integration tests from GitHub actions is required to succeed before merging into the main branch. 
