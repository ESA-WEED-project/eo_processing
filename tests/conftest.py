import json
import os
from pathlib import Path

import openeo
import pytest
from openeo.rest._testing import build_capabilities, DummyBackend
from openeo.util import url_join
import tests.config_collections as collections


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        dest="integration",
        default=False,
        help="enable integration tests for running changed process graphs",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark test as integration test")


def pytest_collection_modifyitems(config, items):

    if config.getoption("--integration"):
        # integration given in cli: skip all tests without the marker
        for item in items:
            if "integration" not in item.keywords:
                item.add_marker(pytest.mark.skip(
                    reason="Test not marked as integration test and --integration given"
                ))
    else:
        # integration not given in cli: skip all tests with the marker
        skip_integration = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


OPENEO_API_URL = "https://oeo.test/"
STAC_CAT_URL = "https://catalogue.weed.test"

GROUNDTRUTH_DIR = "tests//resources"
BBOX = {"east": 4880000, "south": 2898000, "west": 4878000, "north": 2900000, 'crs': 'EPSG:3035'} # 2x2 km bbox in Germany
DATE_START = "2021-01-01"
DATE_END = "2022-01-01"
TARGET_CRS: int = 3035
TARGET_RESOLUTION: float = 10.

INTEGRATION_JOB_OPTIONS = {
      "driver-memory": "1000m",
      "driver-memoryOverhead": "1000m",
      "executor-memory": "1500m",
      "executor-memoryOverhead": "1500m",
      "python-memory": "4000m",
      "max-executors": 20,
      "udf-dependency-archives": [
         "https://s3.waw3-1.cloudferro.com/swift/v1/project_dependencies/onnx_dependencies_1.16.3.zip#onnx_deps"
         ]}


@pytest.fixture
def api_capabilities() -> dict:
    """
    Fixture to be overridden for customizing the capabilities doc used by connection fixtures.
    To be used as kwargs for `build_capabilities`

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client
    """
    return {}

@pytest.fixture
def oeo_con100(requests_mock, api_capabilities):
    """
    Fixture to create a connection to the dummy backend using openeo 1.0.0.

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client.
    """
    requests_mock.get(
        OPENEO_API_URL + "udf_runtimes",
        json={
            "Python": {
                "type": "language",
                "default": "3",
                "versions": {"3": {"libraries": {}}},
            },
        },
    )
    requests_mock.get(
        OPENEO_API_URL, json=build_capabilities(api_version="1.0.0", **api_capabilities)
    )
    # TODO remove Mocking and use DummyBackend after production openeo.rest.testing 
    requests_mock.get(OPENEO_API_URL+ "collections/SENTINEL1_GRD", json=collections.DEFAULT_S1_METADATA)
    requests_mock.get(OPENEO_API_URL+ "collections/SENTINEL2_L2A", json=collections.DEFAULT_S2_METADATA)
    requests_mock.get(OPENEO_API_URL+ "collections/COPERNICUS_30", json=collections.DEFAULT_DEM_METADATA)
    requests_mock.get(url_join(STAC_CAT_URL, "collections/wern_features"), json=collections.DEFAULT_WERN_METADATA)

    return openeo.connect(OPENEO_API_URL)

@pytest.fixture
def dummy_backend(requests_mock, oeo_con100) -> DummyBackend:
    """
    Fixture to create a dummy backend for testing.

    This backend is used to test the job manager with a dummy backend
    using the DummyBackend defined in openeo for testing purposes.

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client
    """
    dummy_backend = DummyBackend(requests_mock=requests_mock, connection=oeo_con100)

    # Setup Mock Collection
    dummy_backend.setup_collection("SENTINEL1_GRD", bands=collections.DEFAULT_S1_METADATA["cube:dimensions"]["bands"]["values"])
    dummy_backend.setup_collection("SENTINEL2_L2A", bands=collections.DEFAULT_S2_METADATA["cube:dimensions"]["bands"]["values"])
    dummy_backend.setup_collection("COPERNICUS_30", bands=collections.DEFAULT_DEM_METADATA["cube:dimensions"]["bands"]["values"])

    # Mock STAC
    # TODO use Dummy load_stac after created by the core Team
    requests_mock.get(url_join(STAC_CAT_URL, "collections/wern_features"), json=collections.DEFAULT_WERN_METADATA)

    dummy_backend.setup_file_format("GTiff")
    # dummy_backend.setup_file_format("netCDF")
    return dummy_backend



def load_json_from_path(filepath: str):
    with open(filepath, "r") as json_file:
        return json.load(json_file)

def compare_job_info(job_info: dict, filename: str, as_benchmark_scenario: bool=False):
    """
    Compare the job info with the saved job info.


    If benchmark_scenario is False, only the process graph is compared.
    If benchmark_scenario is True, the whole job info (PG, job_options, backend, description,...) is compared.

    Inspired by testing in https://github.com/VITO-RS-Vegetation/lcfm-production.

    """
    # Construct paths for expected files
    if not filename.__str__().startswith(GROUNDTRUTH_DIR):
        groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)
    else:
        groundtruth_filepath = filename

    # Get generayed and expected process graphs
    pg = job_info.get("process_graph")
    groundtruth = load_json_from_path(groundtruth_filepath)
    reference_data = groundtruth.get("reference_data", {})

    if as_benchmark_scenario:
        result = {
            "id": "WEED_" + Path(filename).stem,
            "type": "openeo",
            "description": f"Integration test from the WEED eo-processing {Path(filename).stem}",
            "backend": "openeo.dataspace.copernicus.eu",
            "process_graph": pg,
            "job_options": INTEGRATION_JOB_OPTIONS, 
            "reference_data": reference_data,
        }
    else:
        result = {
            "process_graph": pg,
        }

    # Compare the saved process graph with the one created by the job manager
    assert (
        result == groundtruth
    ), "Process graph does not match the saved process graph. Run pytest with `-vv` to see the differences."


