import json
import os
from pathlib import Path

import openeo
import pytest
from openeo.rest._testing import build_capabilities
from tests.test_process_graphs_integration import INTEGRATION_JOB_OPTIONS

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


API_URL = "https://oeo.test/"
GROUNDTRUTH_DIR = "tests//resources"
BBOX = {"east": 4880000, "south": 2896000, "west": 4876000, "north": 2900000, 'crs': 'EPSG:3035'} # 4x4 km bbox in Germany
DATE_START = "2021-01-01"
DATE_END = "2022-01-01"

DEFAULT_S1_METADATA = {
    "id": "SENTINEL1_GRD",
    "cube:dimensions":{
        "x": {"type": "spatial"}, 
        "y": {"type": "spatial"}, 
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["VV", "VH"]}
        },
    "summaries": {
        "platform": ["sentinel-1"],
        "eo:bands": [
        {"name": "VV", "common_name": "VV", "center_wavelength": 0.055},
        {"name": "VH", "common_name": "VH", "center_wavelength": 0.06}]
        }
}

DEFAULT_S2_METADATA = {
    "id": "SENTINEL2_L2A",
    "cube:dimensions": {
        "x": {"type": "spatial"},
        "y": {"type": "spatial"},
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12", "SCL"]}
        },
    "summaries": {
        "platform": ["sentinel-2a", "sentinel-2b"],
        "eo:bands": [
        {"name": "B01", "common_name": None, "center_wavelength": 0.443},
        {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
        {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
        {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
        {"name": "B05", "common_name": None, "center_wavelength": 0.7039},
        {"name": "B06", "common_name": None, "center_wavelength": 0.7402},
        {"name": "B07", "common_name": None, "center_wavelength": 0.7825},
        {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351},
        {"name": "B8A", "common_name": None, "center_wavelength": 0.8648},
        {"name": "B09", "common_name": None, "center_wavelength": 0.945},
        {"name": "B11", "common_name": "swir16", "center_wavelength": 1.610},
        {"name": "B12", "common_name": "swir22", "center_wavelength": 2.190},
        {"name": "SCL", "common_name": "SCL", "center_wavelength": None}]
    }
}

@pytest.fixture
def api_capabilities() -> dict:
    """
    Fixture to be overridden for customizing the capabilities doc used by connection fixtures.
    To be used as kwargs for `build_capabilities`

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client
    """
    return {}

@pytest.fixture
def con100(requests_mock, api_capabilities):
    """
    Fixture to create a connection to the dummy backend using openeo 1.0.0.

    This is used for testing the job manager with a dummy backend.

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client.
    """
    requests_mock.get(
        API_URL, json=build_capabilities(api_version="1.0.0", **api_capabilities)
    )
    requests_mock.get(API_URL+ "collections/SENTINEL1_GRD", json=DEFAULT_S1_METADATA)
    requests_mock.get(API_URL+ "collections/SENTINEL2_L2A", json=DEFAULT_S2_METADATA)
    return openeo.connect(API_URL)

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
    # Get process graph from job info
    pg = job_info.get("process_graph")


    if as_benchmark_scenario:
        result = {
            "id": Path().stem,
            "type": "openeo",
            "description": f"Integration test from the WEED eo-processing {os.path.splitext(filename)[0]}",
            "backend": "openeo.dataspace.copernicus.eu",
            "process_graph": pg,
            "job_options": INTEGRATION_JOB_OPTIONS, 
            "reference_data": {},
        }
    else:
        result = pg

    # Construct paths for generated and expected files
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)

    # Compare the saved process graph with the one created by the job manager
    assert (
        result == load_json_from_path(groundtruth_filepath).get("process_graph")
    ), "Process graph does not match the saved process graph. Run pytest with `-vv` to see the differences."


