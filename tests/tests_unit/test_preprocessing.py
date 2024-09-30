import pytest
import json
import os
from unittest import mock
import openeo

from eo_processing.openeo.preprocessing import ts_datacube_extraction, extract_S1_datacube, extract_S2_datacube, S2_BANDS

# Define directories for generated and expected process graphs
GROUNDTRUTH_DIR = "resources"

BBOX = {
    'east': 5.5,
    'south': 49.5,
    'west': 4.5,
    'north': 50.5,
    'crs': 'EPSG:4326'
}

DATE_START = "2021-01-01"
DATE_END = "2021-12-31"
CONNECTION = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc()

# Helper function to load a JSON file
def load_json_from_path(filepath):
    with open(filepath, "r") as json_file:
        return json.load(json_file)


# Define the different test scenarios and processing options
ts_test_scenarios = [
    ("ts_datacube_extraction_S1.json", {"S1_collection": None}),
    ("ts_datacube_extraction_combined.json", {}),
    ("ts_datacube_extraction_S2_with_masking.json", {"SLC_masking_algo": "mask_scl_dilation"}),
    ("ts_datacube_extraction_S1_interpolation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M",
        "time_interpolation": True
    }),
    ("ts_datacube_extraction_combined_custom_crs.json", {
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    })
]

# Define test scenarios for `extract_S1_datacube`
s1_test_scenarios = [
    ("extract_S1_basic.json", {"S1_collection": "SENTINEL1_GRD"}),
    ("extract_S1_temporal_aggregation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M"
    }),
    ("extract_S1_custom_crs.json", {
        "S1_collection": "SENTINEL1_GRD",
        "target_crs": "EPSG:3857",
        "resolution": 30.0
    })
]

# Define test scenarios for `extract_S2_datacube`
s2_test_scenarios = [
    ("extract_S2_basic.json", {"S2_collection": "SENTINEL2_L2A"}),
    ("extract_S2_with_masking.json", {
        "S2_collection": "SENTINEL2_L2A",
        "SLC_masking_algo": "mask_scl_dilation"
    }),
    ("extract_S2_temporal_resampling.json", {
        "S2_collection": "SENTINEL2_L2A",
        "ts_interval": "P1M",
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    })
]

# Parametrize the test cases for `ts_datacube_extraction` scenarios
@pytest.mark.parametrize("filename, params", ts_test_scenarios)
def test_ts_datacube_extraction(filename, params):
    """
    Test the `ts_datacube_extraction` function for various scenarios.
    """
    # Assuming `ts_datacube_extraction` is defined somewhere and returns a datacube object with `to_json()` method
    # Use mockup code or import actual function here

    # Mock up a connection object for testing
    connection = CONNECTION  # Define this function to return a mock connection object

    # Create the data cube and generate the process graph
    data_cube = ts_datacube_extraction(connection,
                                       bbox=BBOX,
                                       start=DATE_START,
                                       end=DATE_END,
                                       **params)
    
    generated_process_graph = data_cube.to_json()

    # Construct paths for generated and expected files
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)

    # Load and normalize the expected process graph
    groundtruth_process_graph = load_json_from_path(groundtruth_filepath)

    # Convert both process graphs to JSON strings for comparison
    assert json.dumps(json.loads(generated_process_graph), sort_keys=True, indent=4) == json.dumps(groundtruth_process_graph, sort_keys=True, indent=4)

# Parametrize the test cases for `extract_S1_datacube` scenarios
@pytest.mark.parametrize("filename, params", s1_test_scenarios)
def test_extract_S1_datacube(filename, params):
    """
    Test the `extract_S1_datacube` function for various Sentinel-1 scenarios.
    """

    # Mock up a connection object for testing
    connection = CONNECTION

    # Create the data cube and generate the process graph
    data_cube = extract_S1_datacube(connection,
                                    bbox=BBOX,
                                    start=DATE_START,
                                    end=DATE_END,
                                    **params)
    
    generated_process_graph = data_cube.to_json()

    # Construct paths for generated and expected files
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)

    # Load and normalize the expected process graph
    groundtruth_process_graph = load_json_from_path(groundtruth_filepath)

    # Convert both process graphs to JSON strings for comparison
    assert json.dumps(json.loads(generated_process_graph), sort_keys=True, indent=4) == json.dumps(groundtruth_process_graph, sort_keys=True, indent=4)


# Parametrize the test cases for `extract_S2_datacube` scenarios
@pytest.mark.parametrize("filename, params", s2_test_scenarios)
def test_extract_S2_datacube(filename, params):
    """
    Test the `extract_S2_datacube` function for various Sentinel-2 scenarios.
    """

    # Mock up a connection object for testing
    connection = CONNECTION

    # Create the data cube and generate the process graph
    data_cube = extract_S2_datacube(connection,
                                    bbox=BBOX,
                                    start=DATE_START,
                                    end=DATE_END,
                                    **params)
    
    generated_process_graph = data_cube.to_json()

    # Construct paths for generated and expected files
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)

    # Load and normalize the expected process graph
    groundtruth_process_graph = load_json_from_path(groundtruth_filepath)

    # Convert both process graphs to JSON strings for comparison
    assert json.dumps(json.loads(generated_process_graph), sort_keys=True, indent=4) == json.dumps(groundtruth_process_graph, sort_keys=True, indent=4)
