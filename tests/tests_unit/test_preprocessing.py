import json
import os
import pytest
import openeo
from unittest.mock import patch
from openeo.rest.datacube import DataCube

from eo_processing.openeo.preprocessing import ts_datacube_extraction, extract_S1_datacube, extract_S2_datacube

# Constants for the test setup
BBOX = {'east': 5.5, 'south': 49.5, 'west': 4.5, 'north': 50.5, 'crs': 'EPSG:4326'}
DATE_START = "2021-01-01"
DATE_END = "2021-12-31"
API_URL = "https://oeo.test"
GROUNDTRUTH_DIR = "tests/resources"  # Fixed typo here


DEFAULT_S1_METADATA = {
    "cube:dimensions": {"x": {"type": "spatial"}, "y": {"type": "spatial"}, "t": {"type": "temporal"},
                        "bands": {"type": "bands", "values": ["VV", "VH"]}},
    "summaries": {"eo:bands": [{"name": "VV", "common_name": "VV", "center_wavelength": 0.055},
                               {"name": "VH", "common_name": "VH", "center_wavelength": 0.06}]}
}

DEFAULT_S2_METADATA = {
    "cube:dimensions": {"x": {"type": "spatial"}, "y": {"type": "spatial"}, "t": {"type": "temporal"},
                        "bands": {"type": "bands", "values": [
                            "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12", "SCL"
                        ]}},
    "summaries": {"eo:bands": [
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
        {"name": "SCL", "common_name": "SCL", "center_wavelength": None} 
    ]}
}


def dict_from_json(filepath):
    with open(filepath, "r") as json_file:
        return json.load(json_file)


# Mock connection setup
@pytest.fixture
def connection(requests_mock):
    """
    Mock connection to the OpenEO backend with sample metadata for Sentinel-1 and Sentinel-2.
    """
    requests_mock.get(API_URL + "/", json={"api_version": "1.0.0"})
    requests_mock.get(API_URL + "/collections/SENTINEL1_GRD", json=DEFAULT_S1_METADATA)
    requests_mock.get(API_URL + "/collections/SENTINEL2_L2A", json=DEFAULT_S2_METADATA)
    return openeo.connect(API_URL)


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


@pytest.mark.parametrize(["filename", "params"], ts_test_scenarios)
def test_ts_datacube_extraction(connection, filename, params):
    """
    Test `ts_datacube_extraction` function for different scenarios.
    """
    data_cube = connection.load_collection(
        collection_id="SENTINEL2_L2A" if "S2" in filename else "SENTINEL1_GRD"
    )

    with patch('eo_processing.openeo.preprocessing.ts_datacube_extraction', return_value=data_cube):
        extracted_cube = ts_datacube_extraction(connection, bbox=BBOX, start=DATE_START, end=DATE_END, **params)

        # Verify generated process graph against ground truth
        generated_process_graph = extracted_cube.to_dict()
        groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)
        groundtruth_process_graph = dict_from_json(groundtruth_filepath)

        # Compare dictionaries directly
        assert generated_process_graph == groundtruth_process_graph


@pytest.mark.parametrize(["filename", "params"], s1_test_scenarios)
def test_extract_S1_datacube(connection, filename, params):
    """
    Test the `extract_S1_datacube` function for various Sentinel-1 scenarios.
    """
    data_cube = connection.load_collection("SENTINEL1_GRD")

    with patch('eo_processing.openeo.preprocessing.extract_S1_datacube', return_value=data_cube):
        extracted_cube = extract_S1_datacube(connection, bbox=BBOX, start=DATE_START, end=DATE_END, **params)
    
    generated_process_graph = extracted_cube.to_dict()
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)
    groundtruth_process_graph = dict_from_json(groundtruth_filepath)

    assert generated_process_graph == groundtruth_process_graph


@pytest.mark.parametrize(["filename", "params"], s2_test_scenarios)
def test_extract_S2_datacube(connection, filename, params):
    """
    Test the `extract_S2_datacube` function for various Sentinel-2 scenarios.
    """
    data_cube = connection.load_collection("SENTINEL2_L2A")

    with patch('eo_processing.openeo.preprocessing.extract_S2_datacube', return_value=data_cube):
        extracted_cube = extract_S2_datacube(connection, bbox=BBOX, start=DATE_START, end=DATE_END, **params)
    
    generated_process_graph = extracted_cube.to_dict()
    groundtruth_filepath = os.path.join(GROUNDTRUTH_DIR, filename)
    groundtruth_process_graph = dict_from_json(groundtruth_filepath)

    assert generated_process_graph == groundtruth_process_graph