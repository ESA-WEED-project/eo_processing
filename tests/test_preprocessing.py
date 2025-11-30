import json
import pytest
from unittest.mock import patch

from eo_processing.openeo.preprocessing import ts_datacube_extraction, extract_S1_datacube, extract_S2_datacube
from tests.conftest import BBOX, DATE_START, DATE_END, \
    oeo_con100, compare_job_info

# Define the different test scenarios and processing options
ts_test_scenarios = [
    #("cube_extraction/ts_datacube_extraction_S1.json", {"S1_collection": "SENTINEL1_GRD"}, False),
    #("cube_extraction/ts_datacube_extraction_S2.json", {"S2_collection": "SENTINEL2_L2A"}, False),
    ("cube_extraction/ts_datacube_extraction_combined.json", {}, False),
    ("cube_extraction/ts_datacube_extraction_S1_integration.json", {"S1_collection": "SENTINEL1_GRD"}, True),
    ("cube_extraction/ts_datacube_extraction_S2_integration.json", {"S2_collection": "SENTINEL2_L2A"}, True),
    ("cube_extraction/ts_datacube_extraction_combined_integration.json", {}, True),
    ("cube_extraction/ts_datacube_extraction_S2_with_masking.json", {"SLC_masking_algo": "mask_scl_dilation"},
     False),
    ("cube_extraction/ts_datacube_extraction_S1_interpolation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M",
        "time_interpolation": True
    },
    False),
    ("cube_extraction/ts_datacube_extraction_combined_custom_crs.json", {
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    },
    False)
]


# Define test scenarios for `extract_S1_datacube`
s1_test_scenarios = [
    ("cube_extraction/extract_S1_basic.json", {"S1_collection": "SENTINEL1_GRD"}, False),
    ("cube_extraction/extract_S1_integration.json", {"S1_collection": "SENTINEL1_GRD"}, True),
    ("cube_extraction/extract_S1_temporal_aggregation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M"
    },
    False),
    ("cube_extraction/extract_S1_custom_crs.json", {
        "S1_collection": "SENTINEL1_GRD",
        "target_crs": "EPSG:3857",
        "resolution": 30.0
    },
    False)
]


# Define test scenarios for `extract_S2_datacube`
s2_test_scenarios = [
    ("cube_extraction/extract_S2_basic.json", {"S2_collection": "SENTINEL2_L2A"}, False),
    ("cube_extraction/extract_S2_integration.json", {"S2_collection": "SENTINEL2_L2A"}, True),
    ("cube_extraction/extract_S2_with_masking.json", {
        "S2_collection": "SENTINEL2_L2A",
        "SLC_masking_algo": "mask_scl_dilation"
    },
    False),
    ("cube_extraction/extract_S2_temporal_resampling.json", {
        "S2_collection": "SENTINEL2_L2A",
        "ts_interval": "P1M",
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    },
    False)
]


@pytest.mark.parametrize("groundtruth_filename, params, integration", ts_test_scenarios)
def test_ts_datacube_extraction(oeo_con100, groundtruth_filename, params, integration):
    """
    Test `ts_datacube_extraction` function for different scenarios.
    """
    # Pass the mock connection explicitly
    data_cube = oeo_con100.load_collection(
        collection_id="SENTINEL2_L2A" if "S2" in groundtruth_filename else "SENTINEL1_GRD",
    )

    # Use the mock for ts_datacube_extraction
    with patch('eo_processing.openeo.preprocessing.ts_datacube_extraction', return_value=data_cube):
        extracted_cube = ts_datacube_extraction(oeo_con100, bbox=BBOX, start=DATE_START, end=DATE_END, **params)

    generated_process_graph = extracted_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)


@pytest.mark.parametrize("groundtruth_filename, params, integration", s1_test_scenarios)
def test_extract_S1_datacube(oeo_con100, groundtruth_filename, params, integration):
    """
    Test the `extract_S1_datacube` function for various Sentinel-1 scenarios.
    """
    # Load the mock data cube
    data_cube = oeo_con100.load_collection(
        collection_id="SENTINEL1_GRD",
    )

    # Use the mock for ts_datacube_extraction
    with patch('eo_processing.openeo.preprocessing.extract_S1_datacube', return_value=data_cube):
        extracted_cube = extract_S1_datacube(oeo_con100, bbox=BBOX, start=DATE_START, end=DATE_END, **params)
    
    generated_process_graph = extracted_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)


@pytest.mark.parametrize("groundtruth_filename, params, integration", s2_test_scenarios)
def test_extract_S2_datacube(oeo_con100, groundtruth_filename, params, integration):
    """
    Test the `extract_S2_datacube` function for various Sentinel-2 scenarios.
    """
    # Load the mock data cube
    data_cube = oeo_con100.load_collection(
        collection_id="SENTINEL2_L2A"
    )

    # Use the mock for extract_S2_datacube
    with patch('eo_processing.openeo.preprocessing.extract_S2_datacube', return_value=data_cube):
        extracted_cube = extract_S2_datacube(oeo_con100, bbox=BBOX, start=DATE_START, end=DATE_END, **params)

    generated_process_graph = extracted_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)