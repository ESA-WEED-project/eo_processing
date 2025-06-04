import json
import pytest
from eo_processing.openeo.processing import generate_master_feature_cube, \
    generate_S1_feature_cube, generate_S2_feature_cube
from tests.tests_unit.conftest import BBOX, DATE_START, DATE_END, \
    con100, compare_job_info


@pytest.mark.parametrize(
    "groundtruth_filename",[("generate_master_feature_cube.json")],
)
def test_generate_master_feature_cube(con100, groundtruth_filename):
    """
    Test generate_master_feature_cube function with an annual data cube
    """
    # Generate job info
    generated_cube=generate_master_feature_cube(
        connection=con100,
        S1_collection='SENTINEL1_GRD',
        S2_collection='SENTINEL2_L2A',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)

    generated_process_graph=generated_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename)


@pytest.mark.parametrize(
    "groundtruth_filename",[("generate_S1_feature_cube.json")],
)
def test_generate_S1_feature_cube(con100, groundtruth_filename):
    """
    Test generate_S1_feature_cube function with an annual data cube
    """
    # Generate job info
    generated_cube=generate_S1_feature_cube(
        connection=con100,
        S1_collection='SENTINEL1_GRD',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)

    generated_process_graph=generated_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename)

@pytest.mark.parametrize(
    "groundtruth_filename",[("generate_S2_feature_cube.json")],
)
def test_generate_S2_feature_cube(con100, groundtruth_filename):
    """
    Test generate_S2_feature_cube function with an annual data cube
    """
    # Generate job info
    generated_cube=generate_S2_feature_cube(
        connection=con100,
        S2_collection='SENTINEL2_L2A',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)

    generated_process_graph=generated_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename)