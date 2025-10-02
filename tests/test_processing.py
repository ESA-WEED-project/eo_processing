import json
import pytest
from pathlib import Path
import os
import openeo
from eo_processing.utils.helper import getUDFpath
from eo_processing.openeo.processing import generate_master_feature_cube, \
    generate_S1_feature_cube, generate_S2_feature_cube

from tests.conftest import BBOX, DATE_START, DATE_END, TARGET_CRS, TARGET_RESOLUTION, STAC_CAT_URL, \
    oeo_con100, compare_job_info
import tests.config_collections as collections

basedir = Path(os.path.dirname(os.path.dirname(__file__)))

@pytest.mark.parametrize(
    "groundtruth_filename, integration",
    [
        ("cube_generation/generate_S1_feature_cube.json", False),
        ("cube_generation/generate_S1_feature_cube_integration.json", True)
    ],
)
def test_generate_S1_feature_cube(oeo_con100, groundtruth_filename, integration):
    """
    Test generate_S1_feature_cube function with an annual data cube
    """
    # Generate job info
    generated_cube=generate_S1_feature_cube(
        connection=oeo_con100,
        S1_collection='SENTINEL1_GRD',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)

    generated_process_graph=generated_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)


@pytest.mark.parametrize(
    "groundtruth_filename, integration",
    [
        ("cube_generation/generate_S2_feature_cube.json", False),
        ("cube_generation/generate_S2_feature_cube_integration.json", True)
    ],
)
def test_generate_S2_feature_cube(oeo_con100, groundtruth_filename, integration):
    """
    Test generate_S2_feature_cube function with an annual data cube
    """
    # Generate job info
    generated_cube=generate_S2_feature_cube(
        connection=oeo_con100,
        S2_collection='SENTINEL2_L2A',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)

    generated_process_graph=generated_cube.to_json()

    # Assert that the job info is generated
    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)


@pytest.mark.parametrize(
    "groundtruth_filename, model_id, WERN_url, integration",
    [
        ("cube_generation/generate_master_feature_cube_with_catboost_inference.json", 
         "EUNIS2021plus_EU_v1_2024_PAN", 
         STAC_CAT_URL+ '/collections/wern_features',
         False),
        ("cube_generation/generate_master_feature_cube_with_catboost_inference_integration.json",
         "EUNIS2021plus_EU_v1_2024_PAN", 
         'https://catalogue.weed.apex.esa.int/collections/wenr_features',
         True)
    ],
)
def test_master_cube_with_udf_catboost_inference(oeo_con100, groundtruth_filename, model_id, WERN_url, integration):
    """
    Test generate_master_feature_cube function with an annual data cube with the catboost inference applied
    """
    # Create Master feature Cube with WENR & DEM STACs merged
    data_cube=generate_master_feature_cube(
        connection=oeo_con100,
        S1_collection='SENTINEL1_GRD',
        S2_collection='SENTINEL2_L2A',
        bbox=BBOX,
        start=DATE_START,
        end=DATE_END)
    
    # load the DEM from a CDSE collection
    DEM = oeo_con100.load_collection("COPERNICUS_30", bands=["DEM"])
    # reduce the temporal domain since copernicus_30 collection is "special" and feature only are one time stamp
    DEM = DEM.reduce_dimension(dimension='t', reducer=lambda x: x.last(ignore_nodata=True))
    # resample the cube to 10m and EPSG of corresponding 20x20km grid tile
    DEM = DEM.resample_spatial(projection=TARGET_CRS,
                                    resolution=TARGET_RESOLUTION,
                                    method="near").filter_bbox(BBOX)
    
    # merge into the S1/S2 data cube
    data_cube = data_cube.merge_cubes(DEM)

    # load the WERN features from public STAC
    WENR = oeo_con100.load_stac(WERN_url)
    # resample the cube to 10m and EPSG of corresponding 20x20km grid tile
    WENR = WENR.resample_spatial(projection=TARGET_CRS,
                                    resolution=TARGET_RESOLUTION,
                                    method="near").filter_bbox(BBOX)
    # drop the time dimension
    try:
        WENR = WENR.drop_dimension('t')
    except:
        # workaround if we still have the client issues with the time dimensions for STAC dataset with only one time stamp
        WENR.metadata = WENR.metadata.add_dimension("t", label=None, type="temporal")
        WENR = WENR.drop_dimension('t')
        
    # merge into the S1/S2 data cube
    data_cube = data_cube.merge_cubes(WENR)

    # Apply the UDF to the data cube.
    udf  = openeo.UDF.from_file(
            getUDFpath('udf_catboost_inference.py'),
            runtime = "python",
            version="3.11",
            context={"model_id": model_id})

    generated_cube = data_cube.apply_dimension(process=udf, dimension="bands")

    generated_cube = generated_cube.linear_scale_range(0,100, 0,100)

    # Assert that the job info is generated
    generated_process_graph=generated_cube.to_json()

    assert generated_process_graph is not None, "Post data is None"
    
    # Compare generated job info with the ground truth process graph
    compare_job_info(json.loads(generated_process_graph), groundtruth_filename, as_benchmark_scenario=integration)
