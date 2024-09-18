#%%

"""
since we currently do not have STAC collections for all potential inputs 
we will test if we can get the catboost model to run (functionally) on cubes with 
65 entires. Since 65 bands are used during the inference.
"""

#%% Dummy function for fake cube generation

def compute_quantiles(base_features, quantiles=[0.10, 0.25, 0.50, 0.75, 0.90]):
    """
    Computes specified quantiles (default P10, P25, P50, P75, P90) 
    for each band in the base_features along the time dimension.
    
    Args:
        base_features: A data structure (e.g., xarray or similar) that contains
                       time series data along a dimension.
        quantiles: A list of quantiles to compute (default: [0.10, 0.25, 0.50, 0.75, 0.90])
    
    Returns:
        A data structure with computed quantiles, renamed to reflect
        both the band and the quantile.
    """
    
    def compute_stats(timeseries, quantiles):
        return timeseries.quantiles(probabilities=quantiles)
    
    # Compute the quantiles for each band along the time dimension ('t')
    stats = base_features.apply_dimension(
        dimension="t", target_dimension="bands", process=lambda ts: compute_stats(ts, quantiles)
    )
    
    # Generate band names by appending the quantile labels (P10, P25, etc.) to each band
    quantile_labels = [f"P{int(q*100)}" for q in quantiles]
    all_bands = [
        f"{band}_{label}"
        for band in base_features.metadata.band_names
        for label in quantile_labels
    ]
    
    return stats.rename_labels("bands", all_bands)

#%%
import openeo

connection = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc()

#input parameters
spatial_extent={"west": 5.14, "south": 51.17, "east": 5.17, "north": 51.19}
temporal_extent=["2021-02-01", "2021-03-01"]
max_cloud_cover = 90

#set job dependencies (URL to zipped model)
DEPENDENCY_URL = "https://s3.waw3-1.cloudferro.com/swift/v1/project_dependencies/onnx_dependencies_1.16.3.zip"
MODEL_URL = "https://s3.waw3-1.cloudferro.com/swift/v1/project_dependencies/WEED_test_catboost.zip"

job_options = {}
job_options["udf-dependency-archives"] = [
            f"{DEPENDENCY_URL}#onnx_deps",
            f"{MODEL_URL}#onnx_models",
        ]
#get input cube
#TODO change by worldcover 'B04-p50-10m', 'B03-p50-10m', 'B02-p50-10m' bands
s2 = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=["B01", "B02", "B03", "B04", "B05", "B06","B07","B08", "B8A", "B09", "B11", "B12","SCL"],
        max_cloud_cover=max_cloud_cover)

s2_expanded = compute_quantiles(s2)

# Load the UDF from a file.
udf = openeo.UDF.from_file("udf_catboost_inference.py")

# Apply the UDF to the data cube.
catboost_classification = s2_expanded.apply_neighborhood(
    process=udf,
    size=[
        {"dimension": "x", "value": 100, "unit": "px"},
        {"dimension": "y", "value": 100, "unit": "px"},
    ],
    overlap=[
        {"dimension": "x", "value": 0, "unit": "px"},
        {"dimension": "y", "value": 0, "unit": "px"},
    ])

#run inference
output = catboost_classification.rename_labels(dimension="bands",target= [
 'predicted_label', 'prob_class_30000', 'prob_class_40000',
 'prob_class_50000', 'prob_class_60000', 'prob_class_70000',
 'prob_class_80000', 'prob_class_90000', 'prob_class_100000',
 'prob_class_110000'])

output.execute_batch("output.nc", job_options=job_options)


