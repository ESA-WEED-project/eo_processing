#%%
import os
import json
import openeo
import sys

sys.path.append(r'C:\Git_projects\eo_processing\src')
from eo_processing.openeo.preprocessing import ts_datacube_extraction, extract_S1_datacube, extract_S2_datacube, S2_BANDS

# Establish OpenEO connection (adjust if needed for your environment)
connection = openeo.connect("openeo.dataspace.copernicus.eu").authenticate_oidc()

# Define the spatial and temporal extent for the tests
spatial_extent = {
    'east': 4880000,
    'south': 2898000,
    'west': 4878000,
    'north': 2900000,
    'crs': 'EPSG:3035'
}
temporal_extent = ["2021-01-01", "2022-01-01"]

# Directory to save the process graphs
output_directory = "resources/unit-tests"
os.makedirs(output_directory, exist_ok=True)

#%%

def generate_process_graphs(function, scenarios, connection, bbox, start, end, output_dir):
    for filename, processing_options in scenarios:
        try:
            # Create the data cube with the specified processing options
            datacube = function(
                connection=connection,
                bbox=bbox,
                start=start,
                end=end,
                **processing_options
            )
            
            # Convert the process graph to JSON format
            process_graph_json = datacube.to_json()

            # Save the process graph JSON to a file
            output_path = os.path.join(output_dir, filename)
            with open(output_path, "w") as json_file:
                json_file.write(process_graph_json)
                print(f"Process graph saved: {output_path}")

        except ValueError as e:
            # If an invalid option was provided, log the error (this is expected for certain tests)
            print(f"Error generating process graph for {filename}: {e}")

#%%

# Define the different test scenarios and processing options
ts_test_scenarios = [
    
    # Sentinel-2 Only, Basic
    ("ts_datacube_extraction_S1.json", {"S1_collection": None}),
    
    # Combined Sentinel-1 and Sentinel-2
    ("ts_datacube_extraction_combined.json", {}),
    
    # Sentinel-2 with Cloud Masking
    ("ts_datacube_extraction_S2_with_masking.json", {"SLC_masking_algo": "mask_scl_dilation"}),
    
    # Temporal Aggregation and Interpolation
    ("ts_datacube_extraction_S1_interpolation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M",
        "time_interpolation": True
    }),
    
    # Combined with Custom CRS and Resolution
    ("ts_datacube_extraction_combined_custom_crs.json", {
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    })
]

# Define test scenarios for `extract_S1_datacube`
s1_test_scenarios = [
    # Basic Sentinel-1 Data Extraction
    ("extract_S1_basic.json", {"S1_collection": "SENTINEL1_GRD"}),
    
    # Sentinel-1 with Temporal Aggregation
    ("extract_S1_temporal_aggregation.json", {
        "S1_collection": "SENTINEL1_GRD",
        "ts_interval": "P1M"
    }),
    
    # Sentinel-1 with Resampling and Custom CRS
    ("extract_S1_custom_crs.json", {
        "S1_collection": "SENTINEL1_GRD",
        "target_crs": "EPSG:3857",
        "resolution": 30.0
    })
]

# Define test scenarios for `extract_S2_datacube`
s2_test_scenarios = [
    # Basic Sentinel-2 Data Extraction
    ("extract_S2_basic.json", {"S2_collection": "SENTINEL2_L2A"}),
    
    # Sentinel-2 with Cloud Masking
    ("extract_S2_with_masking.json", {
        "S2_collection": "SENTINEL2_L2A",
        "SLC_masking_algo": "mask_scl_dilation"
    }),
    
    # Sentinel-2 with Temporal Aggregation and Resampling
    ("extract_S2_temporal_resampling.json", {
        "S2_collection": "SENTINEL2_L2A",
        "ts_interval": "P1M",
        "target_crs": "EPSG:3857",
        "resolution": 20.0
    })
]

#%%

# Generate process graphs for `ts_datacube_extraction`
generate_process_graphs(
    function=ts_datacube_extraction,
    scenarios=ts_test_scenarios,
    connection=connection,
    bbox=spatial_extent,
    start=temporal_extent[0],
    end=temporal_extent[1],
    output_dir=output_directory
)

# Generate process graphs for `extract_S1_datacube`
generate_process_graphs(
    function=extract_S1_datacube,
    scenarios=s1_test_scenarios,
    connection=connection,
    bbox=spatial_extent,
    start=temporal_extent[0],
    end=temporal_extent[1],
    output_dir=output_directory
)

# Generate process graphs for `extract_S2_datacube`
generate_process_graphs(
    function=extract_S2_datacube,
    scenarios=s2_test_scenarios,
    connection=connection,
    bbox=spatial_extent,
    start=temporal_extent[0],
    end=temporal_extent[1],
    output_dir=output_directory
)

print("All process graphs have been generated successfully!")
# %%
