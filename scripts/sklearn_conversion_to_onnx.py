"""
test script to convert catboost model to onnx as reference for the UNIX tests

"""
# Add the parent directory to sys.path
import os
from joblib import load
from eo_processing.utils.onnx_model_utilities import convert_model_to_onnx_with_metadata

# Define paths and features
sklearn_model_path = r'C:\\tmp\\model\\dim_reduction_pca.pkl'
output_onnx_path = r'C:\\Users\\wannijnj\\Documents\\Projects\\WEED\\eo_processing\\src\\eo_processing\\resources\\models\\dim_reduction_pca.onnx'

input_features = ["B01_mean", "B01_median", "B01_min", "B01_max", "B01_q05", "B01_q25", "B01_q75", "B01_q95",
    "B02_mean", "B02_median", "B02_min", "B02_max", "B02_q05", "B02_q25", "B02_q75", "B02_q95",
    "B03_mean", "B03_median", "B03_min", "B03_max", "B03_q05", "B03_q25", "B03_q75", "B03_q95",
    "B04_mean", "B04_median", "B04_min", "B04_max", "B04_q05", "B04_q25", "B04_q75", "B04_q95",
    "B05_mean", "B05_median", "B05_min", "B05_max", "B05_q05", "B05_q25", "B05_q75", "B05_q95",
    "B06_mean", "B06_median", "B06_min", "B06_max", "B06_q05", "B06_q25", "B06_q75", "B06_q95",
    "B07_mean", "B07_median", "B07_min", "B07_max", "B07_q05", "B07_q25", "B07_q75", "B07_q95",
    "B08_mean", "B08_median", "B08_min", "B08_max", "B08_q05", "B08_q25", "B08_q75", "B08_q95",
    "B8A_mean", "B8A_median", "B8A_min", "B8A_max", "B8A_q05", "B8A_q25", "B8A_q75", "B8A_q95",
    "B09_mean", "B09_median", "B09_min", "B09_max", "B09_q05", "B09_q25", "B09_q75", "B09_q95",
    "B11_mean", "B11_median", "B11_min", "B11_max", "B11_q05", "B11_q25", "B11_q75", "B11_q95",
    "B12_mean", "B12_median", "B12_min", "B12_max", "B12_q05", "B12_q25", "B12_q75", "B12_q95",
    "NDVI_mean", "NDVI_median", "NDVI_min", "NDVI_max", "NDVI_q05", "NDVI_q25", "NDVI_q75", "NDVI_q95"
]

output_features = ['COMP1', 'COMP2', 'COMP3']

metadata_keys = [
    "n_components_", "components_", "explained_variance_",
    "explained_variance_ratio_", "mean_", "n_features_", "n_samples_"
]

# Get `model` metadata from your trained PCA model
add_metadata_dict = {}
model = load(sklearn_model_path)
for key in metadata_keys:
    if hasattr(model, key):
        value = getattr(model, key)
        # Convert numpy arrays or other complex types to lists
        if hasattr(value, "tolist"):
            value = value.tolist()
        add_metadata_dict[key] = value


convert_model_to_onnx_with_metadata(sklearn_model_path, 
                                    input_features, 
                                    output_features, 
                                    output_onnx_path,
                                    target_opset=9,
                                    add_metadata=add_metadata_dict)