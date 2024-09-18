#%%
import os
import json
import onnx
from catboost import CatBoostClassifier

# Function to load the CSV file and extract metadata
# Function to load metadata from JSON file
def load_metadata_from_json(json_path: str):
    """Load the JSON file and extract metadata."""
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found at: {json_path}")
    
    try:
        with open(json_path, 'r') as file:
            metadata = json.load(file)
            if not isinstance(metadata, list) or not metadata:
                raise ValueError("JSON file does not contain a valid list of features.")
            return metadata
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON file: {e}")
    except Exception as e:
        raise ValueError(f"Failed to read JSON file: {e}")
    
# Function to load the CatBoost model
def load_catboost_model(catboost_model_path: str):
    """Load the CatBoost model from a saved .cbm file."""
    if not os.path.exists(catboost_model_path):
        raise FileNotFoundError(f"CatBoost model file not found at: {catboost_model_path}")
    
    model = CatBoostClassifier()
    try:
        model.load_model(catboost_model_path, format="cbm")
        return model
    except Exception as e:
        raise ValueError(f"Failed to load CatBoost model: {e}")

# Function to save the model as ONNX
def save_model_to_onnx(model, output_onnx_path: str):
    """Save the CatBoost model to ONNX format."""
    try:
        model.save_model(output_onnx_path, format="onnx")
    except Exception as e:
        raise ValueError(f"Failed to save model to ONNX format: {e}")

# Function to add metadata to the ONNX model
def add_metadata_to_onnx(onnx_path: str, metadata: list = None):
    """Load the ONNX model and add metadata (column headers) to the doc_string."""
    try:
        onnx_model = onnx.load(onnx_path)
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model: {e}")
    
    # Add metadata to the doc_string
    if metadata:
        features_string = ', '.join(metadata)  # Convert list of headers to a string
        # Add new metadata
        onnx_model.metadata_props.append(onnx.StringStringEntryProto(key='trained_features', value=features_string))

    try:
        onnx.save(onnx_model, onnx_path)
        print(f"ONNX model saved with metadata in: {onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to save ONNX model with metadata: {e}")
    
def onnx_output_path(catboost_model_path: str) -> str:
    # Get the directory and the file name without extension
    directory, file_name = os.path.split(catboost_model_path)
    # Replace the file extension from .cbm to .onnx
    new_file_name = file_name.replace('.cbm', '.onnx')
    # Join the directory and the new file name
    output_onnx_path = os.path.join(directory, new_file_name)
    return output_onnx_path

# Main conversion function that ties everything together
def convert_catboost_model_to_onnx_with_metadata(catboost_model_path: str, output_onnx_path: str = None, metadata_path: str = None):
    """Main function to convert CatBoost model to ONNX format with optional metadata from Excel."""
    
    # Step 1: Load metadata from Excel (if provided)
    metadata = None
    if metadata_path:
        metadata = load_metadata_from_json(metadata_path)
    
    # Step 2: Load the CatBoost model
    model = load_catboost_model(catboost_model_path)

    if output_onnx_path is None:
        output_onnx_path = onnx_output_path(catboost_model_path)
    
    # Step 3: Save the CatBoost model to ONNX format
    save_model_to_onnx(model, output_onnx_path)
    
    # Step 4: Add metadata to the ONNX model (if available)
    if metadata:
        add_metadata_to_onnx(output_onnx_path, metadata)
    else:
        print(f"Model saved without additional metadata in: {output_onnx_path}")

#%%
# Example usage
catboost_model_path = 'H:\\slovakia\\models\\v5\\L1\\models\\65predictors\\v1\\catboost_v1.cbm'
output_onnx_path = 'C:\\Git_projects\\test.onnx'
metadata_path = 'H:\\slovakia\\models\\v5\\L1\\models\\65predictors\\v1\\\\predictors.json'

convert_catboost_model_to_onnx_with_metadata(
     catboost_model_path= catboost_model_path,      # Path to your saved CatBoost model (.cbm)
     output_onnx_path = output_onnx_path,
     metadata_path=metadata_path            # Path to the Excel file containing column headers (optional)
)

onnx_model = onnx.load(output_onnx_path)

metadata = {}
for meta in onnx_model.metadata_props:
    metadata[meta.key] = meta.value

print(metadata["trained_features"])


#%%
