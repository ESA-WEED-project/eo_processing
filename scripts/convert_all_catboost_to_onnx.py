#%%
import sys
import os
import json
import onnx

sys.path.append(os.path.abspath('C:/Git_projects/eo_processing/src'))
from eo_processing.utils.onnx_model_utilities import convert_catboost_model_to_onnx_with_metadata, extract_features_from_onnx

def load_input_features(json_path: str):
    """Load input features from a JSON file."""
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON file not found at: {json_path}")
    
    with open(json_path, 'r') as f:
        input_features = json.load(f)
    
    if not isinstance(input_features, list):
        raise ValueError(f"Expected a list of input features in the JSON file, but got {type(input_features)}.")
    
    return input_features

def generate_onnx_name(cbm_path):
    """
    Generate a meaningful ONNX file name based on the directory structure.
    This includes the level (e.g., L1, L2) and class name (e.g., class_C, class_D).
    """
    # Extract relevant parts of the path
    parts = cbm_path.split(os.sep)
    
    # Dynamically find the level (e.g., L1, L2, L3, etc.)
    level = None
    for part in parts:
        if part.startswith("L"):  # Look for the part that starts with "L"
            level = part
            break

    if not level:
        raise ValueError(f"Level not found in the path: {cbm_path}")
    
    # Check if the directory contains a class name (e.g., class-C, class-D, etc.)
    class_name = None
    for part in parts:
        if "class-" in part:
            # Replace hyphen with underscore to make it filename-friendly
            class_name = part.replace("class-", "class_")
            break
    
    # Construct the ONNX filename
    if class_name:
        # If class name is found, format as "Level_ClassName.onnx"
        onnx_name = f"{level}_{class_name}.onnx"
    else:
        # Otherwise, just use the level (e.g., "L1.onnx")
        onnx_name = f"{level}.onnx"
    
    return onnx_name

def extract_input_features_from_onnx(onnx_path):
    """
    Extract input features from an ONNX model's metadata.
    """
    try:
        model = onnx.load(onnx_path)
        metadata = model.metadata_props()
        
        # Look for the 'input_features' key in metadata
        input_features = None
        for meta in metadata:
            if meta.key == "input_features":
                input_features = json.loads(meta.value)  # Assuming features are stored as JSON
                break
                
        return input_features
    except Exception as e:
        print(f"Error loading ONNX file for input feature extraction: {e}")
        return None

def traverse_and_convert_models(base_dir, output_dir):
    """
    Traverse nested directories to find CatBoost models,
    convert them to ONNX format with metadata,
    and save them to a specified output directory.
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    for root, _, files in os.walk(base_dir):

        # Check if "catboost_v1.cbm" exists in the files list
        for file_name in files:
            if file_name == "catboost_v1.cbm":
                # Full path to the CatBoost model
                cbm_path = os.path.join(root, file_name)

                # Generate ONNX file name and path
                onnx_name = generate_onnx_name(cbm_path)
                onnx_path = os.path.join(output_dir, onnx_name)

                # Read input features from features.json if it exists
                features_path = os.path.join(root, "predictors.json")
                if os.path.exists(features_path):
                    with open(features_path, "r") as f:
                        features = json.load(f)
                    input_features = features.get("input_features", [])
                    output_features = features.get("output_features", None)  # Optional
                else:
                    print(f"Warning: 'features.json' not found in {root}. Skipping metadata.")
                    input_features = []
                    output_features = None

                # Check if the ONNX model already exists and if its metadata is valid
                if os.path.exists(onnx_path):
                    print(f"Checking existing ONNX model: {onnx_path}")
                    try:
                        existing_metadata = extract_features_from_onnx(onnx_path)

                        # If the input features do not match, we need to redo the conversion
                        if existing_metadata['input_features'] != input_features:
                            print(f"Input features do not match for {onnx_path}, rerunning conversion...")
                        else:
                            print(f"ONNX model {onnx_path} is up-to-date.")
                            continue  # Skip processing this model if the metadata is valid
                    except Exception as e:
                        print(f"Error loading ONNX model for {onnx_path}: {e}")
                        print("Reprocessing model...")
                else:
                    print(f"ONNX model does not exist for {cbm_path}, processing...")

                # Convert model to ONNX format with metadata
                try:
                    print(f"Processing: {cbm_path}")
                    convert_catboost_model_to_onnx_with_metadata(
                        catboost_model_path=cbm_path,
                        input_features=input_features,
                        output_features=output_features,
                        output_onnx_path=onnx_path
                    )
                    print(f"Converted and saved: {onnx_path}")
                except Exception as e:
                    print(f"Error processing {cbm_path}: {e}")

#%%
base_dir = r"H:\slovakia\openEO_tests"
    
# Directory to store the converted ONNX models
output_dir = r"H:\slovakia\openEO_tests\onnx"

# Call the function
traverse_and_convert_models(base_dir, output_dir)