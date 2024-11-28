#%%
import os
import onnx
from catboost import CatBoostClassifier
from openeo.udf import inspect
from eo_processing.utils.external_dependency_utilities import download_file

# Function to load the CatBoost model
def load_catboost_model(catboost_model_path: str):
    """Load the CatBoost model from a saved .cbm file."""
    if not os.path.exists(catboost_model_path):
        raise FileNotFoundError(f"CatBoost model file not found at: {catboost_model_path}")
    
    model = CatBoostClassifier()
    try:
        model.load_model(catboost_model_path, format="cbm")
        inspect(message=f"Model loaded from {catboost_model_path}")
        return model
    except Exception as e:
        raise ValueError(f"Failed to load CatBoost model from {catboost_model_path}: {e}")

# Function to save the model as ONNX
def save_model_to_onnx(model, output_onnx_path: str):
    """Save the CatBoost model to ONNX format."""
    try:
        model.save_model(output_onnx_path, format="onnx")
        inspect(message=f"Model saved to ONNX format at {output_onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to save model to ONNX format at {output_onnx_path}: {e}")


# Function to add metadata to the ONNX model
def add_metadata_to_onnx(onnx_path: str, input_features: list = None, output_features: list = None):
    """Load the ONNX model and add input/output feature metadata to the doc_string."""
    try:
        onnx_model = onnx.load(onnx_path)
        inspect(message=f"Loaded ONNX model from {onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {onnx_path}: {e}")
    
    if input_features or output_features:
        metadata_entries = []

        # Validate and add input features
        if input_features:
            if isinstance(input_features, list) and input_features:
                input_features_string = ', '.join(input_features)
                metadata_entries.append(onnx.StringStringEntryProto(key='input_features', value=input_features_string))
            else:
                raise ValueError("Input features must be a non-empty list.")

        # Validate and add output features
        if output_features:
            if isinstance(output_features, list) and output_features:
                output_features_string = ', '.join(output_features)
                metadata_entries.append(onnx.StringStringEntryProto(key='output_features', value=output_features_string))
            else:
                raise ValueError("Output features must be a non-empty list.")

        # Add new metadata
        onnx_model.metadata_props.extend(metadata_entries)
        inspect(message=f"Metadata added: input_features = {input_features}, output_features = {output_features}")
    
    try:
        onnx.save(onnx_model, onnx_path)
        inspect(message=f"ONNX model with metadata saved to {onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to save ONNX model with metadata at {onnx_path}: {e}")


# Main conversion function that ties everything together
def convert_catboost_model_to_onnx_with_metadata(catboost_model_path: str, 
                                                 input_features: list, 
                                                 output_features: list, 
                                                 output_onnx_path: str = None):
    """Main function to convert CatBoost model to ONNX format with input/output features."""
    
    # Step 1: Load the CatBoost model
    model = load_catboost_model(catboost_model_path)

    if output_onnx_path is None:
        output_onnx_path = onnx_output_path(catboost_model_path)
    
    # Step 2: Save the CatBoost model to ONNX format
    save_model_to_onnx(model, output_onnx_path)
    
    # Step 3: Add input/output features metadata to the ONNX model
    add_metadata_to_onnx(output_onnx_path, input_features=input_features, output_features=output_features)

    inspect(message=f"Model successfully converted and saved to ONNX format with input/output features in metadata.")

# Function to extract metadata from ONNX model
def extract_features_from_onnx(onnx_model_path: str):
    """Extract and return input and output features from the ONNX model's metadata."""
    try:
        onnx_model = onnx.load(onnx_model_path)
        inspect(message=f"Loaded ONNX model from {onnx_model_path}")
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {onnx_model_path}: {e}")

    metadata = {prop.key: prop.value for prop in onnx_model.metadata_props}

    input_features = metadata.get('input_features', '')
    output_features = metadata.get('output_features', '')

    input_features_list = input_features.split(', ') if input_features else []
    output_features_list = output_features.split(', ') if output_features else []

    if not input_features_list:
        inspect(message="No input features found in metadata.")
    if not output_features_list:
        inspect(message="No output features found in metadata.")
    
    return {
        'input_features': input_features_list,
        'output_features': output_features_list
    }

#dependency management
def onnx_output_path(catboost_model_path: str) -> str:
    """Generate the expected output ONNX path."""
    if not os.path.exists(catboost_model_path):
        raise FileNotFoundError(f"CatBoost model file not found at: {catboost_model_path}")

    directory, file_name = os.path.split(catboost_model_path)
    new_file_name = file_name.replace('.cbm', '.onnx')
    output_onnx_path = os.path.join(directory, new_file_name)
    inspect(message=f"ONNX output path generated: {output_onnx_path}")
    return output_onnx_path


# Main function to download, unzip, and extract metadata using temporary files
def get_training_features_from_model(url: str):
    """
    Download a model file (ZIP or ONNX), process it, 
    and extract input/output features.
    """
    # Step 1: Process the model file
    onnx_model_path = download_file(url)
    
    # Step 2: Extract metadata from the ONNX model
    try:
        features = extract_features_from_onnx(onnx_model_path)
        return features
    finally:
        # Ensure ONNX file cleanup
        if os.path.exists(onnx_model_path):
            os.remove(onnx_model_path)






