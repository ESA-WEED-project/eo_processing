#%%
import os
import onnx
import requests
import zipfile
import tempfile
import shutil
from catboost import CatBoostClassifier


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
def add_metadata_to_onnx(onnx_path: str, input_features: list = None, output_features: list = None):
    """Load the ONNX model and add input/output feature metadata to the doc_string."""
    try:
        onnx_model = onnx.load(onnx_path)
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model: {e}")
    
    # Add metadata to the doc_string
    if input_features or output_features:
        metadata_entries = []
        
        if input_features:
            input_features_string = ', '.join(input_features)  # Convert list of input features to a string
            metadata_entries.append(onnx.StringStringEntryProto(key='input_features', value=input_features_string))
        
        if output_features:
            output_features_string = ', '.join(output_features)  # Convert list of output features to a string
            metadata_entries.append(onnx.StringStringEntryProto(key='output_features', value=output_features_string))

        # Add new metadata
        onnx_model.metadata_props.extend(metadata_entries)

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

    print(f"Model successfully converted and saved to ONNX format with input/output features in metadata.")


# Function to download a zip file to a temporary file
def download_zip_to_tempfile(url: str):
    """Download a zip file to a temporary file."""
    # Create a temporary file
    temp_zip = tempfile.NamedTemporaryFile(delete=False)
    
    try:
        print(f"Downloading file from {url}...")
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            for chunk in response.iter_content(chunk_size=1024):
                temp_zip.write(chunk)
            print(f"Downloaded zip file to temporary file {temp_zip.name}")
        else:
            raise ValueError(f"Failed to download file, status code: {response.status_code}")
    except Exception as e:
        temp_zip.close()
        os.remove(temp_zip.name)  # Cleanup if download fails
        raise ValueError(f"Error during download: {e}")
    
    temp_zip.close()  # Close the file after writing
    return temp_zip.name

# Function to unzip the file to a temporary directory
def unzip_to_tempdir(zip_file_path: str):
    """Unzip a file to a temporary directory and return the ONNX model path."""
    temp_dir = tempfile.mkdtemp()  # Create a temporary directory
    
    try:
        print(f"Unzipping the file {zip_file_path} to {temp_dir}...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        print(f"Unzipped files to {temp_dir}")
    except Exception as e:
        shutil.rmtree(temp_dir)  # Cleanup if unzip fails
        raise ValueError(f"Failed to unzip file: {e}")
    
    # Find the ONNX model in the extracted files
    onnx_model_path = None
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            if file.endswith(".onnx"):
                onnx_model_path = os.path.join(root, file)
                break
    
    if not onnx_model_path:
        shutil.rmtree(temp_dir)  # Cleanup if ONNX file is not found
        raise FileNotFoundError("ONNX model file not found in the unzipped folder.")
    
    return onnx_model_path, temp_dir

# Function to extract metadata from ONNX model
def extract_features_from_onnx(onnx_model_path: str):
    """Extract and return input and output features from the ONNX model's metadata."""
    try:
        onnx_model = onnx.load(onnx_model_path)
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model: {e}")

    metadata = {prop.key: prop.value for prop in onnx_model.metadata_props}

    input_features = metadata.get('input_features', '')
    output_features = metadata.get('output_features', '')

    input_features_list = input_features.split(', ') if input_features else []
    output_features_list = output_features.split(', ') if output_features else []

    return {
        'input_features': input_features_list,
        'output_features': output_features_list
    }

# Main function to download, unzip, and extract metadata using temporary files
def get_training_features_from_model_zip(url: str):
    """Download a zip from a URL, extract ONNX model, and get the feature list using temporary files."""
    
    # Step 1: Download the ZIP file to a temporary file
    temp_zip_path = download_zip_to_tempfile(url)
    
    # Step 2: Unzip the ZIP file to a temporary directory
    onnx_model_path, temp_dir = unzip_to_tempdir(temp_zip_path)
    
    # Step 3: Extract metadata from the ONNX model
    features = extract_features_from_onnx(onnx_model_path)
    
    # Step 4: Clean up temporary files
    os.remove(temp_zip_path)  # Remove the temporary zip file
    shutil.rmtree(temp_dir)  # Remove the temporary directory
    
    return features






