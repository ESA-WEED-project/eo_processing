# /// script
# dependencies = [
# "filelock",
# "onnxruntime",
# ]
# ///

import os
import functools
import requests
import tempfile
import onnxruntime as ort
import xarray as xr
import numpy as np
import shutil
from urllib.parse import urlparse
from openeo.udf import inspect
from typing import Dict
from filelock import FileLock


def is_zip_file(url: str) -> bool:
    """Check if the URL points to a ZIP file."""
    return url.lower().endswith('.zip')

def is_onnx_file(file_path: str) -> bool:
    """Check if the file is an ONNX model based on its extension."""
    return file_path.endswith('.onnx')
    
def download_file_with_lock(url: str, max_file_size_mb: int = 100, cache_dir: str = '/tmp/cache') -> str:
    """Download a file with concurrency protection and store it temporarily."""
    
    # Extract the file name from the URL (e.g., "model_1.onnx")
    file_name = os.path.basename(urlparse(url).path)
    
    # Construct the file path within the cache directory (e.g., '/tmp/cache/model.onnx')
    file_path = os.path.join(cache_dir, file_name)
    
    # Lock file to prevent concurrent downloads
    lock_path = file_path + '.download.lock'
    lock = FileLock(lock_path)
    
    with lock:
        # Check if the file already exists in the cache
        if os.path.exists(file_path):
            print(f"File {file_path} already exists in cache.")
            return file_path
        
        try:
            # Download the file to a temporary location
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".onnx")
            temp_file_path = temp_file.name  # Store the temporary file path
            
            inspect(message=f"Downloading file from {url}...")
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                file_size = 0
                with temp_file:
                    for chunk in response.iter_content(chunk_size=1024):
                        temp_file.write(chunk)
                        file_size += len(chunk)
                        if file_size > max_file_size_mb * 1024 * 1024:
                            raise ValueError(f"Downloaded file exceeds the size limit of {max_file_size_mb} MB")

                inspect(message=f"Downloaded file to {temp_file_path}")
                
                # After download is complete, move the file from temp to the final destination
                shutil.move(temp_file_path, file_path)  # Move the file to final location

                return file_path  # Return path of the final model file

            else:
                raise ValueError(f"Failed to download file, status code: {response.status_code}")

        except Exception as e:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)  # Clean up temporary file on error
            raise ValueError(f"Error downloading file: {e}")

@functools.lru_cache(maxsize=5)
def load_onnx_model(model_url: str, cache_dir: str = '/tmp/cache') -> ort.InferenceSession:
    """
    Load an ONNX model into an ONNX Runtime session.

    Args:
        model_url (str): The URL or file path to the ONNX model.
        cache_dir (str): Directory for caching or processing model files.

    Returns:
        ort.InferenceSession: The ONNX Runtime session for the loaded model.

    Raises:
        ValueError: If the model file cannot be processed or loaded.
    """
    try:
        # Process the model file to ensure it's a valid ONNX model
        model_path = download_file_with_lock(model_url, cache_dir=cache_dir)

        # Initialize the ONNX Runtime session
        inspect(message=f"Initializing ONNX Runtime session for model at {model_path}...")
        ort_session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
        inspect(message="ONNX model successfully loaded into ONNX Runtime session.")
        return ort_session

    except Exception as e:
        raise ValueError(f"Failed to load the ONNX model from {model_url}. Error: {e}")

def preprocess_input(input_xr: xr.DataArray, ort_session: ort.InferenceSession) -> tuple:
    """
    Preprocess the input DataArray by ensuring the dimensions are in the correct order,
    reshaping it, and returning the reshaped numpy array and the original shape.
    """
    input_xr = input_xr.transpose("y", "x", "bands")
    input_shape = input_xr.shape
    input_np = input_xr.values.reshape(-1, ort_session.get_inputs()[0].shape[1])
    return input_np, input_shape

def run_inference(input_np: np.ndarray, ort_session: ort.InferenceSession) -> tuple:
    """
    Run inference using the ONNX runtime session and return predicted labels and probabilities.
    """
    ort_inputs = {ort_session.get_inputs()[0].name: input_np}
    ort_outputs = ort_session.run(None, ort_inputs)
    probabilities_dicts = ort_outputs[1]
    return probabilities_dicts

def postprocess_output(probabilities_dicts: list, input_shape: tuple) -> tuple:
    """
    Postprocess the output by reshaping the predicted labels and probabilities into the original spatial structure.
    """

    # Assuming class labels are the same across all dictionaries (probabilities)
    class_labels = list(probabilities_dicts[0].keys())

    # Convert probabilities from dicts into a 2D array with shape (n_samples, n_classes)
    probabilities = np.array([[prob[class_id] for class_id in class_labels] for prob in probabilities_dicts])

    # Reshape probabilities into (n_classes, height, width)
    probabilities = probabilities.T.reshape(len(class_labels), input_shape[0], input_shape[1]) * 100  # Scale by 100 as needed
    probabilities = probabilities.astype('uint8')

    return probabilities

def create_output_xarray(probabilities: np.ndarray, 
                         input_xr: xr.DataArray) -> xr.DataArray:
    """
    Create an xarray DataArray with predicted labels and probabilities stacked along the bands dimension.
    """
    #combined_data = np.concatenate([
    #    predicted_labels[np.newaxis, :, :],  # Shape (1, y, x) for predicted labels
    #    probabilities  # Shape (n_classes, y, x) for probabilities
    #], axis=0)

    return xr.DataArray(
        probabilities,
        dims=["bands", "y", "x"],
        coords={
            'y': input_xr.coords['y'],
            'x': input_xr.coords['x']
        }
    )

def apply_model(input_xr: xr.DataArray, context: Dict) -> xr.DataArray:
    """
    Run inference on the given input data using the provided ONNX runtime session.
    This method is called for each timestep in the chunk received by apply_datacube.
    """
    # Step 1: Load the ONNX model
    try:
        ort_session = load_onnx_model(context.get("model_url"), cache_dir="/tmp/cache")
    except ValueError as e:
        raise RuntimeError(f"Model loading failed: {e}")

    # Step 2: Preprocess the input
    input_np, input_shape = preprocess_input(input_xr, ort_session)

    # Step 3: Perform inference
    probabilities_dicts = run_inference(input_np, ort_session)

    # Step 4: Postprocess the output
    probabilities = postprocess_output(probabilities_dicts, input_shape)

    # Step 5: Create the output xarray
    return create_output_xarray(probabilities, input_xr)

def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """
    Function that is called for each chunk of data that is processed.
    The function name and arguments are defined by the UDF API.
    
    More information can be found here: 
    https://open-eo.github.io/openeo-python-client/udf.html#udf-function-names-and-signatures

    """

    cube.transpose("y", "x", "bands")
    cube = cube.fillna(0)
    cube = cube.astype('float32')

    # Apply the model for each timestep in the chunk
    output_data = apply_model(cube, context)
    

    return output_data