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
from typing import Dict, List, Tuple
from filelock import FileLock


def is_onnx_file(file_path):
    return file_path.endswith('.onnx')
    
def download_file_with_lock(url, max_file_size_mb = 100, cache_dir = '/tmp/cache'):

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
def load_onnx_model(model_url, cache_dir = '/tmp/cache'):
    try:
        # Process the model file to ensure it's a valid ONNX model
        model_path = download_file_with_lock(model_url, cache_dir=cache_dir)

        # Initialize the ONNX Runtime session
        inspect(message=f"Initializing ONNX Runtime session for model at {model_path}...")
        ort_session = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])

        # Extract metadata
        model_meta = ort_session.get_modelmeta()

        input_features = model_meta.custom_metadata_map.get("input_features", "").split(",")
        input_features = [band.strip() for band in input_features]

        output_features = model_meta.custom_metadata_map.get("output_features", "").split(",")
        output_features = [band.strip() for band in output_features]

        metadata = {
            "input_features": input_features,
            "output_features": output_features,
        }

        inspect(message=f"Successfully extracted features from model at {model_path}...")
        return ort_session, metadata

    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {model_url}: {e}")

def preprocess_input(input_xr,
                     ort_session) :
    input_xr = input_xr.transpose("y", "x", "bands")
    input_shape = input_xr.shape
    input_np = input_xr.values.reshape(-1, ort_session.get_inputs()[0].shape[1])
    return input_np, input_shape

def run_inference(input_np, ort_session):
    ort_inputs = {ort_session.get_inputs()[0].name: input_np}
    ort_outputs = ort_session.run(None, ort_inputs)
    probabilities_dicts = ort_outputs[1] # just take probability results
    return probabilities_dicts

def postprocess_output(probabilities_dicts,
                       input_shape) :

    # get the class labels assuming they are the same across all dictionaries (probabilities)
    class_labels = list(probabilities_dicts[0].keys())

    # Convert probabilities from dicts for each sample into a 2D array with shape (n_samples, n_classes)
    probabilities = np.array([[prob[class_id] for class_id in class_labels] for prob in probabilities_dicts])

    # Reshape probabilities into (bands, y, x), scale and convert to Byte
    probabilities = probabilities.T.reshape(len(class_labels), input_shape[0], input_shape[1]) * 100  # Scale by 100
    probabilities = probabilities.astype('uint8')

    return probabilities

def create_output_xarray(probabilities,
                         input_xr) :

    return xr.DataArray(
        probabilities,
        dims=["bands", "y", "x"],
        coords={
            'y': input_xr.coords['y'],
            'x': input_xr.coords['x']
        }
    )

def apply_datacube(cube, context) :

    # fill nan in cube and make sure cube is in right dtype for inference
    cube = cube.fillna(0)
    cube = cube.astype('float32')

    # get the list of models to apply on the cube from context
    model_urls = context.get("model_list")

    # loop over the models and apply on input array
    output_cube_initialized = False
    for i, url in enumerate(model_urls):

        inspect(message=f"running inference for: {url}")
 
        # load the ONNX model and extract metadata
        ort_session, metadata = load_onnx_model(url, cache_dir="/tmp/cache")
        input_band = metadata['input_features']

        # Subset the data array using the selected indices
        subsampled_data_array = cube.sel(bands=input_band)

        # preprocess input array to numpy array in correct shape
        input_np, input_shape = preprocess_input(subsampled_data_array, ort_session)
        # run inference
        probabilities_dicts = run_inference(input_np, ort_session)
        # post-process probabilities to correct shape and Byte dtype
        probabilities = postprocess_output(probabilities_dicts, input_shape)
        # convert back to Xarray DataArray
        model_output_cube = create_output_xarray(probabilities, subsampled_data_array)

        # merge the model results into one super-cube
        if not output_cube_initialized:
            # Initialize the output_cube only on the first iteration
            output_cube = model_output_cube
            output_cube_initialized = True
        else:
            # Append to output_cube starting from the second iteration
            output_cube = xr.concat([output_cube, model_output_cube], dim="bands")

    # make sure output Xarray has the correct dtype
    output_cube = output_cube.astype('uint8')

    return output_cube
