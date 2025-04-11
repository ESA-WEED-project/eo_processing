import os
import sys
import functools
import requests
import tempfile
import xarray as xr
import numpy as np
import shutil
from urllib.parse import urlparse
from openeo.udf import inspect
from typing import Dict, List, Tuple, Union

sys.path.append("onnx_deps") 
import onnxruntime as ort

def is_onnx_file(file_path: str) -> bool:
    """
    Determines if a file is an ONNX file based on its extension.

    This function checks the provided file path and determines whether the file
    is an ONNX file by checking if the file name ends with the `.onnx` file extension.

    :param file_path: The path to the file whose extension is to be verified.
    :return: True if the file has a `.onnx` extension, otherwise False.
    """
    return file_path.endswith('.onnx')

def download_file(url: str, max_file_size_mb: int = 100, cache_dir: str = '/tmp/cache') -> str:
    """
    Downloads a file from the specified URL. The file is
    cached in a given directory, and downloads of the same file are prevented using a locking
    mechanism. If the file already exists in the cache, it will not be downloaded again.

    :param url: The URL of the file to download.
    :param max_file_size_mb: Maximum allowable file size in megabytes. Defaults to 100 MB.
    :param cache_dir: Directory where the downloaded file will be cached. Defaults to '/tmp/cache'.
    :return: The path to the downloaded file in the cache directory.

    :raises ValueError: If the file size exceeds the maximum limit or if there is an issue during the
        download process.
    """
    # Construct the file path within the cache directory (e.g., '/tmp/cache/model.onnx')
    os.makedirs(cache_dir, exist_ok=True)  # Ensure cache directory exists

    file_name = os.path.basename(urlparse(url).path)
    file_path = os.path.join(cache_dir, file_name)

    if os.path.exists(file_path):
        inspect(message=f"File {file_path} already exists in cache.")
        return file_path

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise error if the request fails

        file_size = 0
        with tempfile.NamedTemporaryFile(delete=False, suffix=".onnx") as temp_file:
            temp_file_path = temp_file.name
            for chunk in response.iter_content(chunk_size=1024):
                temp_file.write(chunk)
                file_size += len(chunk)
                if file_size > max_file_size_mb * 1024 * 1024:
                    raise ValueError(f"Downloaded file exceeds the size limit of {max_file_size_mb} MB")

        shutil.move(temp_file_path, file_path)  # Move AFTER the `with` block, so the file isn't deleted
        return file_path

    except Exception as e:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)  # Cleanup if an error occurs
        raise ValueError(f"Error downloading file: {e}")

@functools.lru_cache(maxsize=1)
def load_onnx_model(model_url: str, cache_dir: str = '/tmp/cache') -> Tuple[ort.InferenceSession, Dict[str, List[str]]]:
    """
    Loads an ONNX model from a given URL, caches the model locally, and initializes an ONNX Runtime
    inference session. Additionally, extracts metadata such as input and output features from the model.

    The function ensures the ONNX model is downloaded and locally stored in the specified cache directory
    to optimize repeated access. It also validates the model file and establishes an inference session with
    the CPU Execution Provider before safely extracting relevant metadata.

    :param model_url: The URL pointing to the ONNX model to be downloaded and loaded. The URL must provide
                      a valid ONNX model file.
    :param cache_dir: An optional directory path to store the cached ONNX model. Defaults to '/tmp/cache',
                      ensuring local caching for repeated access.
    :return: A tuple where the first element is an initialized ONNX Runtime inference session, and the second
             element is a dictionary containing metadata extracted from the model. The metadata includes lists
             of input features and output features.
    :raises ValueError: If the ONNX model fails to load, initialize, or metadata extraction fails within the
                        processing steps.
    """
    try:
        # Process the model file to ensure it's a valid ONNX model
        inspect(message=f"downloading model file from {model_url}...")
        model_path = download_file(model_url, cache_dir=cache_dir)

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

        inspect(message=f"Successfully extracted metadata from model at {model_path}...")
        return ort_session, metadata

    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {model_url}: {e}")

def preprocess_input(input_xr: xr.DataArray,
                     ort_session: ort.InferenceSession) -> Tuple[np.ndarray, Tuple[int, int, int]]:
    """
    Preprocesses input data for model inference using an ONNX runtime session. This
    function takes an xarray DataArray, rearranges its dimensions, and reshapes its
    values to match the input requirements of the ONNX model specified by the given
    ONNX InferenceSession.

    :param input_xr: Input data in the format of an xarray DataArray. The expected
        dimensions are "y", "x", and "bands", and the order of the dimensions will
        be transposed to match this requirement.
    :param ort_session: ONNX runtime inference session that specifies the model for
        inference. Used to determine the required input shape of the model.
    :return: A tuple containing:
        - A numpy array formatted to fit the input shape of the ONNX model.
        - The original shape of the input data as a tuple with the transposed "y",
          "x", and "bands" dimensions.
    """
    input_xr = input_xr.transpose("y", "x", "bands")
    input_shape = input_xr.shape
    input_np = input_xr.values.reshape(-1, ort_session.get_inputs()[0].shape[1])
    return input_np, input_shape

def run_inference(input_np: np.ndarray, ort_session: ort.InferenceSession) -> List[Dict[Union[str, int], float]]:
    """
    Executes inference using an ONNX Runtime session and input numpy array. This function
    constructs the input data for the ONNX runtime, runs the session, and extracts the
    output probabilities as list of dictionaries.

    :param input_np: Numpy array containing the input tensor data for the inference.
    :param ort_session: ONNX Runtime inference session object used to execute the model.
    :return: A list of dictionaries where each dictionary maps string labels to their
        corresponding probability values for each sample, as obtained from the model's output.
    """
    ort_inputs = {ort_session.get_inputs()[0].name: input_np}
    ort_outputs = ort_session.run(None, ort_inputs)
    probabilities_dicts = ort_outputs[1]  # just take probability results
    return probabilities_dicts

def postprocess_output(probabilities_dicts: List[Dict[Union[str, int], float]],
                       input_shape: Tuple[int, int, int]) -> np.ndarray:
    """
    Processes the output probabilities of a model into a reshaped and scaled NumPy array.

    This function takes a list of dictionaries representing the probabilities for each
    class per sample, along with the input shape of the data. It then processes the
    probabilities into a 3D array where each band represents the scaled class probabilities,
    reshaped based on the given input shape. The resulting array is scaled to percentages
    (0-100) and converted to byte format.

    :param probabilities_dicts: A list where each dictionary contains probabilities
        for each class, with class labels as keys and their corresponding probabilities as values.
    :param input_shape: A tuple representing the input shape in the form (height, width, bands).
    :return: A 3D NumPy array of shape (bands, height, width) containing scaled
        probabilities for each class in byte format.
    """

    # get the class labels assuming they are the same across all dictionaries (probabilities)
    class_labels = list(probabilities_dicts[0].keys())

    # Convert probabilities from dicts for each sample into a 2D array with shape (n_samples, n_classes)
    probabilities = np.array([[prob[class_id] for class_id in class_labels] for prob in probabilities_dicts])

    # Reshape probabilities into (bands, y, x), scale and convert to Byte
    probabilities = probabilities.T.reshape(len(class_labels), input_shape[0], input_shape[1]) * 100  # Scale by 100
    probabilities = probabilities.astype('uint8')

    return probabilities

def create_output_xarray(probabilities: np.ndarray,
                         input_xr: xr.DataArray) -> xr.DataArray:
    """
    Generate an xarray.DataArray output based on the input probabilistic numpy array and the
    coordinate information from the input xarray.DataArray. This function takes the probability
    data and organizes it into a structured DataArray format with dimensions and corresponding
    coordinates inherited from the reference input.

    :param probabilities: A 3D numpy array containing probability values structured in
        [bands, y, x] format. Represents the generated probabilities for input data.
    :param input_xr: The input reference xarray.DataArray. Used to inherit x and y coordinate
        information and ensure the output DataArray retains the input spatial alignment.
    :return: A 3D xarray.DataArray, structured with the given probabilities, and
        dimensions [bands, y, x]. Its coordinates for y and x are derived from the input_xr.
    """
    return xr.DataArray(
        probabilities,
        dims=["bands", "y", "x"],
        coords={
            'y': input_xr.coords['y'],
            'x': input_xr.coords['x']
        }
    )

def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """
    Applies multiple ONNX models on a given data cube for inference. The function ensures that the input
    cube is processed to fill any missing values and is in the correct data type to be compatible with the
    models. Models are loaded from the specified context, and their corresponding metadata is utilized to
    select appropriate sub-bands from the cube. Each model is applied sequentially, and the results are
    assembled into an output cube with all processed band outputs.

    Note: The function name and arguments are defined by the UDF API.
    More information can be found here:
    https://open-eo.github.io/openeo-python-client/udf.html#udf-function-names-and-signatures

    :param cube: The data cube on which the models will be applied. It must be an `xr.DataArray`.
    :param context: A dictionary that includes inference configuration, notably the list of models under
        "model_list" key.
    :return: An `xr.DataArray` representing the processed output cube after successfully applying all models.
    """
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
        inspect(message=f"Subsetting the feature datacube by needed input features.")
        subsampled_data_array = cube.sel(bands=input_band)

        # preprocess input array to numpy array in correct shape
        input_np, input_shape = preprocess_input(subsampled_data_array, ort_session)
        # run inference
        inspect(message=f"Running inference ...")
        probabilities_dicts = run_inference(input_np, ort_session)
        # post-process probabilities to correct shape and Byte dtype
        inspect(message=f"Post-processing probabilities and converting to xarray DataArray...")
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
