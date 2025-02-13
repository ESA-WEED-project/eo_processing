#%%
import os
import onnx
from catboost import CatBoostClassifier
from openeo.udf import inspect
from eo_processing.utils.external_dependency_utilities import download_file
from typing import List

def load_catboost_model(catboost_model_path: str) -> CatBoostClassifier:
    """
    Loads a CatBoost model from the specified file path. This function ensures that
    the provided file path exists and attempts to load the model using the CatBoost
    library. If the file is not found or loading fails for any reason, appropriate
    exceptions will be raised.

    :param catboost_model_path: Path to the CatBoost model file to be loaded.
    :return: A loaded instance of the CatBoostClassifier.
    :raises FileNotFoundError: If the provided file path does not exist.
    :raises ValueError: If the model fails to load with the specified file path.
    """
    if not os.path.exists(catboost_model_path):
        raise FileNotFoundError(f"CatBoost model file not found at: {catboost_model_path}")
    
    model = CatBoostClassifier()
    try:
        model.load_model(catboost_model_path, format="cbm")
        inspect(message=f"Model loaded from {catboost_model_path}")
        return model
    except Exception as e:
        raise ValueError(f"Failed to load CatBoost model from {catboost_model_path}: {e}")

def save_model_to_onnx(model: CatBoostClassifier, output_onnx_path: str) -> None:
    """
    Saves a CatBoost model in ONNX format.

    This function takes a trained CatBoostClassifier model and saves it to the specified
    output path in ONNX format. If the saving process fails for any reason, an exception
    is raised with relevant details.

    :param model: The trained CatBoostClassifier model to be saved. The model should be
        fully trained and ready for exporting to ONNX format.
    :param output_onnx_path: The destination file path where the ONNX model will be saved.
        The path should include the desired filename and .onnx extension.
    :return: This function does not return a value. It only saves the model to the specified
        location.
    :raises ValueError: If the model fails to save to ONNX format.
    """
    try:
        model.save_model(output_onnx_path, format="onnx")
        inspect(message=f"Model saved to ONNX format at {output_onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to save model to ONNX format at {output_onnx_path}: {e}")

def add_metadata_to_onnx(onnx_path: str, input_features: list = None, output_features: list = None) -> None:
    """
    Adds metadata entries to an ONNX model and saves the updated model back to the same file.

    This function allows adding input and output features as metadata entries to an ONNX model.
    The metadata is appended to the model's existing properties. If either input or output features
    are provided, they are validated and added as metadata entries. The updated model is then saved
    back to the provided ONNX file path.

    :param onnx_path: The file path to the ONNX model to which metadata will be added.
    :param input_features: A list of input feature names to be added as metadata.
        Must be a non-empty list if provided.
    :param output_features: A list of output feature names to be added as metadata.
        Must be a non-empty list if provided.
    :return: None
    :raises ValueError: If the ONNX model cannot be loaded or saved, or if inputs/outputs
        are not valid non-empty lists.
    """
    import onnx
    from onnx import StringStringEntryProto

    try:
        onnx_model = onnx.load(onnx_path)
        inspect(message=f"Loaded ONNX model from {onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {onnx_path}: {e}")
    
    metadata_entries = []

    # Validate and add input features
    if input_features:
        if isinstance(input_features, list) and input_features:
            input_features_string = ', '.join(input_features)
            metadata_entries.append(StringStringEntryProto(key='input_features', value=input_features_string))
        else:
            raise ValueError("Input features must be a non-empty list.")

    # Validate and add output features (optional)
    if output_features:
        if isinstance(output_features, list) and output_features:
            output_features_string = ', '.join(output_features)
            metadata_entries.append(StringStringEntryProto(key='output_features', value=output_features_string))
        else:
            raise ValueError("Output features must be a non-empty list.")

    # Add new metadata
    if metadata_entries:
        onnx_model.metadata_props.extend(metadata_entries)
        inspect(message=f"Metadata added: input_features = {input_features}, output_features = {output_features}")
    
    try:
        onnx.save(onnx_model, onnx_path)
        inspect(message=f"ONNX model with metadata saved to {onnx_path}")
    except Exception as e:
        raise ValueError(f"Failed to save ONNX model with metadata at {onnx_path}: {e}")

def convert_catboost_model_to_onnx_with_metadata(catboost_model_path: str, input_features: list,
                                                 output_features: list = None, output_onnx_path: str = None) -> None:
    """
    Converts a CatBoost model to ONNX format and adds metadata for input/output features.

    This function takes a trained CatBoost model stored on disk, converts it to the ONNX format,
    and embeds metadata for the input and output features provided by the user. The resulting
    ONNX model is saved at the specified or default output path. This makes it easier to
    integrate the model into production pipelines with consistent feature specifications.

    :param catboost_model_path: Path to the trained CatBoost model to be converted to ONNX.
    :param input_features: List of input feature names to include as metadata in the ONNX model.
    :param output_features: Optional list of output feature names to include as metadata.
    :param output_onnx_path: Optional path where the ONNX model with metadata will be saved.
                             If not provided, a default path based on `catboost_model_path` is generated.
    :return: None, as the function saves the converted ONNX model directly to the specified path.
    """
    
    # Step 1: Load the CatBoost model
    model = load_catboost_model(catboost_model_path)

    if output_onnx_path is None:
        output_onnx_path = onnx_output_path(catboost_model_path)
    
    # Step 2: Save the CatBoost model to ONNX format
    save_model_to_onnx(model, output_onnx_path)
    
    # Step 3: Add input/output features metadata to the ONNX model
    add_metadata_to_onnx(output_onnx_path, input_features=input_features, output_features=output_features)

    inspect(message="Model successfully converted and saved to ONNX format with input/output features in metadata.")

def extract_features_from_onnx(onnx_model_path: str) -> dict[str, List[str]]:
    """
    Extracts input and output features from an ONNX model by reading the metadata properties.
    If no output features are defined in the metadata, it assigns default output features
    based on the model output names.

    :param onnx_model_path: The file path to the ONNX model.
    :return: A dictionary containing the list of input features under the key 'input_features'
             and the list of output features under the key 'output_features'.
    :raises ValueError: If the ONNX model fails to load.
    """
    try:
        onnx_model = onnx.load(onnx_model_path)
        print(f"Loaded ONNX model from {onnx_model_path}")
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {onnx_model_path}: {e}")

    # Extract metadata
    metadata = {prop.key: prop.value for prop in onnx_model.metadata_props}

    # Retrieve input and output features from metadata
    input_features = metadata.get('input_features', '')
    output_features = metadata.get('output_features', '')

    input_features_list = input_features.split(', ') if input_features else []
    output_features_list = output_features.split(', ') if output_features else []

    # Log if no features found in metadata
    if not input_features_list:
        print("No input features found in metadata.")
    if not output_features_list:
        print("No output features found in metadata.")

        # Assign default output features based on the number of model outputs
        output_names = [output.name for output in onnx_model.graph.output]
        output_features_list = output_names
        print(f"Default output features assigned: {output_features_list}")
    
    return {
        'input_features': input_features_list,
        'output_features': output_features_list
    }

def onnx_output_path(catboost_model_path: str) -> str:
    """
    Generates the ONNX output file path for a given CatBoost model file.

    This function takes the file path of a CatBoost model as input and computes
    the corresponding ONNX file path by replacing the file extension with `.onnx`.
    It verifies the existence of the provided file and raises an exception if
    not found. The function also logs the generated ONNX path for inspection.

    :param catboost_model_path: The file path of the CatBoost model (must include
        the `.cbm` extension). The path must exist, otherwise an exception
        is raised.
    :return: The file path for the ONNX model with the `.onnx` extension.
    :raises FileNotFoundError: If the specified CatBoost model file does not
        exist at the provided path.
    """
    if not os.path.exists(catboost_model_path):
        raise FileNotFoundError(f"CatBoost model file not found at: {catboost_model_path}")

    directory, file_name = os.path.split(catboost_model_path)
    new_file_name = file_name.replace('.cbm', '.onnx')
    output_onnx_path = os.path.join(directory, new_file_name)
    inspect(message=f"ONNX output path generated: {output_onnx_path}")
    return output_onnx_path

def get_training_features_from_model(url: str) -> dict[str, List[str]]:
    """
    Extract training features from an ONNX model provided by a URL.

    This function downloads an ONNX model file from the specified URL, extracts
    training feature definitions using a custom extraction method, and returns
    the features as a dictionary mapping feature names to their respective
    training values. After extraction, the function ensures that the downloaded
    model file is removed from the filesystem regardless of success or failure
    in feature extraction.

    :param url: The URL where the ONNX model file is hosted.
    :return: A dictionary containing the list of input features under the key 'input_features'
             and the list of output features under the key 'output_features'.
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

def get_name_output_cube_features(model_urls: List[str]) -> List[str]:
    """
    Processes cube features from a list of model URLs by extracting and renaming output features.

    For each model URL, this function:
    1. Extracts metadata using the `get_training_features_from_model` function.
    2. Renames output features by prefixing them with the model name.
    3. Flattens the resulting list of features into a single list.

    :param model_urls: A list of URLs pointing to the ONNX models.
    :return: A flattened list of renamed output features.
    """
    cube_features = []

    for model_url in model_urls:
        metadata = get_training_features_from_model(model_url)

        # Extract input and output features
        input_features = metadata.get('input_features', [])
        output_features = metadata.get('output_features', [])

        # Derive the model name from the URL
        model_name = os.path.basename(model_url).split('.')[0]

        # Rename the output features with the model name prefix
        renamed_features = [f"{model_name}_{feature}" for feature in output_features]
        
        # Append renamed features to the cube list
        cube_features.append(renamed_features)

    # Flatten the list of lists
    flattened_features = [item for sublist in cube_features for item in sublist]
    return flattened_features
