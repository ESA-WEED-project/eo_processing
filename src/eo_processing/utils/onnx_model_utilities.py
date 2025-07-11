#%%
import joblib
import json
import os
import onnx
from catboost import CatBoostClassifier
from sklearn.base import BaseEstimator
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
from typing import List, Optional, Dict, Union
from eo_processing.utils.external_dependency_utilities import download_file

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
        print(f"Model loaded from {catboost_model_path}")
        return model
    except Exception as e:
        raise ValueError(f"Failed to load CatBoost model from {catboost_model_path}: {e}")

def load_sklearn_model(model_path: str) -> BaseEstimator:
    """
    Loads a pickled scikit-learn model or object from the specified file path using joblib.
    If the file is not found or loading fails for any reason, appropriate exceptions will be raised.

    :param model_path: Path to the pickled model or object file.
    :return: The loaded Python object.
    :raises FileNotFoundError: If the file does not exist.
    :raises ValueError: If loading fails.
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")
    try:
        model = joblib.load(model_path)
        print(f"Model loaded from {model_path}")
        return model
    except Exception as e:
        raise ValueError(f"Failed to load model from {model_path}: {e}")
      

def save_model_to_onnx(model: Union[CatBoostClassifier, BaseEstimator],
                       output_onnx_path: str,
                       input_features: List[str],
                       target_opset: int= 9) -> None:
    """
    Saves a CatBoost or scikit-learn base model in ONNX format.

    :param model: The trained model
    :param output_onnx_path: File path to save the ONNX model (should end in .onnx).
    :param input_features: a List of strings of the input features
    :param target_opset: target IR version of the output onnx file
    :raises ValueError: If model type is unsupported or saving fails.
    """
    try:
        print(f"Type model: {model}")
        if isinstance(model, CatBoostClassifier):
            model.save_model(
                output_onnx_path,
                format="onnx",
                export_parameters={
                    "onnx_doc_string": "CatBoost model in ONNX format.",
                    "onnx_domain": "ai.catboost",
                    "onnx_model_version": 1,
                    "onnx_graph_name": os.path.splitext(os.path.basename(output_onnx_path))[0]
                }
            )
            print(f"CatBoost model saved to ONNX: {os.path.basename(output_onnx_path)}")

        elif isinstance(model, BaseEstimator):
            n_features = len(input_features)
            
            initial_type = [('input', FloatTensorType([None, n_features]))]
            onnx_model = convert_sklearn(model, initial_types=initial_type, target_opset=target_opset)

            # Modify ONNX metadata
            onnx_model.doc_string = "scikit-learn base model in ONNX format"
            onnx_model.domain = "ai.sklearn"
            onnx_model.model_version = 1
            onnx_model.graph.name = os.path.splitext(os.path.basename(output_onnx_path))[0]
            
            with open(output_onnx_path, "wb") as f:
                f.write(onnx_model.SerializeToString())
            
            print(f"scikit-learn base model saved to ONNX: {os.path.basename(output_onnx_path)}")

        else:
            raise ValueError("Unsupported model type. Only CatBoostClassifier and scikit-learn base models are supported.")

    except Exception as e:
        raise ValueError(f"Failed to save model to ONNX format at {output_onnx_path}: {e}")

def add_metadata_to_onnx(onnx_path: str, input_features: Optional[List] = None,
                         output_features: Optional[List] = None, add_metadata: Optional[Dict] = None) -> None:
    """
    Add metadata to an ONNX model file.

    This function injects metadata such as input features, output features, class
    mappings, and any additional key-value pairs into the metadata properties of
    an ONNX model. The metadata is saved back to the ONNX model file specified by
    the user.

    :param onnx_path: The file path to the ONNX model to which metadata will be added.
    :param input_features: A list of input feature names to add as metadata. Each
        item in the list must be a string.
    :param output_features: A list of output feature names to add as metadata.
        Each item in the list must be a string. The function also uses this for
        creating a class mapping dictionary.
    :param add_metadata: An optional dictionary containing additional key-value
        metadata entries to add to the ONNX model.

    :raises ValueError: If the ONNX model fails to load or save, if `input_features`
        or `output_features` are invalid or not non-empty lists, or if `add_metadata`
        is not a dictionary.
    """
    try:
        onnx_model = onnx.load(onnx_path)
        print(f"Reload ONNX model from {os.path.basename(onnx_path)} for metadata infusion")
    except Exception as e:
        raise ValueError(f"Failed to load ONNX model from {onnx_path}: {e}")
    
    metadata_entries: Dict = {}

    # Validate and add input feature names
    if input_features:
        if isinstance(input_features, list) and input_features:
            input_features_string = ', '.join(input_features)
            metadata_entries.update(input_features = input_features_string)
        else:
            raise ValueError("Input features must be a non-empty list.")

    # Validate and add output band names bases on class_names (optional)
    if output_features:
        if isinstance(output_features, list) and output_features:
            output_features_string = ', '.join(output_features)
            metadata_entries.update(output_features = output_features_string)
        else:
            raise ValueError("Output features must be a non-empty list.")

    if metadata_entries:
        print(f"Metadata added: input_features = {input_features}, \noutput_features = {output_features}")

    # create also from the outbandnames the dict for the class_names encoder
    class_dict = {}
    for bname in output_features:
        parts = bname.split('-')
        if len(parts) >= 2 and parts[-1].isdigit():
            class_dict[int(parts[-1])] = parts[-2]
        else:
            print(f"Warning: Skipping malformed output name '{bname}'")


    metadata_entries.update({
        'class_names': class_dict,
        'model_author': 'VITO',
        'model_license': 'CC BY-NC-SA 4.0',
    })

    # add additional if exist
    if add_metadata:
        if isinstance(add_metadata, dict):
            metadata_entries.update(add_metadata)
        else:
            raise ValueError("add_metadata must be a dict.")

    # write metadata to ONNX model
    for k,v in metadata_entries.items():
        m = onnx_model.metadata_props.add()
        m.key = k
        m.value = json.dumps(v)

    try:
        onnx.save(onnx_model, onnx_path)
        print(f"ONNX model with metadata saved: {os.path.basename(onnx_path)}")
    except Exception as e:
        raise ValueError(f"Failed to save ONNX model with metadata at {onnx_path}: {e}")

def convert_model_to_onnx_with_metadata(model_path: str,
                                        input_features: Dict,
                                        output_features: Dict,
                                        output_onnx_path: Dict,
                                        target_opset: int = 9,
                                        add_metadata: Optional[Dict] = None) -> None:
    """
    Convert a CatBoost or scikit-learn base models to ONNX format and optionally add metadata.

    This function loads a pre-trained CatBoost or scikit-learn base model from the given path, converts it
    to ONNX format, and saves it. Optionally, it allows adding metadata for input
    and output features to the ONNX model.

    :param model_path: Path to the model file to be converted.
    :param input_features: Optional list of input feature names to be added as ONNX
        metadata.
    :param output_features: Optional list of output feature names to be added as ONNX
        metadata.
    :param output_onnx_path: Optional path to save the converted ONNX model. If not
        specified, a default ONNX filename will be created based on the model
        path.
    :param add_metadata: Optional dictionary containing any additional metadata to be
        added to the ONNX model.
    :param target_opset: target IR version of the output onnx file
    """
    
    # Step 1: Load the model
    try: 
        if model_path.endswith(".cbm"):
            model = load_catboost_model(model_path)
        elif model_path.endswith((".pkl", ".joblib")):
            model = load_sklearn_model(model_path)
    except Exception as e: 
        raise ValueError(f"Failed to load ONNX model from {model_path}: {e}")

    if output_onnx_path is None:
        output_onnx_path = onnx_output_path(model_path)
    
    # Step 2: Save the model to ONNX format
    save_model_to_onnx(model, output_onnx_path, input_features, target_opset)
    
    # Step 3: Add input/output features metadata to the ONNX model
    add_metadata_to_onnx(output_onnx_path, input_features=input_features, output_features=output_features,
                         add_metadata=add_metadata)

    print("Model successfully converted and saved to ONNX format with input/output features in metadata.")

def extract_features_from_onnx(onnx_model_path: str) -> Dict[str, List[str]]:
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

    if input_features:
        if input_features.startswith('"') and input_features.endswith('"'):
            input_features = input_features[1:-1]

    if output_features:
        if output_features.startswith('"') and output_features.endswith('"'):
            output_features = output_features[1:-1]

    input_features_list = [feat.strip() for feat in input_features.split(',')] if input_features else []
    output_features_list = [feat.strip() for feat in output_features.split(',')] if output_features else []

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

def onnx_output_path(model_path: str) -> str:
    """
    Generates the ONNX output file path for a given CatBoost or scikit-learn base model file.

    This function takes the file path of a CatBoost or scikit-learn base model as input and computes
    the corresponding ONNX file path by replacing the file extension with `.onnx`.
    It verifies the existence of the provided file and raises an exception if
    not found. The function also logs the generated ONNX path for inspection.

    :param model_path: The file path of the CatBoost or scikit-learn base model (must include
        the `.cbm` extension). The path must exist, otherwise an exception
        is raised.
    :return: The file path for the ONNX model with the `.onnx` extension.
    :raises FileNotFoundError: If the specified model file does not
        exist at the provided path.
    """
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at: {model_path}")

    directory, file_name = os.path.split(model_path)
    for ext in ('.cbm', '.pkl', '.joblib'):
        if file_name.endswith(ext):
            new_file_name = file_name[:-len(ext)] + '.onnx'
            break
    output_onnx_path = os.path.join(directory, new_file_name)
    print(f"ONNX output path generated: {output_onnx_path}")
    return output_onnx_path

def get_training_features_from_model(url: str) -> Dict[str, List[str]]:
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
    Extract output cube features from the given ONNX model URLs.

    This function processes a list of ONNX model URLs to extract output features
    (band names) from their metadata. The metadata is fetched by utilizing the
    `get_training_features_from_model` function, and the output features are
    collected and accumulated into a single list.

    :param model_urls: A list of strings, where each string is the URL of an ONNX model.

    :return: A list of strings representing the cube features (output features)
        extracted from the ONNX models.
    """
    cube_features = []

    for model_url in model_urls:
        # get needed input_features / output band_names from ONNX model
        metadata = get_training_features_from_model(model_url)

        # Extract output features and extend
        cube_features.extend(metadata.get('output_features', []))

    return cube_features
