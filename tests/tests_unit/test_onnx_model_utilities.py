
import pytest
import os
from unittest import mock
from catboost import CatBoostClassifier
import onnx
from onnx import helper, TensorProto
import numpy as np
from eo_processing.utils.onnx_model_utilities import (load_catboost_model, save_model_to_onnx, add_metadata_to_onnx, extract_features_from_onnx)



# Mocking the CatBoost model
@pytest.fixture
def mock_catboost_model():
    model = mock.MagicMock(spec=CatBoostClassifier)
    return model

# Mock os.path.exists to return True for model file presence
@pytest.fixture
def mock_exists(monkeypatch):
    monkeypatch.setattr(os.path, 'exists', lambda path: True)

#add some testing with actual models

@pytest.fixture
def real_catboost_model(tmpdir):
    # Create a simple dataset to train the model
    X_train = np.array([[0, 1], [1, 0], [1, 1], [0, 0]])
    y_train = np.array([0, 1, 1, 0])

    # Initialize the CatBoostClassifier and train it
    model = CatBoostClassifier(iterations=1, depth=2, learning_rate=1, loss_function='Logloss', verbose=False)
    model.fit(X_train, y_train)

    # Save the trained model to a temporary ONNX file
    onnx_model_path = os.path.join(tmpdir, "trained_model.onnx")
    model.save_model(onnx_model_path, format="onnx")

    # Return both the model and the path to the ONNX file
    return model, onnx_model_path

@pytest.fixture
def real_onnx_model(tmpdir):
    # Create a simple ONNX model
    X = helper.make_tensor_value_info('X', TensorProto.FLOAT, [1, 2])
    Y = helper.make_tensor_value_info('Y', TensorProto.FLOAT, [1, 2])

    node = helper.make_node(
        'Relu',  # node operation
        inputs=['X'],
        outputs=['Y'],
    )

    graph = helper.make_graph(
        [node],
        'test-graph',  # name
        [X],  # inputs
        [Y],  # outputs
    )

    model = helper.make_model(graph)

    # Save the ONNX model to a temporary file
    onnx_model_path = os.path.join(tmpdir, "test_model.onnx")
    onnx.save(model, onnx_model_path)

    return onnx_model_path  # Return the path to the ONNX model

# Test load_catboost_model
def test_load_catboost_model(mock_exists, mock_catboost_model):
    model_path = "fake_model_path.cbm"

    with mock.patch('catboost.CatBoostClassifier.load_model', return_value=None):
        model = load_catboost_model(model_path)
        assert isinstance(model, CatBoostClassifier)

def test_load_catboost_model_file_not_found():
    model_path = "non_existent_model.cbm"
    with pytest.raises(FileNotFoundError):
        load_catboost_model(model_path)

def test_load_catboost_model_fail(mock_exists):
    model_path = "corrupt_model.cbm"
    
    with mock.patch('catboost.CatBoostClassifier.load_model', side_effect=Exception("Load error")):
        with pytest.raises(ValueError, match="Failed to load CatBoost model"):
            load_catboost_model(model_path)

# Test save_model_to_onnx
def test_save_model_to_onnx(mock_catboost_model):
    onnx_path = "model.onnx"
    with mock.patch('catboost.CatBoostClassifier.save_model', return_value=None):
        save_model_to_onnx(mock_catboost_model, onnx_path)

def test_save_model_to_onnx_fail(mock_catboost_model):

    # Define a dummy ONNX path
    onnx_path = "invalid_model.onnx"
    
    # Mock the `save_model` method to raise an Exception
    mock_catboost_model.save_model.side_effect = Exception("Save error")
    
    # Expect the save_model_to_onnx to raise a ValueError
    with pytest.raises(ValueError, match="Failed to save model to ONNX format"):
        save_model_to_onnx(mock_catboost_model, onnx_path)

# Test add_metadata_to_onnx
def test_add_metadata_to_onnx():
    onnx_path = "test_model.onnx"
    
    # Create a mock ONNX model
    mock_onnx_model = mock.MagicMock()
    
    # Patch the onnx.load and onnx.save methods
    with mock.patch('onnx.load', return_value=mock_onnx_model), \
         mock.patch('onnx.save', return_value=None):
        add_metadata_to_onnx(onnx_path, input_features=['feature1', 'feature2'], output_features=['output1'])
    
    # Verify if metadata was correctly appended
    assert mock_onnx_model.metadata_props.extend.called

def test_add_metadata_to_onnx_fail_load():
    onnx_path = "invalid_model.onnx"
    
    with mock.patch('onnx.load', side_effect=Exception("Load error")):
        with pytest.raises(ValueError, match="Failed to load ONNX model"):
            add_metadata_to_onnx(onnx_path)

def test_add_metadata_to_onnx_fail_save():
    onnx_path = "test_model.onnx"
    mock_onnx_model = mock.MagicMock()

    with mock.patch('onnx.load', return_value=mock_onnx_model), \
         mock.patch('onnx.save', side_effect=Exception("Save error")):
        with pytest.raises(ValueError, match="Failed to save ONNX model with metadata"):
            add_metadata_to_onnx(onnx_path)

def test_add_metadata_to_onnx_with_real_model(real_onnx_model):
    input_features = ['feature1', 'feature2']
    output_features = ['output1']

    # Call the function with the real ONNX model path
    add_metadata_to_onnx(real_onnx_model, input_features=input_features, output_features=output_features)

    # Load the modified ONNX model and check the metadata
    onnx_model = onnx.load(real_onnx_model)
    
    metadata = {prop.key: prop.value for prop in onnx_model.metadata_props}
    assert metadata['input_features'] == ', '.join(input_features)
    assert metadata['output_features'] == ', '.join(output_features)

def test_extract_features_from_onnx_with_real_model(real_onnx_model):
    input_features = ['feature1', 'feature2']
    output_features = ['output1']

    # First, add metadata to the ONNX model
    add_metadata_to_onnx(real_onnx_model, input_features=input_features, output_features=output_features)

    # Then extract the metadata using the function
    features = extract_features_from_onnx(real_onnx_model)

    # Verify the extracted features
    assert features['input_features'] == input_features
    assert features['output_features'] == output_features


# Test extract_features_from_onnx
def test_extract_features_from_onnx():
    mock_onnx_model = mock.MagicMock()
    
    mock_onnx_model.metadata_props = [
        onnx.StringStringEntryProto(key='input_features', value='feature1, feature2'),
        onnx.StringStringEntryProto(key='output_features', value='output1')
    ]
    
    with mock.patch('onnx.load', return_value=mock_onnx_model):
        features = extract_features_from_onnx("test_model.onnx")
        assert features == {
            'input_features': ['feature1', 'feature2'],
            'output_features': ['output1']
        }

def test_extract_features_from_onnx_fail():
    with mock.patch('onnx.load', side_effect=Exception("Load error")):
        with pytest.raises(ValueError, match="Failed to load ONNX model"):
            extract_features_from_onnx("invalid_model.onnx")

def test_save_model_to_onnx_with_real_model(real_catboost_model):
    model, onnx_path = real_catboost_model

    # Call the function to save the model in ONNX format
    save_model_to_onnx(model, onnx_path)

    # Ensure the ONNX file was created
    assert os.path.exists(onnx_path)

def test_add_metadata_to_onnx_with_trained_model(real_catboost_model):
    _, onnx_path = real_catboost_model

    input_features = ['feature1', 'feature2']
    output_features = ['output1']

    # Add metadata to the saved ONNX model
    add_metadata_to_onnx(onnx_path, input_features=input_features, output_features=output_features)

    # Load the ONNX model and check if the metadata was added correctly
    onnx_model = onnx.load(onnx_path)
    metadata = {prop.key: prop.value for prop in onnx_model.metadata_props}

    assert metadata['input_features'] == ', '.join(input_features)
    assert metadata['output_features'] == ', '.join(output_features)