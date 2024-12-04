import pytest
import os
from unittest.mock import patch, mock_open, MagicMock
from urllib.parse import urlparse
from eo_processing.utils.external_dependency_utilities import is_onnx_file, validate_onnx_file, download_file


# Test is_onnx_file
def test_is_onnx_file_valid():
    assert is_onnx_file("model.onnx") is True

def test_is_onnx_file_invalid():
    assert is_onnx_file("model.txt") is False

# Test validate_onnx_file
@patch("onnx.load")
def test_validate_onnx_file_valid(mock_onnx_load):
    mock_onnx_load.return_value = True
    assert validate_onnx_file("model.onnx") is True

@patch("onnx.load")
def test_validate_onnx_file_invalid(mock_onnx_load):
    mock_onnx_load.side_effect = Exception("Invalid ONNX file")
    assert validate_onnx_file("model.onnx") is False

# Test download_file
@patch("os.path.exists")
def test_download_file_already_exists(mock_exists):
    mock_exists.return_value = True
    expected_path = os.path.normpath("/tmp/cache/model.onnx")  # Normalize path for cross-platform compatibility
    result = download_file("http://example.com/model.onnx")
    assert os.path.normpath(result) == expected_path  # Ensure paths are compared in a normalized form

@patch("requests.get")
@patch("os.rename")
@patch("os.path.exists", side_effect=lambda path: False)
@patch("tempfile.NamedTemporaryFile")
def test_download_file_successful(mock_tempfile, mock_exists, mock_rename, mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content = MagicMock(return_value=[b"data"] * 10)
    mock_get.return_value = mock_response

    mock_temp_file = MagicMock()
    mock_tempfile.return_value = mock_temp_file
    mock_temp_file.name = os.path.normpath("/tmp/tempfile.onnx")  # Normalize temporary file path
    mock_temp_file.close = MagicMock()

    result = download_file("http://example.com/model.onnx")
    expected_path = os.path.normpath("/tmp/cache/model.onnx")
    assert os.path.normpath(result) == expected_path  # Normalize the result for comparison

