#%%
import os
import requests
import tempfile
from openeo.udf import inspect
from urllib.parse import urlparse
import onnx


def is_onnx_file(file_path: str) -> bool:
    """Check if the file is an ONNX model based on its extension."""
    return file_path.endswith('.onnx')

def validate_onnx_file(file_path: str) -> bool:
    """Validate if the given file is a valid ONNX model."""
    try:
        onnx.load(file_path)  # Attempt to load the ONNX model
        return True
    except Exception as e:
        inspect(message=f"Validation failed for ONNX file {file_path}: {e}")
        return False

def download_file(url: str, max_file_size_mb: int = 100, cache_dir: str = '/tmp/cache') -> str:
    """Download a file with concurrency protection and store it temporarily."""
    
    # Extract the file name from the URL (e.g., "model_1.onnx")
    file_name = os.path.basename(urlparse(url).path)
    
    # Construct the file path within the cache directory (e.g., '/tmp/cache/model.onnx')
    file_path = os.path.join(cache_dir, file_name)
    
    # Ensure the cache directory exists
    os.makedirs(cache_dir, exist_ok=True)

    # Check if the file already exists in the cache
    if os.path.exists(file_path):
        inspect(f"File {file_path} already exists in cache.")
        return file_path
    
    # Temporary file management
    temp_file_path = None
    try:
        # Download the file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".onnx") as temp_file:
            temp_file_path = temp_file.name  # Store the temporary file path
            print(f"Downloading file from {url}...")
            
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                file_size = 0
                for chunk in response.iter_content(chunk_size=1024):
                    temp_file.write(chunk)
                    file_size += len(chunk)
                    if file_size > max_file_size_mb * 1024 * 1024:
                        raise ValueError(f"Downloaded file exceeds the size limit of {max_file_size_mb} MB")

                inspect(f"Downloaded file to {temp_file_path}")
            else:
                raise ValueError(f"Failed to download file, status code: {response.status_code}")
        
        # After download is complete, move the file from temp to the final destination
        os.rename(temp_file_path, file_path)  # Move the file to final location
        return file_path  # Return path of the final model file

    except Exception as e:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)  # Clean up temporary file on error
        raise ValueError(f"Error downloading file: {e}")





