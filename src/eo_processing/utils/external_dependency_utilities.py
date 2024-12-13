#%%
import os
import requests
import tempfile
from openeo.udf import inspect
from urllib.parse import urlparse
import onnx


def is_onnx_file(file_path: str) -> bool:
    """
    Determines if the provided file path corresponds to an ONNX file.

    This function checks if the file path ends with the '.onnx' extension,
    which is the standard extension for ONNX files.

    :param file_path: The path of the file to check.
    :return: A boolean value indicating if the file path corresponds to
        an ONNX file (True) or not (False).
    """
    return file_path.endswith('.onnx')


def validate_onnx_file(file_path: str) -> bool:
    """
    Validates whether a given ONNX file is loadable and properly formatted.

    This function attempts to load the ONNX file specified by the provided
    file path. If the file can be successfully loaded, the function returns
    True, indicating that the ONNX file is valid. If the file fails to load
    due to an exception, the function returns False after logging a validation
    failure message.

    :param file_path: The path to the ONNX file to validate.
    :return: A boolean value indicating whether the ONNX file is valid.
    """
    try:
        onnx.load(file_path)  # Attempt to load the ONNX model
        return True
    except Exception as e:
        inspect(message=f"Validation failed for ONNX file {file_path}: {e}")
        return False

def download_file(url: str, max_file_size_mb: int = 100, cache_dir: str = '/tmp/cache') -> str:
    """
    Downloads a file from a given URL and stores it in a specified cache directory. If the file already exists
    in the cache, it is returned immediately. The function ensures that the file size does not exceed a
    specified limit. It also handles temporary file management during download and cleans up in case of
    errors. The downloaded file is moved from a temporary location to the specified cache directory upon
    successful completion.

    :param url: The URL of the file to be downloaded.
    :param max_file_size_mb: Maximum allowed file size in megabytes. Defaults to 100 MB.
    :param cache_dir: The directory where the downloaded file will be cached. Defaults to '/tmp/cache'.
    :return: The path of the downloaded file within the cache directory.
    """
    
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
