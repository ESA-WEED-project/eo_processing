#%%
import os
import requests
import tempfile
from openeo.udf import inspect
from filelock import FileLock
import shutil
import zipfile
import onnx

def is_zip_file(url: str) -> bool:
    """Check if the URL points to a ZIP file."""
    return url.lower().endswith('.zip')

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

def download_file_with_lock(url: str, max_file_size_mb: int = 100, cache_dir: str = '/tmp/cache') -> str:
    """Download a file with concurrency protection and store it temporarily."""
    file_name = os.path.basename(url)
    file_path = os.path.join(cache_dir, file_name)
    lock_path = file_path + '.download.lock'

    lock = FileLock(lock_path)
    
    with lock:
        # Check if the file already exists in the cache
        if os.path.exists(file_path):
            inspect(message=f"File {file_path} already exists in cache.")
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
                final_file_path = os.path.join(cache_dir, file_name)
                os.rename(temp_file_path, final_file_path)  # Move the file to final location

                return final_file_path  # Return path of the final model file

            else:
                raise ValueError(f"Failed to download file, status code: {response.status_code}")

        except Exception as e:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)  # Clean up temporary file on error
            raise ValueError(f"Error downloading file: {e}")


def extract_onnx_from_zip(zip_path: str) -> str:
    """Extract an ONNX model from a ZIP file and return its path."""
    temp_dir = tempfile.mkdtemp()
    try:
        inspect(message=f"Extracting ZIP file {zip_path} to {temp_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".onnx"):
                    onnx_model_path = os.path.join(root, file)
                    inspect(message=f"Found ONNX model at {onnx_model_path}")
                    return onnx_model_path
        
        raise FileNotFoundError("No ONNX model found in the ZIP archive.")
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise ValueError(f"Error extracting ONNX from ZIP: {e}")


def process_model_file_with_lock(url: str, cache_dir: str = '/tmp/cache') -> str:
    """
    Process a model file (ZIP or ONNX) with concurrency protection.
    Ensures only one workflow processes the same file at a time.
    """
    # Step 1: Download the file with locking
    downloaded_file_path = download_file_with_lock(url, cache_dir=cache_dir)
    
    # Step 2: Determine file type and process
    file_name = os.path.basename(downloaded_file_path)
    lock_path = os.path.join(cache_dir, file_name + '.process.lock')
    lock = FileLock(lock_path)

    with lock:
        try:
            if is_zip_file(downloaded_file_path):
                # Step 3: If it's a ZIP file, extract the ONNX model
                onnx_path = extract_onnx_from_zip(downloaded_file_path)
                os.remove(downloaded_file_path)  # Cleanup the ZIP file after extraction
                
                if is_onnx_file(onnx_path) and validate_onnx_file(onnx_path):
                    inspect(message=f"Extracted and validated ONNX model at {onnx_path}.")
                    return onnx_path
                else:
                    raise ValueError(f"File {onnx_path} is not a valid ONNX model.")
                
            elif is_onnx_file(downloaded_file_path) and validate_onnx_file(downloaded_file_path):
                inspect(message=f"File {downloaded_file_path} is a valid ONNX model.")
                return downloaded_file_path  # Return valid ONNX file directly
            else:
                raise ValueError(f"File {downloaded_file_path} is not a valid ONNX model.")
        except Exception as e:
            raise ValueError(f"Error processing model file: {e}")

    



