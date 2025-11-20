from __future__ import annotations
import os
from ast import literal_eval

import boto3
from botocore.exceptions import ClientError
from eo_processing.utils.dotenv_utils import set_dotenv_vars_from_dict, DOTENV
from eo_processing.utils.helper import string_to_dict
from getpass import getpass
import geopandas as gpd
from hashlib import md5
import hvac
from pydrive2.fs import GDriveFileSystem
from requests import auth, delete, post, put
import tempfile
import time
from tqdm import tqdm
from typing import Union, Dict, Tuple, List, TYPE_CHECKING, IO, Optional
import psycopg

if TYPE_CHECKING:
    from eo_processing.config.data_formats import (s3_credentials_format, sql_credentials_format,
                                                   gdrive_credentials_format, stac_credentials_format,
                                                   mlflow_credentials_format)
    import pystac

BUCKETS = {'WEED': ['ecdc', 'model', 'extent', 'test', 'ecdc-stac', 'extent-stac'],
           'sonata':['sonata','sonata-stac'],
           'obsgession':['obsgession','obsgession-stac']}
STAC_CATS = ['dev','prod']

class S3_storage:
    """
    Handles operations related to Amazon S3 storage, including initializing storage
    credentials, configuring a Boto3 client, retrieving and managing S3 resources, and
    facilitating transfers of objects between an S3 bucket and local directories.

    This class primarily focuses on interacting with an S3 bucket using Boto3, enabling
    users to fetch stored content, download files, and manage credentials within a
    controlled setup. The design supports error handling for misconfigured credentials
    and retries for failed operations while remaining flexible through parameterized
    inputs.
    """
    def __init__(self, s3_credentials: Optional[s3_credentials_format] = None):
        """
        Initializes the configuration for S3 storage credentials. If no credentials are provided,
        a default set of empty values is used, along with a warning message. Validates the input
        credentials if provided and raises an error for invalid formats. The S3 client is initialized
        as None to be configured later.

        :param s3_credentials: Dictionary containing S3 credentials with keys matching
                               the `s3_credentials_format` type. If not provided, defaults
                               to None and an uninitialized storage object will be created.

        :raises ValueError: If the provided S3 credentials do not match the expected format.
        """
        from eo_processing.config.data_formats import s3_credentials_format
        if s3_credentials is None:
            print('WARNING: no S3 credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.s3_credentials = {
                "AWS_ACCESS_KEY_ID": None,
                "AWS_SECRET_ACCESS_KEY": None,
                "s3_endpoint": None,
                "bucket_name": None,
                "export_workspace": None
            }
        elif ((isinstance(s3_credentials, dict))
              and (set(s3_credentials.keys()) == set(s3_credentials_format.__annotations__.keys()))):
            self.s3_credentials = {
                "AWS_ACCESS_KEY_ID": s3_credentials['s3_access_key'],
                "AWS_SECRET_ACCESS_KEY": s3_credentials['s3_secret_key'],
                "s3_endpoint": s3_credentials['s3_endpoint'],
                "bucket_name": s3_credentials['bucket_name'],
                "export_workspace": s3_credentials['export_workspace']
            }
        else:
            raise ValueError('The provided S3 credentials are not valid. Please check the documentation '
                             'for the correct format.')
        self.s3_client: Optional[boto3.client] = None
        self.s3_bucket = self.s3_credentials['bucket_name']
        self.export_workspace = self.s3_credentials['export_workspace']

    def _init_boto3(self) -> None:
        """
        Initializes a Boto3 session and S3 client.

        This method sets up a session using Boto3 and creates an S3 client with the
        provided credentials and endpoint. It utilizes the s3_credentials attribute to
        fetch necessary connection details, such as the service endpoint, access key
        ID, and secret access key.

        :raises KeyError: If any of the required keys ('s3_endpoint', 'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY') are missing in the `s3_credentials` attribute.

        :return: None
        """
        if any(value is None for value in self.s3_credentials.values()):
            raise KeyError('Missing required S3 credentials. Please ini storage object correctly.')

        # TODO: bugfix to overcome the boto3 checksum errors for upload and download
        from botocore.config import Config
        config = Config(
            request_checksum_calculation='WHEN_REQUIRED',
            response_checksum_validation='WHEN_REQUIRED')

        session = boto3.session.Session()
        try:
            self.s3_client = session.client(
                service_name='s3',
                config=config,
                endpoint_url=self.s3_credentials['s3_endpoint'],
                aws_access_key_id=self.s3_credentials['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=self.s3_credentials['AWS_SECRET_ACCESS_KEY']
            )
        except:
            raise Exception('Error connecting to S3: ' + str(self.s3_credentials['s3_endpoint']))

    def get_s3_client(self) -> boto3.client:
        """
        Provides access to an S3 client instance from the boto3 library. The S3 client
        is used to interact with Amazon S3 services for operations such as uploading,
        downloading, or managing AWS S3 resources. If the S3 client has not been
        initialized yet, the method will internally invoke a helper function to
        initialize it before returning the instance.

        :return: The S3 client instance initialized via boto3.
        """
        if self.s3_client is None:
            self._init_boto3()

        return self.s3_client

    def get_export_workspace(self) -> str:
        """
        Gets the export workspace directory as a string.

        This method retrieves the current export workspace directory, which is stored
        as part of the instance data. The export workspace is typically used to define
        the directory path where exported files are saved.

        :return: The export workspace directory as a string.
        """
        return self.export_workspace

    def get_s3_bucket_name(self) -> str:
        """
        Retrieve the name of the S3 bucket associated with the object.

        :return: The name of the S3 bucket as a string.
        """
        return self.s3_bucket

    def get_s3_content(self, s3_directory: str, recursive: bool = True) -> List:
        """
        Retrieves the contents of a specified S3 directory from an S3 bucket.

        This method interacts with the AWS S3 service using the Boto3 library to list
        objects within a given S3 directory. It allows for optional recursive listing,
        and leverages pagination to fetch all available objects when the number exceeds
        the maximum allowed by S3 in a single request.

        :param s3_directory: The prefix (directory) in the S3 bucket to list objects from.
        :param recursive: A boolean flag that determines if the listing is recursive.
            Defaults to True.
        :raises TypeError: If the s3_client or s3_bucket attribute has not been
            properly initialized.
        :returns: A dictionary containing details for the objects found in the
            specified directory.
        """
        if self.s3_client is None:
            self._init_boto3()

        # do some checks to get the s3_prefix right
        if not s3_directory:
            s3_prefix = ''
        else:
            s3_prefix = s3_directory if s3_directory.endswith('/') else f"{s3_directory}/"

        result = []
        continuation_token = None

        while True:
            list_kwargs = dict(
                MaxKeys=1000,
                Bucket=self.s3_bucket,
                Prefix=s3_prefix
            )
            if not recursive:
                list_kwargs['Delimiter'] = '/'

            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = self.s3_client.list_objects_v2(**list_kwargs)
            result.extend(response.get('Contents', []))

            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')

        return result

    def download_s3_content(self, s3_objects: str, out_dir: str, retry: int = 0, download_json: bool = False) -> None :
        """
        Downloads content from an CreoDIAS S3 bucket based on the provided prefix. This function retrieves
        all objects under the specified prefix from the S3 bucket, creates any necessary local directories,
        and downloads the objects to a local directory. It also supports retrying the download process
        a specified number of times in case of failure. Optionally, JSON files can be excluded from
        the download.

        :param s3_objects: The prefix path in the S3 bucket to retrieve objects from.
        :param out_dir: The local directory to which the retrieved S3 objects will be downloaded.
        :param retry: The current retry attempt count. Defaults to 0.
        :param download_json: Indicates whether JSON files should be downloaded. If False, such files
                              will be excluded from the download. Defaults to False.
        :return: None
        """
        if self.s3_client is None:
            self._init_boto3()

        # Create a reusable Paginator
        paginator = self.s3_client.get_paginator('list_objects')

        lst = []
        operation_parameters = {'Bucket': self.s3_bucket,
        'Prefix': s3_objects}
        # Create a PageIterator from the Paginator
        page_iterator = paginator.paginate(**operation_parameters)

        for page in page_iterator:
            for line in page['Contents']:
                try:
                    lst.append(line['Key'])
                except:
                    pass
        try:
            for element in lst:
                if element.endswith('/'):
                    continue
                elif element.endswith('.json') and not download_json:
                    continue
                else:
                    dirname = os.path.dirname(s3_objects)
                    # basename = os.path.basename(s3_objects)
                    outname = os.path.join(out_dir, os.path.relpath(element, dirname))
                    if os.path.exists(outname):
                        continue
                    os.makedirs(os.path.dirname(outname), exist_ok=True)

                    self.s3_client.download_file(self.s3_bucket, element, outname)
        except:
            if retry < 5:
                time.sleep(10)
                self.download_s3_content(s3_objects, out_dir, retry=retry+1, download_json=download_json)
            else:
                raise Exception('Copying data from S3 failed: ' + str(self.s3_bucket + '/' + s3_objects))

    def get_file_urls(self, s3_directory: str = 'models', extension: str = '.onnx',
                      recursive: bool = True) -> List[str]:
        """
        Generate a list of complete S3 file URLs based on specified filters.

        Constructs and returns URLs for files stored in an S3 bucket that match the
        specified directory, file extension, and recursion option.

        :param s3_directory: Directory within the S3 bucket to filter files from.
            Defaults to 'models'.
        :param extension: File extension to filter by. Defaults to '.onnx'.
        :param recursive: Flag indicating whether to include files from subdirectories. If True, the method
            will recursively search inside subdirectories. Default is True
        :return: A list of file URLs matching the specified filters.
        """
        # get all filtered file_keys
        file_keys = self.get_file_keys(s3_directory, extension, recursive=recursive)

        # define the base URL to the specified S3 Model storage
        base_url = f"{self.s3_credentials['s3_endpoint']}/swift/v1/{self.s3_bucket}/"

        return [f"{base_url}{element}" for element in file_keys]

    def get_file_keys(self, s3_directory: str = 'results', extension: str = '.tif',
                      recursive: bool = True) -> List[str]:
        """
        Retrieve a list of file keys from a specified S3 directory filtered by file extension.

        This method interacts with an S3 bucket to retrieve the keys (file paths) for all objects in a specific directory.
        You can filter the results by file extension and also choose to include subdirectories in the search.

        :param s3_directory: The target directory in the S3 bucket from which to retrieve file keys.
            By default, this is set to 'results'.
        :param extension: The file extension to filter the keys. Only objects with keys that end with this
            extension will be included in the result. Default is '.tif'.
        :param recursive: Flag indicating whether to include files from subdirectories. If True, the method
            will recursively search inside subdirectories. Default is True.
        :return: A list of strings representing the keys of files in the specified S3 folder that match
            the given extension.
        """
        # get all content of the model subfolder in the bucket
        response = self.get_s3_content(s3_directory, recursive=recursive)

        if not response:
            print('No files found in the selected folder with the given extension.')
            return []

        return [obj['Key'] for obj in response if obj['Key'].endswith(extension)]

    def convert_file_key_2_url(self, s3_object_key: str) -> str:
        """
        Converts an S3 object key into a complete URL using the provided S3 endpoint,
        bucket, and object key. This method validates that the input is a string and
        checks the existence of the object in the S3 bucket before constructing the URL.

        :param s3_object_key: The unique identifier for the object stored in the S3 bucket.
        :return: A string representing the complete URL of the S3 object.
        :raises TypeError: If the provided `s3_object_key` is not a string.
        :raises Exception: If the file with the given `s3_object_key` does not exist
                           in the specified S3 bucket.
        """
        if not isinstance(s3_object_key, str):
            raise TypeError("s3_object_key must be a string. One single s3_file_key.")

        if not self.s3_object_exists(s3_object_key):
            raise Exception(f"File with key {s3_object_key} does not exist in S3 bucket {self.s3_bucket}.")

        # define the base URL to the specified S3 Model storage
        base_url = f"{self.s3_credentials['s3_endpoint']}/swift/v1/{self.s3_bucket}/"

        return f"{base_url}{s3_object_key}"

    def download_file_key(self, s3_object_key: str, temp_folder: str,
                          progress_bar: bool = False, etag_check: bool = False, exist_check: bool = False) -> str:
        """
        Downloads a file from an S3 bucket using the given `s3_object_key` and saves it to the specified
        temporary folder. Optionally, it can show a progress bar, perform an ETag check to ensure data
        integrity, and check if the file already exists to skip download.

        The purpose of this method is to provide an efficient and secure way to download an S3 object,
        along with options for verifying file integrity and handling existing files.

        :param s3_object_key: The key of the S3 object to be downloaded. Must be a string.
        :param temp_folder: The temporary folder where the file will be saved.
        :param progress_bar: Whether to display a progress bar during the download. Defaults to False.
        :param etag_check: Whether to perform an ETag validation check after downloading. Defaults to False.
        :param exist_check: Whether to skip the download if the file already exists locally. Defaults to False.
        :raises TypeError: If `s3_object_key` is not a string.
        :raises Exception: If the S3 object does not exist or if the ETag check fails.
        :raises FileExistsError: If an error occurs during the file download process.
        :returns: The local file path where the S3 object was saved.
        """
        if self.s3_client is None:
            self._init_boto3()

        if not isinstance(s3_object_key, str):
            raise TypeError("s3_object_key must be a string. One single s3_file_key.")

        if not self.s3_object_exists(s3_object_key):
            raise Exception(f"File with key {s3_object_key} does not exist in S3 bucket {self.s3_bucket}.")

        os.makedirs(os.path.normpath(temp_folder), exist_ok=True)

        # Local path to save the file
        local_file_path = os.path.join(os.path.normpath(temp_folder), os.path.basename(s3_object_key))

        # check if download is already existing
        if exist_check:
            if os.path.exists(local_file_path):
                print('File already exists. Skipping download.')
                if etag_check:
                    if not self.evaluate_etag(local_file_path, s3_object_key):
                        raise Exception('The downloaded file does not match the ETag of the S3 object.')

                return local_file_path
        # download file
        try:
            if progress_bar:
                total_size = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_object_key)['ContentLength']
                self.s3_client.download_file(
                    self.s3_bucket, s3_object_key, local_file_path,
                    Callback=ProgressPercentage(s3_object_key, total_size)
                )
            else:
                self.s3_client.download_file(self.s3_bucket, s3_object_key, local_file_path)
        except Exception as e:
            raise FileExistsError(f"Error downloading file {s3_object_key}: {e}")

        if etag_check:
            if not self.evaluate_etag(local_file_path, s3_object_key):
                raise Exception('The downloaded file does not match the ETag of the S3 object.')

        return local_file_path

    def upload_file_to_s3(self, local_file_path: str, s3_prefix: str = '',
                          progress_bar: bool = False, etag_check: bool = False, exist_check: bool = False) -> str:
        """
        Uploads a local file to an S3 bucket. The method checks for the file's existence locally
        before attempting to upload to the specified S3 prefix. Supports options to display a
        progress bar during upload, check for existence of the file on S3, and validate the
        uploaded file using an ETag verification mechanism.

        :param local_file_path: A string representing the local path of the file to be uploaded.
            Must be a valid path to an existing file.
        :param s3_prefix: A string representing the prefix/directory on the S3 bucket where
            the file should be uploaded. Defaults to an empty string.
        :param progress_bar: A boolean indicating whether to display the progress bar during
            the upload process. Defaults to False.
        :param etag_check: A boolean indicating whether to perform an ETag checksum comparison
            between the uploaded file and the local file to validate data integrity. Defaults to False.
        :param exist_check: A boolean indicating whether to skip the upload if the file already exists
            on the S3 bucket with the same key. Defaults to False.

        :return: A string representing the S3 object key of the uploaded file.
        """
        if self.s3_client is None:
            self._init_boto3()

        if not os.path.exists(os.path.normpath(local_file_path)):
            raise FileNotFoundError(f"File {local_file_path} does not exist.")

        s3_object_key = f"{s3_prefix}/{os.path.basename(local_file_path)}".strip('/')

        # check if we can avoid upload
        if exist_check:
            if self.s3_object_exists(s3_object_key):
                print(f"File with key {s3_object_key} already exists. Skipping upload.")
                if etag_check:
                    if not self.evaluate_etag(local_file_path, s3_object_key):
                        raise Exception('The uploaded file does not match the MDF5 of the local file.')
                return s3_object_key
        # upload file
        try:
            if progress_bar:
                total_size = os.path.getsize(os.path.normpath(local_file_path))
                self.s3_client.upload_file(
                    local_file_path, self.s3_bucket, s3_object_key,
                    Callback=ProgressPercentage(s3_object_key, total_size, 'uploading')
                )
            else:
                self.s3_client.upload_file(local_file_path, self.s3_bucket, s3_object_key)
        except Exception as e:
            print(f"Error uploading file to S3: {e}")

        if etag_check:
            if not self.evaluate_etag(local_file_path, s3_object_key):
                raise Exception('The uploaded file does not match the MDF5 of the local file.')

        return s3_object_key

    def upload_directory_to_s3(self, local_dir_path: str, s3_prefix: str = '',
                               progress_bar: bool = False, etag_check: bool = False, exist_check: bool = False) -> List[str]:
        """
        :param local_dir_path: A string representing the local path of the directory to be uploaded.
            Must be a valid path to an existing file.
        :param s3_prefix: A string representing the prefix/directory on the S3 bucket where
            the files of the local directory should be uploaded folowwing the relative path. Defaults to an empty string.
        :param progress_bar: A boolean indicating whether to display the progress bar during
            the upload process (per file). Defaults to False.
        :param etag_check: A boolean indicating whether to perform an ETag checksum comparison
            between the uploaded file and the local file to validate data integrity. Defaults to False.
        :param exist_check: A boolean indicating whether to skip the upload if the file in a directory already exists
            on the S3 bucket with the same key. Defaults to False.

        :return: A List of strings representing the S3 object keys of the uploaded files.
        """
        if not os.path.isdir(local_dir_path):
            raise NotADirectoryError(f"{local_dir_path} is not a directory or does not exist.")

        s3_object_keys = []
        for root, dirs, files in os.walk(local_dir_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, start=local_dir_path)
                s3_object_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

                # Call the existing method for each file
                s3_object_key = self.upload_file_to_s3(
                    local_file_path=local_file_path,
                    s3_prefix=os.path.dirname(s3_object_key),
                    progress_bar=progress_bar,
                    etag_check=etag_check,
                    exist_check=exist_check
                )
                s3_object_keys.append(s3_object_key)

        return s3_object_keys

    def s3_object_exists(self, s3_object_key: str) -> bool:
        """
        Checks if an S3 object exists in the specified bucket. This function determines
        whether a specific object key is present by performing a metadata lookup through
        the head_object method. It handles scenarios where the object does not exist or
        when other errors occur, such as access denial.

        :param s3_object_key: The key of the object to check in the S3 bucket.
        :return: True if the object exists, False if it does not.
        :raises Exception: If an error occurs other than the object not existing, such as access
        denial or bucket misconfiguration.
        """
        if self.s3_client is None:
            self._init_boto3()

        try:
            # Check object metadata using head_object
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_object_key)
            return True  # Key exists
        except ClientError as e:
            # Object does not exist (or another error has occurred)
            if e.response['Error']['Code'] == '404':
                return False  # Key does not exist
            else:
                # Raise exception for other errors (e.g., access denied or bucket not found)
                raise Exception(f"Failed to check if object exists: {e}")

    def s3_bucket_exists(self) -> bool:
        """
        Checks if an S3 bucket exists. This function determines
        whether the S3 bucket is present by performing a metadata lookup through
        the head_bucket method. It handles scenarios where the bucket does not exist or
        when other errors occur, such as access denial.

        :return: True if the head_bucket exists, False if it does not (or temporal unavailability).
        :raises Exception: If an error occurs other than the object not existing, such as access
        denial or bucket misconfiguration.
        """

        if self.s3_client is None:
            self._init_boto3()

        try:
            # Check object metadata using head_object
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            return True  # Key exists
        except ClientError as e:
            # Object does not exist (or another error has occurred)
            if e.response['Error']['Code'] == '404':
                return False  # Key does not exist
            else:
                # Raise exception for other errors (e.g., access denied or bucket not found)
                raise Exception(f"Failed to check if bucket exists: {e}")


    def delete_file_key(self, s3_object_key: str) -> bool:
        """
        Deletes a file from an S3 bucket with the specified object key.

        This method initializes the S3 client if it is not already initialized, validates
        the type of the `s3_object_key` parameter, and attempts to delete the object
        associated with the provided key from the S3 bucket. Errors during the
        deletion process are logged to the console.

        :param s3_object_key: The key (identifier) of the file in the S3 bucket that is
                              targeted for deletion.
        """
        if not isinstance(s3_object_key, str):
            raise TypeError("s3_object_key must be a string. One single s3_file_key.")

        if self.s3_object_exists(s3_object_key):
            try:
                self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_object_key)
                return True
            except Exception as e:
                print(f"Error deleting file from S3: {e}")
                return False
        else:
            print(f"File with key {s3_object_key} does not exist in S3 bucket {self.s3_bucket}.")
            return False

    def get_etag(self, s3_object_key: str) -> str:
        """
        Retrieves the ETag of an S3 object specified by the given object key. The ETag
        is typically used to verify the integrity of the object.

        This method initializes the boto3 S3 client if it has not been already and
        validates that the parameter `s3_object_key` is of type `str`. If the key is
        not a string (e.g., it is a list or dictionary), a `TypeError` will be raised.
        The function fetches the metadata of the object from the S3 bucket and extracts
        the ETag value after removing any surrounding double quotes.

        :param s3_object_key: The key of the object in the S3 bucket whose ETag needs
            to be retrieved. Must be a string.
        :return: The ETag of the specified S3 object as a string.
        :raises TypeError: If `s3_object_key` is not a string or is of an unsupported
            data type (e.g., list or dictionary).
        """
        if self.s3_client is None:
            self._init_boto3()

        if not isinstance(s3_object_key, str):
            raise TypeError("s3_object_key must be a string. One single s3_file_key.")

        s3_resp = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_object_key)
        return s3_resp['ETag'].strip('"')

    def evaluate_etag(self, local_file_path: str, s3_object_key: str) -> bool:
        """
        Evaluates whether the ETag of a local file matches the ETag of a corresponding S3 object.
        The ETag comparison is performed by calculating potential multipart ETags using different part sizes
        and comparing them to the ETag retrieved from S3. If a matching ETag is found, it returns True;
        otherwise, it returns False.

        :param local_file_path: The file path of the local file to be compared.
        :param s3_object_key: The object key of the file in S3 to retrieve its ETag.
        :return: True if the calculated ETag of the local file matches the S3 ETag, False otherwise.

        :raises ValueError: If the S3 ETag cannot be split into valid multipart format.
        :raises FileNotFoundError: If the specified local file path does not exist.
        :raises OSError: If there is an issue accessing the local file to determine its size.
        """
        # get etag of s3_file
        s3_etag = self.get_etag(s3_object_key)

        # get file size of local file
        filesize = os.path.getsize(os.path.normpath(local_file_path))

        # check if we have a chunked S3 file
        try:
            num_parts = int(s3_etag.split('-')[1])
        except IndexError:
            num_parts = 1

        # run etag generation for loacal file and comparison
        if num_parts == 1:
            if s3_etag == calculate_md5(os.path.normpath(local_file_path)):
                return True
        else:
            partsizes = [  ## Default Partsizes Map
                8388608,  # aws_cli/boto3
                15728640,  # s3cmd
                _factor_of_1MB(filesize, num_parts)  # Used by many clients to upload large files
            ]

            for partsize in filter(_possible_partsizes(filesize, num_parts), partsizes):
                if s3_etag == _calc_etag(os.path.normpath(local_file_path), partsize):
                    return True

        return False
    
class MLFlow_storage:
    """
    Handles operations related to ML Flow storage:initializing storage
    credentials.

    This class primarily focuses on interacting with the  ML Flow through environment variables.
    """
    def __init__(self, mflow_credentials: Optional[mlflow_credentials_format] = None):
        """
        Initialize MLflow storage configuration and load tracking credentials:

        - If no credentials are provided *and* no environment variables are available (DOTENV=False),
        a warning is issued and an uninitialized (None-filled) credential set is created.
        - If a credentials dictionary is provided and matches the expected
        `mlflow_credentials_format` schema, the values are validated and assigned.
        - If a credentials dictionary is not provided but environment variables exist (DOTENV=True),
        credentials are loaded from the environment.
        - Otherwise, a ValueError is raised for invalid or mismatched credential formats.

        :param mlflow_credentials: Dictionary containing ML Flow credentials with keys matching
                               the `mlflow_credentials_format` type. If not provided, defaults
                               to None and an uninitialized storage object will be created.

        :raises ValueError: If the provided ML FLow credentials do not match the expected format.
        """
        from eo_processing.config.data_formats import mlflow_credentials_format
        if mflow_credentials is None and not DOTENV:
            print('WARNING: no ML Flow credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.mflow_credentials = {
                "MLFLOW_TRACKING_USERNAME": None,
                "MLFLOW_TRACKING_PASSWORD": None,
                "MLFLOW_TRACKING_URI": None,
            }
        elif ((isinstance(mflow_credentials, dict))
              and (set(mflow_credentials.keys()) == set(mlflow_credentials_format.__annotations__.keys()))):
            self.mflow_credentials = {
                "MLFLOW_TRACKING_USERNAME": mflow_credentials['MLFLOW_TRACKING_URI'],
                "MLFLOW_TRACKING_PASSWORD": mflow_credentials['MLFLOW_TRACKING_PASSWORD'],
                "MLFLOW_TRACKING_URI": mflow_credentials['MLFLOW_TRACKING_URI'],
            }
        elif (not((isinstance(mflow_credentials, dict))
              and (set(mflow_credentials.keys()) == set(mlflow_credentials_format.__annotations__.keys())))
              ) and DOTENV:
            self.mflow_credentials = {
                "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI"),
                "MLFLOW_TRACKING_USERNAME": os.getenv("MLFLOW_TRACKING_USERNAME"),
                "MLFLOW_TRACKING_PASSWORD": os.getenv("MLFLOW_TRACKING_PASSWORD"),
            }
        else:
            raise ValueError('The provided ML Flow credentials are not valid. Please check the documentation '
                             'for the correct format.')
        

class SQL_storage:
    """
    Handles SQL database storage interaction.

    This class provides functionalities to initialize SQL storage credentials,
    connect to the database, execute SQL queries, retrieve or update data, and
    perform bulk inserts. The class ensures that all SQL operations are conducted
    with proper connection management and error handling.

    Attributes:
        sql_credentials: Dictionary containing SQL credentials required for
                         database connection. If no credentials are provided,
                         defaults to None, and initializes with placeholders.
        hadoop: Boolean flag for determining usage of Hadoop host in database
                connections. Defaults to False.
    """
    def __init__(self, sql_credentials: Optional[sql_credentials_format] = None):
        """
        Initializes an object with SQL credentials for database connection. If no credentials are
        provided, default values are set without initializing the storage object. Validates the
        given input against the expected SQL credentials format.

        :param sql_credentials: A dictionary containing SQL credentials with keys matching the
            sql_credentials_format. If not provided or invalid, default values are used or an error
            is raised.
        """
        from eo_processing.config.data_formats import sql_credentials_format
        if sql_credentials is None:
            print('WARNING: no sql credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.sql_credentials = {
                "dbname": None,
                "schema": None,
                "password": None,
                "host": None,
                "host_hadoop": None,
                "port": None
            }
        elif ((isinstance(sql_credentials, dict))
              and (set(sql_credentials.keys()) == set(sql_credentials_format.__annotations__.keys()))):
            self.sql_credentials = {
                "dbname": sql_credentials['dbname'],
                "schema": sql_credentials['schema'],
                "password": sql_credentials['password'],
                "host": sql_credentials['host'],
                "port": sql_credentials['port']
            }
        else:
            raise ValueError('The provided sql credentials are not valid. Please check the documentation '
                             'for the correct format.')

        #at the moment we initialize with hadoop false. Not sure if it's need to have?
        self.hadoop = False

    def create_connection(self) -> psycopg.Connection:
        """
        Establishes a connection to a PostgreSQL database using the provided credentials. 
        Supports conditional connection to Hadoop-specific host if specified.

        :param self: The instance of the class containing the method. 

        :raises psycopg.Error: If unable to establish a connection to the database.

        :return: A connection object representing the database connection.
        """
        try:
            # get connection to server
            print("** Establish connection to the database ...")
            if self.hadoop:
                host = self.sql_credentials['host_hadoop']
            else :
                host = self.sql_credentials['host']

            conn = psycopg.connect(dbname=self.sql_credentials['dbname'],
                                   user=self.sql_credentials['schema'],
                                   password=self.sql_credentials['password'],
                                   host=host,
                                   port=self.sql_credentials['port'])


        except psycopg.Error as e:
            print(e.pgerror)
            print(e.pgcode)
            print('-----Could not establish connection to the PostGREsql server...')
            raise

        return conn

    def GenericQueryWithResult(self, sql_statement: str) -> List[tuple]:
        """
        Executes a given SQL statement on the database and returns the fetched results. 

        The function establishes a connection to the database, executes the provided 
        SQL statement using a cursor, and retrieves the resulting data. If an error occurs 
        during execution, it handles the error by printing error codes/messages, performing 
        rollback if possible, and then re-raises the error. Finally, it ensures the proper 
        cleanup of database resources such as closing the cursor and connection.

        :param sql_statement: The SQL query string to be executed.
        :return: A list of tuples containing the query result.
        """
        conn = self.create_connection()

        # now we work in the database
        try:
            print('** get data from request')
            # create cursor
            cur = conn.cursor()
            cur.execute(sql_statement)
            # get data
            vResult = cur.fetchall()
            # close cursor
            cur.close()
        except psycopg.Error as e:
            print(e.pgerror)
            print(e.pgcode)
            # excecute a rollback when the error didn't closed the connection
            try:
                conn.rollback()
            except:
                print('-----No RollBack possible or not needed!')
            if cur.closed == False: cur.close()
            print('** Could not get the data from the table... check error message')
            raise
        finally:
            # close connection
            if conn.closed == 0: conn.close()
        return (vResult)

    def GenericQueryWithOUTResult(self, sql_statement: str) -> None:
        """
        Executes a SQL statement on the database without returning a result. This method commits the
        changes to the database if the execution is successful. In case of an error, it performs a rollback 
        if possible and raises the encountered error.

        :param sql_statement: The SQL statement to be executed.
        """
        conn = self.create_connection()

        # now we work in the database
        try:
            print('** execute statement')
            # create cursor
            cur = conn.cursor()
            cur.execute(sql_statement)
            conn.commit()
            # close cursor
            cur.close()
        except psycopg.Error as e:
            print(e.pgerror)
            print(e.pgcode)
            # excecute a rollback when the error didn't closed the connection
            try:
                conn.rollback()
            except:
                print('-----No RollBack possible or not needed!')
            if cur.closed == False: cur.close()
            print('** Could not execute the sql statement correctly... check error message')
            raise
        finally:
            # close connection
            if conn.closed == 0: conn.close()

    def BulkInsert(self, vTable: str, data: IO, lColumns: List[str]) -> None:
        """
        Executes a bulk insert operation into the specified database table.

        This method inserts data into a specified table in the database using a bulk
        insert operation. It takes the table name, the data to be inserted, and the 
        corresponding column names. Errors during the insert operation are handled 
        by rolling back the transaction and providing error details.

        :param vTable: The name of the database table where data will be inserted.
        :param data: A file-like object (e.g., an open file) containing the data to
            be inserted in a format acceptable by the database.
        :param lColumns: A list of column names in the database table that corresponds
            to the data being inserted.
        """
        conn = self.create_connection()

        # now we work in the database
        try:
            print('**** execute bulk insert')
            # create cursor
            cur = conn.cursor()
            cur.copy_from(data, vTable, null='nan', columns=lColumns)

            conn.commit()
            # close cursor
            cur.close()
        except psycopg.Error as e:
            print(e.pgerror)
            print(e.pgcode)
            # excecute a rollback when the error didn't closed the connection
            try:
                conn.rollback()
            except:
                print('-----No RollBack possible or not needed!')
            if cur.closed == False: cur.close()
            print('**** Could not execute the bulk insert correctly... check error message')
            raise
        finally:
            # close connection
            if conn.closed == 0: conn.close()

    def QueryItems(self, table: str, lcolumns: List[str]) -> List[tuple]:
        """
        Query items from a specified database table.

        This method retrieves data from a PostgreSQL database table based on the specified
        columns. It supports fetching data in batches using a server-side cursor to manage
        large datasets efficiently. If an error occurs during the query process, it attempts
        to rollback the database connection.

        :param table: Name of the database table from which data will be queried.
        :param lcolumns: List of column names to include in the query. Must contain at least one column.
        :raises ValueError: If no columns are specified in the input list.
        :raises psycopg.Error: If an error occurs during the execution of the database query.
        :return: Retrieved data from the query as a list of tuples.
        """
        # pre-check if columns are requested
        if len(lcolumns) == 0:
            print('No columns for the query are specified...')
            raise
        # ini connection
        conn = self.create_connection()

        lresults = []
        ## work in the postgresql database
        try:
            # create cursor
            cur = conn.cursor('server_cursor')
            query_time = time.time()
            # create the SQL query statement
            if len(lcolumns) == 1:
                sql_statement = f"SELECT {lcolumns[0]} FROM {table};"
            else:
                sql_statement = f"SELECT {','.join(lcolumns)} FROM {table};"

            print("** Downloading data from the database ...")
            cur.execute(sql_statement)

            # fetch data in batches using the server side cursor
            while True:
                rows = cur.fetchmany(500000)
                if not rows: break
                lresults.extend(rows)

            # close cursor
            cur.close()
            print('** No errors - all data successfully retrieved from database. (' + "{:10.4f}".format(
                time.time() - query_time) + ' sec)')

        except psycopg.Error as e:
            print(e.pgerror)
            print(e.pgcode)
            # excecute a rollback when the error didn't closed the connection
            try:
                conn.rollback()
            except:
                print('No RollBack possible or not needed!')
            if cur.closed == False: cur.close()
            print('** Could not get the data from the table... check error message')
            raise

        finally:
            # close connection
            if conn.closed == 0: conn.close()

        return lresults

    def StatusUpdateTiles(self, table: str, tileid: int, lcolumns: List[str], lmsg: List[str]) -> bool:
        """
        Updates specific columns in a particular database table for a given tile ID with new values.

        The method executes an UPDATE SQL command to modify the entries in the specified table. Each given
        column in `lcolumns` is updated with corresponding values from `lmsg` where the `tile_id` matches
        the provided `tileid`. The operation ensures proper transaction handling, with commit/rollback
        mechanisms in case of success or failure. Logs possible errors during execution and ensures
        database connection cleanup.

        :param table: The name of the database table to be updated.
        :param tileid: The identifier of the tile for which data is to be updated.
        :param lcolumns: A list of column names that need to be updated.
        :param lmsg: A list of new values corresponding to the specified columns.
        :return: True if the update operation completes successfully, otherwise False.
        """
        # establish connection to data base
        # ini connection
        conn = self.create_connection()

        # set all following in a try loop so if even the pre-processing fails then the connection is closed and rolled back
        try:
            # create cursor
            cur = conn.cursor()
            # prepare UPDATE statement
            print('** update the tile status...')
            for i in range(0, len(lcolumns)):
                sql_statement = "UPDATE %s SET %s = %%s WHERE tile_id = %%s;" % (table, lcolumns[i])
                cur.execute(sql_statement, (lmsg[i], tileid))

            # commit transactions
            conn.commit()
            # close cursor
            cur.close()

        except psycopg.Error as e:
            print("** Could not update the data in the PostgreSQL database - error...")
            print(e.pgerror)
            print(e.pgcode)
            # excecute a rollback when the error didn't closed the connection
            try:
                conn.rollback()
            except:
                print('No RollBack possible or not needed!')

            if cur.closed == False: cur.close()
            return False

        finally:
            # close connection
            if conn.closed == 0: conn.close()

        print('** No errors - all data successfully updated.')
        return True

class gdrive_storage:
    """
    Handles operations with Google Drive storage, including initialization
    using service account credentials, accessing files, and reading them into
    GeoDataFrames. Provides methods for overviews and downloading files.

    This class is designed for interacting with a Google Drive file system
    using a service account. It encapsulates functionalities such as
    initializing credentials, accessing files, and filtering geospatial data.
    Requirements for credentials and proper usage are validated during
    initialization. Provides a convenient interface for accessing and managing
    Google Drive resources.
    """
    def __init__(self, gdrive_entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu", 
                 gdrive_credentials: Optional[gdrive_credentials_format] = None):
        """
        Initializes a class instance with provided or default Google Drive credentials, and sets up 
        the required parameters for accessing Google Drive storage. Validations are performed to ensure 
        the provided credentials meet the expected format. If no credentials are provided, default 
        values are used instead, and a warning is issued. Throws a ValueError when invalid credentials 
        are passed.

        :param gdrive_entry_point: Entry point for the Google Drive system, defined as a string. 
                                    Defaults to "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu".
        :param gdrive_credentials: Optional dictionary containing Google Drive credentials. Should match 
                                    the structure defined in gdrive_credentials_format.

        :raises ValueError: If gdrive_credentials is provided and does not match the expected format.

        :attribute gdrive_fs: Optional attribute that represents the Google Drive file system object, 
                              initialized to None by default.
        :attribute gdrive_credentials: Dictionary that stores the Google Drive credentials. If not 
                                        provided, default placeholder values are used, with all 
                                        keys being set to None.
        :attribute gdrive_entry_point: Attribute storing the root entry point for accessing the 
                                        Google Drive system.
        """
        from eo_processing.config.data_formats import gdrive_credentials_format

        self.gdrive_fs: Optional[GDriveFileSystem] = None

        if gdrive_credentials is None:
            print('WARNING: no gdrive credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.gdrive_credentials = {
                "type": None,
                "project_id": None,
                "private_key_id": None,
                "private_key": None,
                "client_email": None,
                "client_id": None,
                "auth_uri": None,
                "token_uri": None,
                "auth_provider_x509_cert_url": None,
                "universe_domain": None

            }

        elif ((isinstance(gdrive_credentials, dict))
              and (set(gdrive_credentials.keys()) == set(gdrive_credentials_format.__annotations__.keys()))):
            self.gdrive_credentials = {
                "type": gdrive_credentials['type'],
                "project_id": gdrive_credentials['project_id'],
                "private_key_id": gdrive_credentials['private_key_id'],
                "private_key": gdrive_credentials['private_key'],
                "client_email": gdrive_credentials['client_email'],
                "client_id": gdrive_credentials['client_id'],
                "auth_uri": gdrive_credentials['auth_uri'],
                "token_uri": gdrive_credentials['token_uri'],
                "auth_provider_x509_cert_url": gdrive_credentials['auth_provider_x509_cert_url'],
                "universe_domain": gdrive_credentials['universe_domain']
            }

        else:
            raise ValueError('The provided gdrive credentials are not valid. Please check the documentation '
                             'for the correct format.')

        self.gdrive_entry_point=gdrive_entry_point

    def _init_GDRIVE(self) -> None:
        """
        Initializes the GDriveFileSystem for accessing files and folders using
        service account credentials. This method configures the file system
        client specifically for Google Drive and uses the provided service
        account credentials and entry point.

        :raises GDriveFileSystemError: If the initialization fails or the service
                                       account credentials are invalid.
        """
        # return the fsspec filesystem to access the files & folders available for the service account credentials
        try:
            self.gdrive_fs = GDriveFileSystem(self.gdrive_entry_point, use_service_account=True,
                                              client_json=self.gdrive_credentials)
        except:
            raise Exception('Could not initialize GDriveFileSystem.')

    def print_gdrive_overview(self) -> None:
        """
        Prints an overview of the Google Drive filesystem structure starting from
        the root directory. It iterates through each directory and subdirectory,
        listing all files found.

        This function walks through the file system using the provided GDriveFileSystem
        instance, and prints the name of each directory and files contained therein.

        :return: None
        """
        if self.gdrive_fs is None:
            self._init_GDRIVE()

        for dirName, subdirList, fileList in self.gdrive_fs.walk(self.gdrive_fs.root):
            print('Found directory: %s' % dirName)
            for fname in fileList:
                print('\t%s' % fname)

    def get_gdrive_gdf(self, gdrive_path: str,
                       filter_bbox: Union[Tuple, gpd.GeoDataFrame, None] = None) -> gpd.GeoDataFrame:
        """
        Downloads a file from Google Drive and reads it into a GeoDataFrame, with an optional bounding box filter.

        This function creates a temporary directory to store the downloaded file, which is automatically deleted
        after the function execution. It can also filter the GeoDataFrame with the provided bounding box.


        :param gdrive_path: Path to the file on Google Drive
        :param filter_bbox: Optional bounding box to filter the GeoDataFrame, could be a tuple or GeoDataFrame
        :return: A GeoDataFrame containing the data from the downloaded file, optionally filtered by the bounding box
        """
        if self.gdrive_fs is None:
            self._init_GDRIVE()

        # Create a temporary directory that will be automatically deleted
        with tempfile.TemporaryDirectory() as temp_dir:
            self.gdrive_fs.download(f'{self.gdrive_fs.root}/{gdrive_path}', os.path.join(temp_dir, 'temp.gpkg'))

            if filter_bbox:
                return gpd.read_file(os.path.join(temp_dir, 'temp.gpkg'), bbox=filter_bbox)
            else:
                return gpd.read_file(os.path.join(temp_dir, 'temp.gpkg'))

class stac_storage:
    """
    StacStorage class provides an interface to manage STAC collections and items in a catalog.

    The class handles authentication, interacting with a STAC catalog, adding, editing, or deleting 
    collections and items, and checking the validity of provided credentials. It ensures that operations 
    on the catalog are properly authenticated and offers methods to simplify catalog interaction for users.

    Attributes:
        stac_credentials (Optional[stac_credentials_format]): A dictionary containing the required credentials
        to access the STAC catalog. This includes CLIENT_ID, CLIENT_SECRET, TOKEN_URL, and catalog_url.

    Raises:
        ValueError: Raised if the provided stac_credentials do not match the expected format.
    """

    def __init__(self,
                 stac_credentials: Optional[stac_credentials_format] = None) -> None:
        """
        Initializes an object with STAC (SpatioTemporal Asset Catalog) credentials. If no
        credentials are provided, default empty values will be set, and the storage object
        will not be initialized. If credentials are provided, their format is validated to
        ensure compatibility.

        :param stac_credentials: Optional dictionary containing STAC credentials. The dictionary keys
                                 must match the keys defined in `stac_credentials_format` annotation.
                                 If not provided, default values with None will be assigned.
        :raises ValueError: If the provided `stac_credentials` do not match the expected format
                            defined in `stac_credentials_format`.
        """
        from eo_processing.config.data_formats import stac_credentials_format

        if stac_credentials is None:
            print('WARNING: no stac credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.stac_credentials = {
                "CLIENT_ID": None,
                "CLIENT_SECRET": None,
                "TOKEN_URL": None,
                "catalog_url": None,
            }

        elif ((isinstance(stac_credentials, dict))
              and (set(stac_credentials.keys()) == set(stac_credentials_format.__annotations__.keys()))):
            self.stac_credentials = {
                "CLIENT_ID": stac_credentials['CLIENT_ID'],
                "CLIENT_SECRET": stac_credentials['CLIENT_SECRET'],
                "TOKEN_URL": stac_credentials['TOKEN_URL'],
                "catalog_url": stac_credentials['catalog_url']

            }

        else:
            raise ValueError('The provided stac credentials are not valid. Please check the documentation '
                             'for the correct format.')

    def get_catalog_url(self) -> Optional[str]:
        """
        Returns the catalog URL from stored STAC credentials.

        This function retrieves the 'catalog_url' key value from the
        STAC credentials stored within the instance. The value returned
        may be None if the key does not exist in the credentials dictionary.

        :return: The catalog URL as a string if present, or None.
        :rtype: Optional[str]
        """
        return self.stac_credentials.get('catalog_url')

    def get_bearer_auth(self) -> BearerAuth:
        """
        Returns an instance of BearerAuth with a token retrieved using client credentials.

        This method communicates with a token service endpoint to request an
        access token using the `client_credentials` grant type. The access
        token is then encapsulated in a `BearerAuth` object and returned.

        :param self: Instance of the class invoking this method.

        :return: An instance of `BearerAuth` initialized with the retrieved access token.
        """
        data = {
            "grant_type": "client_credentials",
            "client_id": self.stac_credentials["client_id"],
            "client_secret": self.stac_credentials["client_secret"],
            "scope": "openid roles",
        }
        resp = post(self.stac_credentials["token_url"], data=data)
        resp.raise_for_status()
        token = resp.json()["access_token"]

        return BearerAuth(token)

    def upload_collection_to_catalog(self, collection: pystac.Collection, edit_flag: bool = False) -> str:
        """
        Uploads a STAC collection to a catalog. This method either creates a new collection or updates an
        existing collection based on the provided `edit_flag`.

        :param collection: The STAC collection to be uploaded, represented as a `pystac.Collection` object.
        :param edit_flag: A boolean flag indicating whether to edit an existing collection (True) or create
            a new one (False). Default is False.

        :raises RuntimeError: If the collection validation fails (HTTP 400 response).
        :raises requests.exceptions.RequestException: For any non-400 response errors during the HTTP request.

        :return: The ID of the created or updated collection as a string.
        """
        #get the auth
        auth_token = self.get_bearer_auth()
        #get catalog_url
        catalog_url = self.get_catalog_url()

        # load the collection from the created collection.  
        coll = collection.to_dict()
        if "links" in coll:
            coll["links"] = []
        coll.setdefault("_auth", {"read": ["anonymous"], "write": ["stac-admin-prod"]})
        print(coll)
        if edit_flag:
            # update an existing collection
            collection_url = f"{catalog_url}/collections/{collection.id}"
            resp = put(collection_url, auth=auth_token, json=coll)
        else:
            # upload a new collection
            collection_url = f"{catalog_url}/collections/"
            resp = post(collection_url, auth=auth_token, json=coll)
        if resp.status_code == 201:
            coll_id = resp.json()["id"]
            if edit_flag:
                print(f"Collection edited: {coll_id}")
            else:
                print(f"Collection created: {coll_id}")
            return coll_id
        elif resp.status_code == 400:
            raise RuntimeError("Collection validation failed")
        else:
            resp.raise_for_status()

    def upload_items_to_collection(self, collection: pystac.Collection,
                                   items_to_upload: List[Tuple[str, str]]) -> None:
        """
        Uploads or edits items in a given collection using the provided list of item actions.

        This method iterates through a list of items and their specified actions (upload or
        edit) to process them accordingly. It fetches an authentication token and catalog URL,
        manipulates the items in the collection, and sends the appropriate HTTP requests
        to either upload or update the items on the remote server.

        :param collection: The STAC Collection object where items need to be updated or
            uploaded.
        :param items_to_upload: A list of tuples where each tuple contains an item ID (str)
            and the corresponding action ("upload" or "edit").
        """
        #get the auth
        auth_token = self.get_bearer_auth()
        #get catalog_url
        catalog_url = self.get_catalog_url()

        # check if there are items that need to be uploaded only if update is False
        if len(items_to_upload) == 0:
            print("no items to upload")
            return

        print(
            f"Found {len(items_to_upload)} items that need to be uploaded/edied"
        )
        # items url
        items_url = f"{catalog_url}/collections/{collection.id}/items"

        for [item_id, update_edit] in items_to_upload:
            try:
                item = collection.get_item(item_id)
                item.clear_links()
            except Exception as e:
                print(f"Failed to get {item_id} from collection: {e}")
                continue
            if update_edit == "upload":
                resp = post(items_url, auth=auth_token, json=item.to_dict())
            if update_edit == "edit":
                item_url = f"{items_url}/{item.id}"
                resp = put(item_url, auth=auth_token, json=item.to_dict())
            if resp.ok:
                print(f"  ✓ {item_id} has been {update_edit}")
            else:
                print(f"  ✗ {item_id} → {resp.status_code} {resp.text}")

    def delete_collection(self, collection_name: str) -> None:
        """
        Deletes a specified collection from the catalog. This method constructs the   
        URL for the collection, uses authentication to validate the request, and        
        sends a delete request to remove the collection. If the deletion is successful,     
        a confirmation message is printed. Otherwise, an error message with the                     
        appropriate HTTP status code and error details is displayed.                         

        :param collection_name: The name of the collection to delete.                     
        """
        catalog_url = self.get_catalog_url()
        auth_token = self.get_bearer_auth()
        collection_url = f"{catalog_url}/collections/{collection_name}"
        resp = delete(collection_url, auth=auth_token)
        if resp.status_code == 204:
            print(f"Collection {collection_url.rsplit('/')[-1]} deleted successfully")
        else:
            print(f"Failed to delete collection: HTTP {resp.status_code}\n{resp.text}")

class ReadFaker:
    """
    A class that mimics file reading behavior for data in tabular formats.

    This class provides a way to iterate through tabular data row by row, mimicking
    the behavior of reading lines from a file. The data is expected to support the
    itertuples() method, typical of pandas DataFrame objects. Each row is returned
    as a formatted string with tab-separated values.

    Attributes:
        iter: An iterator generated from the itertuples() method of the provided
              data, representing the rows of the dataset.

    Methods:
        readline(size=None): Reads the next row of the data as a tab-separated
                             string.
        read(size=None): Alias for the readline method.
    """

    def __init__(self, data):
        """
        Initializes an iterator over the rows of a DataFrame.

        This class constructor sets up an iterator based on the itertuples method of 
        the provided DataFrame-like object. Each element in the iterator represents 
        a row in the DataFrame. 
        This foundation allows for efficient row-wise access and processing.

        :param data: A DataFrame-like object or similar, containing tabular data. 
                     This object must have an itertuples method.
        """
        self.iter = data.itertuples()

    def readline(self, size: Optional[int] = None) -> str:
        try:
            # get all columns values in that row
            row_values = next(self.iter)[1:]  # element 0 is the index
            # translate the row_values in a string
            row_string = '\t'.join(str(x) for x in row_values) + '\n'
        except StopIteration:
            return ''
        else:
            return row_string

    read = readline

class BearerAuth(auth.AuthBase):
    """
    Provides a custom authentication class for bearer token authentication.

    This class is utilized to authenticate HTTP requests using a bearer token, 
    which is typically used in APIs to authenticate requests securely. The token 
    is passed in the Authorization header of each request.

    Attributes:
        token (str): The bearer token to include in the Authorization header.
    """

    def __init__(self, token: str):
        """
        Represents a class initializer method that sets up the class with a token.

        This method initializes the class by assigning the provided token to an instance
        attribute for future use.

        :param token: The token to be assigned to the class instance.
        """
        self.token = token

    def __call__(self, r):
        """
        Modifies the request headers to include an Authorization token.

        This callable object adjusts the headers of an HTTP request object to
        include an Authorization header containing a Bearer token. It is useful
        for applications or systems requiring token-based authentication.

        :param r: The HTTP request object that will have its headers modified.
        :return: The modified HTTP request object with the added Authorization header.
        """
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r

class SONATA_storage(S3_storage, stac_storage):
    """
    Provides functionalities to manage storage using S3 and STAC storage systems.

    The SONATA_storage class combines and leverages features of S3_storage and
    stac_storage to manage storage and authentication for the SONATA project.
    It allows operations like switching S3 bucket context and setting
    credentials for S3 and STAC environments.

    Attributes:
        s3_client: Optional[boto3.client]
            The client for interacting with S3 storage, initialized as None until
            configured.
        credentials: dict
            The credentials parsed and loaded from the provided credential file.
        s3_credentials: dict
            The credentials specific to access S3 storage.
        s3_bucket: str
            The name of the current S3 bucket in use.
        export_workspace: str
            The export workspace path associated with the current S3 bucket.
    """
    def __init__(self, file_path: str = '~/.sonata_credentials',
                 s3_bucket: str = 'sonata', s3_project: str = 'S3-auth-sonata'):
        """
        Initializes the class instance with provided file path and S3 bucket, setting up
        credentials and configurations for interaction with S3 and STAC services.

        :param file_path: Path to the file containing credentials. Default is
            '~/.sonata_credentials'.
        :param s3_bucket: Name of the S3 bucket to initialize with. Default is 'sonata'.
        """
        self.s3_client: Optional[boto3.client] = None
        self.credentials: dict = read_credential_file(file_path)
        self.s3_project = s3_project
        self.s3_credentials = None
        self.s3_bucket = None
        self.export_workspace = None
        self._set_s3_credentials(bucket=s3_bucket)
        self._set_stac_credentials()

    def switch_s3_bucket(self, bucket: str) -> None:
        """
        Switches the current S3 bucket to the specified bucket name.

        This method updates the credentials or settings related to the
        new specified S3 bucket.

        :param bucket: The name of the S3 bucket to switch to.
        """
        self._set_s3_credentials(bucket=bucket)

    def _set_s3_credentials(self, bucket: str) -> None:
        """
        Sets S3 credentials based on the specified bucket.

        This method validates if the provided bucket exists in the predefined list
        and initializes S3 credentials based on the bucket's configuration from a
        fake credential vault. Additionally, it initializes or reinitializes
        the S3 client if needed.

        :param bucket: The name of the S3 bucket to be validated and used.
        """
        # check
        if not bucket.lower() in BUCKETS.get(self.s3_project.split('-')[-1], []):
            raise Exception(f"Bucket '{bucket}' does not exist in the project {self.s3_project.split('-')[-1]}.")

        fake_vault: dict = self.credentials[self.s3_project]

        # based on bucket we set variables.
        bucket = fake_vault['buckets'][bucket.lower()]

        self.s3_credentials = {
            "AWS_ACCESS_KEY_ID": fake_vault['AWS_ACCESS_KEY_ID'],
            "AWS_SECRET_ACCESS_KEY": fake_vault['AWS_SECRET_ACCESS_KEY'],
            "s3_endpoint": fake_vault['s3_endpoint'],
            "bucket_name": bucket['bucket_name'],
            "export_workspace": bucket['export_workspace']
        }
        self.s3_bucket = self.s3_credentials['bucket_name']
        self.export_workspace = self.s3_credentials['export_workspace']

        # re-init the s3_client if needed
        if self.s3_client is not None:
            self.s3_client.close()
            self._init_boto3()

    def _set_stac_credentials(self) -> None:
        """
        Sets the STAC credentials using the provided credentials data. The method
        retrieves required authentication details and assigns them to the internal
        stac_credentials attribute for further use in accessing STAC services.

        This method does not take any input arguments and does not return a value.
        It strictly operates on the instance's state.

        :param self: Instance of the class.

        :return: None
        """
        STAC_fake_vault = self.credentials[f'STAC-prod-auth']

        self.stac_credentials = {
            "CLIENT_ID": STAC_fake_vault['CLIENT_ID'],
            "CLIENT_SECRET": STAC_fake_vault['CLIENT_SECRET'],
            "TOKEN_URL": STAC_fake_vault['TOKEN_URL'],
            "catalog_url": STAC_fake_vault['catalog_url']
        }

class WEED_storage(S3_storage, SQL_storage, gdrive_storage, stac_storage, MLFlow_storage):
    """
    A unified storage class that integrates various storage backends including S3, Google Drive, SQL, and STAC.

    The WEED_storage class enables seamless access and configuration for different storage services.
    It manages credentials, configurations, and interactions needed for using these services. Designed for
    flexibility, it supports multiple projects and environments efficiently by providing an interface to set
    up, switch, and authenticate storage backends.
    """
    def __init__(self, username: str = 'buchhornm',
                 gdrive_entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu",
                 s3_bucket: str = "ecdc",
                 stac_env: str ='prod',
                 project: str = 'WEED'):
        """
        A class constructor that initializes settings for access to Google Drive, S3 and ML Flow services.

        The constructor sets up default values for the user credentials, Google Drive entry point,
        S3 bucket and ML Flow uri. It also initializes necessary credentials and configurations for using
        Google Drive and S3 storage services.

        :param username: A username string used for accessing credentials.
        :param gdrive_entry_point: The entry point ID of the Google Drive folder.
        :param s3_bucket: The name of the S3 bucket to be used.
        :param stac_env: The environment name for the STAC catalog.
        :param project: The name of the project (e.g. WEED, SONATA, OBSGESSION).
        """
        #super().__init__()
        self.gdrive_fs: Optional[GDriveFileSystem] = None
        self.s3_client: Optional[boto3.client] = None

        self.username = username
        self.credentials = self._get_credentials()
        self.project = project
        self.s3_credentials = None
        self.s3_bucket = None
        self.export_workspace = None
        self._set_s3_credentials(bucket=s3_bucket)
        self._set_sql_credentials()
        self._set_gdrive_credentials(gdrive_entry_point=gdrive_entry_point)
        self._set_stac_credentials(stac_env=stac_env)
        self._set_mlflow_credentials()

    def _get_credentials(self):
        """warper to get the credentials."""
        return get_credentials(self.username)

    def switch_s3_bucket(self, bucket: str) -> None:
        """
        Switches the current S3 bucket to the specified bucket name.

        This method updates the credentials or settings related to the
        new specified S3 bucket.

        :param bucket: The name of the S3 bucket to switch to.
        """
        self._set_s3_credentials(bucket=bucket)

    def _set_s3_credentials(self, bucket: str) -> None:
        """
        Sets the S3 credentials based on the specified bucket name and project. This method checks the validity
        of the bucket against the predefined buckets and retrieves necessary credentials to
        interact with the corresponding S3 service. It also reinitializes the S3 client if it is already
        initialized to ensure it uses the new credentials.
        This function is now also able to handle the case where we are working for different projects.
        The naming in the vault should follow the following convention for non weed projects: S3-auth-<<project name>>.

        :param bucket: The name of the S3 bucket to set credentials for. Must be part of the WEED project.
        :raises Exception: If the specified bucket does not exist in the WEED project.
        """

        # check
        if not bucket.lower() in BUCKETS.get(self.project, []):
            raise Exception(f"Bucket '{bucket}' does not exist in the project '{self.project}'.")

        if self.project == 'WEED':
            s3_vito_vault = string_to_dict(self.credentials['S3-auth'])
        else:
            s3_vito_vault = string_to_dict(self.credentials[f'S3-auth-{self.project}'])

        # based on bucket we set variables.
        bucket = s3_vito_vault['buckets'][bucket.lower()]

        self.s3_credentials = {
            "AWS_ACCESS_KEY_ID": s3_vito_vault['AWS_ACCESS_KEY_ID'],
            "AWS_SECRET_ACCESS_KEY": s3_vito_vault['AWS_SECRET_ACCESS_KEY'],
            "s3_endpoint": s3_vito_vault['s3_endpoint'],
            "bucket_name": bucket['bucket_name'],
            "export_workspace": bucket['export_workspace']
        }
        self.s3_bucket = self.s3_credentials['bucket_name']
        self.export_workspace = self.s3_credentials['export_workspace']

        # re-init the s3_client if needed
        if self.s3_client is not None:
            self.s3_client.close()
            self._init_boto3()

    def _set_sql_credentials(self) -> None:
        """
        Sets SQL credentials for the database connection.

        This method extracts and sets the SQL credentials from
        a structured string stored in the object's credentials attribute.
        The parsed credentials are stored in the `sql_credentials` attribute
        for later use in database-related operations.

        :param self: Instance of the class containing `credentials` attribute and
            where `sql_credentials` will be set.
        """
        sql_vito_vault = string_to_dict(self.credentials['postGreSQL-auth'])

        self.sql_credentials = {
            "dbname": sql_vito_vault['dbname'],
            "schema": sql_vito_vault['schema'],
            "password": sql_vito_vault['password'],
            "host": sql_vito_vault['host'],
            "port": sql_vito_vault['port']
        }

    def _set_mlflow_credentials(self) -> None:
        """
        Sets ML Flow credentials for the API connection by setting 
        them in the environment variables of the python run.

        This method extracts and sets the ML Flow credentials from
        a structured string stored in the object's credentials attribute.
        The parsed credentials are stored in the `mlflow_credentials` attribute
        for later use in API-related operations.

        :param self: Instance of the class containing `credentials` attribute and
            where `mlflow_credentials` will be set.
        """
        sql_vito_vault = string_to_dict(self.credentials["MLflow-auth"])

        self.mflow_credentials = {
            "MLFLOW_TRACKING_USERNAME": sql_vito_vault['user'],
            "MLFLOW_TRACKING_PASSWORD": sql_vito_vault['pass'],
            "MLFLOW_TRACKING_URI": sql_vito_vault['url'],
        }

        set_dotenv_vars_from_dict(self.mflow_credentials)

    def _set_gdrive_credentials(self, gdrive_entry_point: str) -> None:
        """
        Sets Google Drive API credentials and entry point based on provided parameters.

        This method converts credentials stored as a string into a dictionary, then uses
        specific values from it to construct a dictionary of Google Drive API credentials.
        Additionally, it sets the entry point for accessing the Google Drive API.

        :param gdrive_entry_point: The entry point URL for the Google Drive API.
        """
        gdrive_vito_vault = string_to_dict(self.credentials['gdrive-access'])

        self.gdrive_credentials = {
            "type": gdrive_vito_vault['type'],
            "project_id": gdrive_vito_vault['project_id'],
            "private_key_id": gdrive_vito_vault['private_key_id'],
            "private_key": gdrive_vito_vault['private_key'],
            "client_email": gdrive_vito_vault['client_email'],
            "client_id": gdrive_vito_vault['client_id'],
            "auth_uri": gdrive_vito_vault['auth_uri'],
            "token_uri": gdrive_vito_vault['token_uri'],
            "auth_provider_x509_cert_url": gdrive_vito_vault['auth_provider_x509_cert_url'],
            "universe_domain": gdrive_vito_vault['universe_domain']
        }
        self.gdrive_entry_point = gdrive_entry_point

    def _set_stac_credentials(self, stac_env: str) -> None:
        """
        Sets the STAC credentials for the specified environment ('prod' or 'dev') by
        retrieving the corresponding authentication information from the credentials
        dictionary and formatting it. Raises an exception if the provided environment
        does not exist.

        :param stac_env: The environment for which STAC credentials should be set. Must be
                         either 'prod' or 'dev'.
        :raises Exception: Raised if the specified environment does not exist in the WEED
                           project (not 'prod' or 'dev').
        """
        if stac_env not in ['prod', 'dev']:
            raise Exception(f'the specified environment {stac_env} does not exist in the WEED project. '
                            f'It should be prod or dev.')

        STAC_vito_vault = string_to_dict(self.credentials[f'STAC-{stac_env}-auth'])

        self.stac_credentials = {
            "CLIENT_ID": STAC_vito_vault['CLIENT_ID'],
            "CLIENT_SECRET": STAC_vito_vault['CLIENT_SECRET'],
            "TOKEN_URL": STAC_vito_vault['TOKEN_URL'],
            "catalog_url": STAC_vito_vault['catalog_url']
        }

def get_credentials(user :str ) -> Dict[str, str]:
    """
    Retrieves WEED access credentials from Terrascope VAULT using LDAP authentication.

    This method prompts the user to enter their password for Terrascope VAULT, authenticates
    with the VAULT using LDAP, and fetches credentials from the WEED KV storage path.

    :return: credentials as a dictionary
    """
    password_prompt = 'Please enter your password for the Terrascope VAULT: '
    service_account_password = getpass(prompt=password_prompt)

    try:
        client = hvac.Client(url='https://vault.vgt.vito.be')
        client.auth.ldap.login(
            username=user,
            password=service_account_password,
            mount_point='ldap'
        )
        secret_version_response = client.secrets.kv.v2.read_secret_version(mount_point='kv',
                                                                           path='TAP/apps/WEED',
                                                                           raise_on_deleted_version=True)
        client.logout()
    except:
        raise Exception('Could not retrieve WEED credentials from Terrascope VAULT. '
                        'Are you connected to the VITO VPN?')
    return secret_version_response['data']['data']

def read_credential_file(file_path: str = '~/.sonata_credentials') -> Dict[str, str]:
    # check if file exists
    if not os.path.exists(file_path):
        file_path = os.path.expanduser(file_path)
        if not os.path.exists(file_path):
            raise Exception(f"The specified credential file '{file_path}' does not exist.")

    # read file & strip all whitespaces and linebreaks and \n
    with open(file_path, 'r') as file_handle:
        file_string = file_handle.read()
        file_string = file_string.strip()
    # remove all possible \n
    file_string = file_string.replace('\n', '')

    return string_to_dict(file_string)

class ProgressPercentage:
    """
    Class to track and display the progress of file operations.

    This class is designed to handle progress tracking and display for
    file operations such as downloading or uploading. The progress is
    updated based on the amount of bytes processed, and a progress bar
    is displayed to provide a visual representation of the operation.

    Attributes:
    filename: str
        The name of the file being processed.
    _total_size: int
        The total size of the file in bytes.
    _progress: int
        The current number of bytes processed.
    _progress_bar:
        The progress bar instance used for visual display.
    """
    def __init__(self, filename: str, total_size: int, way: str = 'downloading'):
        """
        Initializes an instance of a class designed to handle and visually track the progress of a task,
        such as downloading a file, using a progress bar.

        :param filename: The name of the file that is being processed.
        :param total_size: The total size of the file or task to track progress for.
        :param way: A description of the action being performed (e.g., 'downloading').
        """
        self.filename = filename
        self._total_size = total_size
        self._progress = 0
        self._progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"{way} {self.filename}")

    def __call__(self, bytes_amount):
        """
        Update the progress bar with the given number of bytes and close it if the total size
        has been reached or exceeded.

        :param bytes_amount: The number of bytes to add to the current progress.
        """
        self._progress += bytes_amount
        self._progress_bar.update(bytes_amount)
        if self._progress >= self._total_size:
            self._progress_bar.close()


def _factor_of_1MB(filesize: int, num_parts: int) -> int:
    """
    Determine a factor of 1MB based on file size and number of parts.

    The function calculates a size factor aligned with 1MB blocks for evenly splitting a
    given file size into the specified number of parts. It ensures that the determined
    size for each part is rounded up to the nearest 1MB boundary.

    :param filesize: The size of the file in bytes.
    :param num_parts: The number of parts to divide the file into.
    :return: The computed factor in bytes aligned to the nearest 1MB boundary.
    """
    x = filesize / int(num_parts)
    y = x % 1048576
    return int(x + 1048576 - y)

def _calc_etag(inputfile: str, partsize: int) -> str:
    """
    Calculate the Amazon S3 ETag for a file uploaded in parts.

    The ETag is an MD5 hash of the file's contents or, in the case of a multipart
    upload, a hash of the concatenated binary MD5 digests of each part, followed
    by a dash and the number of parts. This function reads the file in chunks
    of the specified size, computes the MD5 digest for each chunk, and combines
    them to compute the final ETag.

    :param inputfile: The path to the file for which the ETag is to be calculated.
    :param partsize: The size of each part to be considered for multipart
        hashing, in bytes. If the file size exceeds this, it will simulate
        a multipart upload.
    :return: The computed ETag value as a string, optionally including the
        multipart part count if applicable.
    """
    md5_digests = []
    with open(inputfile, 'rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))

def calculate_md5(file_path, chunk_size=8192):
    """
    Calculate the MD5 checksum of a file.

    This function reads the content of a file in chunks to efficiently compute
    the MD5 hash without loading the entire file into memory. It is particularly
    useful for working with large files.

    :param file_path: The path to the file for which the MD5 checksum will be
        calculated.
    :param chunk_size: The size of the chunks to read from the file, in bytes.
        The default value is 8192.
    :return: The hexadecimal MD5 checksum of the file.
    :raises FileNotFoundError: If the specified file does not exist.
    :raises RuntimeError: If any other error occurs while calculating the MD5 checksum.
    """
    md = md5()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):  # Read file in chunks
                md.update(chunk)
        return md.hexdigest()
    except FileNotFoundError:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    except Exception as e:
        raise RuntimeError(f"An error occurred while calculating MD5: {str(e)}")

def _possible_partsizes(filesize: int, num_parts: int) -> callable:
    """
    Calculate possible part sizes for dividing a file into the desired number of parts.

    This function returns a callable that can determine whether a given part
    size is suitable for dividing a file of the specified size into the specified
    number of parts. The callable checks whether the proposed part size is less
    than the total file size and whether dividing the file by the part size results
    in a number of parts not exceeding the allowed number.

    :param filesize: The total size of the file that needs to be divided, in bytes.
    :param num_parts: The desired maximum number of parts into which the file
        can be divided.
    :return: A callable function that takes a part size as input and evaluates
        whether the given part size satisfies the conditions for dividing the file.
    """
    return lambda partsize: partsize < filesize and (float(filesize) / float(partsize)) <= num_parts
