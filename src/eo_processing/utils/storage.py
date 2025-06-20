from __future__ import annotations

from pydrive2.fs import GDriveFileSystem
import hvac
from getpass import getpass
import tempfile
from typing import Union, Dict, Tuple, List, Optional, TYPE_CHECKING
import geopandas as gpd
import os
import boto3
from botocore.exceptions import ClientError
import time
from eo_processing.utils.helper import string_to_dict
from hashlib import md5
from tqdm import tqdm

if TYPE_CHECKING:
    from eo_processing.config.data_formats import s3_credentials_format

WEED_BUCKETS = ['ecdc', 'model', 'extent', 'test', 'ecdc-stac', 'extent-stac']

class storage:
    """
    Manages interaction with S3 storage, including initialization, content retrieval, and file
    operations.

    The class provides a set of methods for accessing and working with data stored in S3-compatible
    cloud storage. This includes initializing an S3 client, fetching content metadata, downloading
    files, and building URLs. It uses Boto3 for communication with the S3 API and offers features
    such as retrying downloads and filtering file types.

    :ivar s3_credentials: Dictionary containing S3 access and storage information. Contains
        the following keys:
            - AWS_ACCESS_KEY_ID: S3 access key required for authentication.
            - AWS_SECRET_ACCESS_KEY: Secret key associated with the S3 access key.
            - s3_endpoint: Endpoint URL for accessing the S3 bucket.
            - bucket_name: Name of the S3 bucket.
            - export_workspace: Directory path where exported files will be saved of S3.

    :ivar s3_client: Initialized S3 client object created using the provided credentials.
        Defaults to None when credentials are not set.

    :ivar s3_bucket: Name of the associated S3 bucket extracted from the credentials. Will
        be `None` if no credentials are provided.
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

class WEED_storage(storage):
    """
    Handles storage operations using both Google Drive and AWS S3/storage services.

    This class provides a unified interface to manage files and data stored in the WEED
    infrastructure. It integrates Google Drive and S3 functionalities using appropriate
    APIs, allowing users to initialize settings, authenticate, browse, and interact with
    these storages.

    It fetches credentials securely from the Terrascope VAULT and enables seamless access
    to storage operations like fetching files, reading data into geospatial dataframes, and
    filtering data with bounding boxes.

    Attributes:
        username: Username for Terrascope VAULT authentication.
        gdrive_entry_point: Entry point ID of the Google Drive folder.
        s3_bucket: Name of the S3 bucket to be used.
        gdrive_fs: Instance for handling Google Drive filesystem operations.
        s3_client: AWS S3 client instance for interacting with S3 storage.
        credentials: Dictionary containing fetched credentials for storages.
        s3_credentials: Dictionary containing S3-specific credentials.
        gdrive_credentials: Google Drive credentials fetched from the VAULT.
        export_workspace: Path for data export associated with the S3 bucket.
    """
    def __init__(self, username: str = 'buchhornm',
                 gdrive_entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu",
                 s3_bucket: str = "ecdc"):
        """
        A class constructor that initializes settings for access to Google Drive and S3 services.

        The constructor sets up default values for the user credentials, Google Drive entry point,
        and S3 bucket. It also initializes necessary credentials and configurations for using
        Google Drive and S3 storage services.

        :param username: A username string used for accessing credentials.
        :param gdrive_entry_point: The entry point ID of the Google Drive folder.
        :param s3_bucket: The name of the S3 bucket to be used.
        """
        #super().__init__()
        self.gdrive_fs: Optional[GDriveFileSystem] = None
        self.s3_client: Optional[boto3.client] = None

        self.username = username
        self.credentials = self._get_credentials()
        self.s3_credentials = None
        self.s3_bucket = None
        self.export_workspace = None
        self._set_s3_credentials(bucket=s3_bucket)
        self.gdrive_credentials = self.credentials['gdrive-access']
        self.gdrive_entry_point = gdrive_entry_point

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
        Sets the S3 credentials based on the specified bucket name. This method checks the validity
        of the bucket against the predefined WEED buckets and retrieves necessary credentials to
        interact with the corresponding S3 service. It also reinitializes the S3 client if it is already
        initialized to ensure it uses the new credentials.

        :param bucket: The name of the S3 bucket to set credentials for. Must be part of the WEED project.
        :raises Exception: If the specified bucket does not exist in the WEED project.
        """
        # check
        if not bucket.lower() in WEED_BUCKETS:
            raise Exception('the specified bucket does not currently exist in the WEED project.')

        s3_vito_vault = string_to_dict(self.credentials['S3-auth'])

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

    def _get_credentials(self) -> Dict[str, str]:
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
                username=self.username,
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
