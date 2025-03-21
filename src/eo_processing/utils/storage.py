from __future__ import annotations
from pydrive2.fs import GDriveFileSystem
import hvac
from getpass import getpass
import tempfile
from typing import Union, Dict, Tuple, List, Optional, TYPE_CHECKING
import geopandas as gpd
import os
import boto3
import time
from eo_processing.utils.helper import string_to_dict
from hashlib import md5
from tqdm import tqdm

if TYPE_CHECKING:
    from eo_processing.config.data_formats import s3_credentials_format

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

    :ivar s3_client: Initialized S3 client object created using the provided credentials.
        Defaults to None when credentials are not set.

    :ivar s3_bucket: Name of the associated S3 bucket extracted from the credentials. Will
        be `None` if no credentials are provided.
    """
    def __init__(self, s3_credentials: Optional[s3_credentials_format] = None):
        """
        Initializes the instance with S3 credentials and sets up the related attributes.

        This constructor initializes the object with provided S3 credentials in a dictionary
        format or defaults to an uninitialized state if no credentials are provided. If the
        credentials are not in the expected format, an error will be raised. Warning messages
        are printed if no credentials are supplied and the storage is not initialized.

        :param s3_credentials: Optional S3 credentials with specific keys defined by
            the `s3_credentials_format`. When provided, it should be a dictionary containing
            the keys `s3_access_key`, `s3_secret_key`, `s3_endpoint`, and `bucket_name`.

        :raises ValueError: If the supplied S3 credentials do not match the expected format,
            this exception is raised.
        """
        from eo_processing.config.data_formats import s3_credentials_format
        if s3_credentials is None:
            print('WARNING: no S3 credentials were given or found in the environment variables. '
                  'The storage object is not initialized.')
            self.s3_credentials = {
                "AWS_ACCESS_KEY_ID": None,
                "AWS_SECRET_ACCESS_KEY": None,
                "s3_endpoint": None,
                "bucket_name": None
            }
        elif ((isinstance(s3_credentials, dict))
              and (set(s3_credentials.keys()) == set(s3_credentials_format.__annotations__.keys()))):
            self.s3_credentials = {
                "AWS_ACCESS_KEY_ID": s3_credentials['s3_access_key'],
                "AWS_SECRET_ACCESS_KEY": s3_credentials['s3_secret_key'],
                "s3_endpoint": s3_credentials['s3_endpoint'],
                "bucket_name": s3_credentials['bucket_name']
            }
        else:
            raise ValueError('The provided S3 credentials are not valid. Please check the documentation '
                             'for the correct format.')
        self.s3_client: Optional[boto3.client] = None
        self.s3_bucket = self.s3_credentials['bucket_name']

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

        session = boto3.session.Session()
        try:
            self.s3_client = session.client(
                service_name='s3',
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

    def get_s3_content(self, s3_directory: str) -> Dict:
        """
        Retrieves the list of objects from an S3 bucket for the specified directory prefix.

        This function connects to the configured S3 bucket and fetches the metadata
        for all objects matching the specified directory prefix. It initializes the
        S3 client if it has not already been initialized.

        :param s3_directory: The directory prefix within the S3 bucket. This should
            represent the folder path within the bucket whose contents need to be
            fetched.
        :return: A dictionary containing the metadata of objects in the specified
            S3 directory, as returned by the S3 client's list_objects_v2 method.
        """
        if self.s3_client is None:
            self._init_boto3()

        return self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=s3_directory)

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

    def get_file_urls(self, s3_directory: str = 'models', extension: str = '.onnx') -> List[str]:
        """
        Retrieves the full URLs of files in the specified S3 directory that match the given
        file extension. The URLs are constructed using the base URL derived from S3 credentials
        and bucket information along with file keys.

        :param s3_directory: The directory in the S3 storage to search for files.
        :param extension: The file extension to filter the files.
        :return: A list of URLs for the files matching the specified directory and
                 extension.
        """
        # get all filtered file_keys
        file_keys = self.get_file_keys(s3_directory, extension)

        # define the base URL to the specified S3 Model storage
        base_url = f"{self.s3_credentials['s3_endpoint']}/swift/v1/{self.s3_bucket}/"

        return [f"{base_url}{element}" for element in file_keys]

    def get_file_keys(self, s3_directory: str = 'results', extension: str = '.tif') -> List[str]:
        """
        Retrieves a list of file keys from an S3 directory that match the specified file extension.

        This method fetches the content of a specified subfolder within an S3 bucket. It filters
        the objects in the folder to include only those whose keys end with the given file extension.
        If no content is found in the folder, a message is printed, and an empty list is returned.

        :param s3_directory: The S3 directory from which the files should be listed.
        :param extension: The file extension used to filter the objects in the directory.
        :return: A list of file keys from the specified S3 directory that match the given file extension.
        """
        # get all content of the model subfolder in the bucket
        response = self.get_s3_content(s3_directory)

        if 'Contents' not in response:
            print('No files found in the selected folder with the given extension.')
            return []

        return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(extension)]

    def download_file_key(self, s3_object_key: str, temp_folder: str, progress_bar: bool = False) -> str:
        """
        Downloads a file from an S3 bucket using the specified object key and saves it
        in a temporary folder. Optionally displays a progress bar for tracking download
        progress. Ensures the `temp_folder` exists and handles various S3 client download
        methods based on the `progress_bar` value.

        :param s3_object_key: The key of the S3 object to be downloaded. It must be a
            single valid object key within the specified S3 bucket.
        :param temp_folder: The local folder path where the downloaded file will be
            saved. If the folder does not exist, it will be created automatically.
        :param progress_bar: A boolean indicating whether to display a progress bar
            during the file download. Defaults to `False`.

        :return: The local file path where the downloaded file is saved.
        """
        if self.s3_client is None:
            self._init_boto3()

        if not isinstance(s3_object_key, str):
            raise TypeError("s3_object_key must be a string. One single s3_file_key.")

        os.makedirs(os.path.normpath(temp_folder), exist_ok=True)

        # Local path to save the file
        local_file_path = os.path.join(os.path.normpath(temp_folder), os.path.basename(s3_object_key))

        try:
            if progress_bar:
                total_size = self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_object_key)['ContentLength']

                with open(local_file_path, 'wb') as f:
                    self.s3_client.download_fileobj(self.s3_bucket, s3_object_key, f,
                                               Callback=ProgressPercentage(s3_object_key, total_size))
            else:
                self.s3_client.download_file(self.s3_bucket, s3_object_key, local_file_path)
        except Exception as e:
            raise FileExistsError(f"Error downloading file {s3_object_key}: {e}")

        return local_file_path

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

        # calculate etag of local downloaded file
        filesize = os.path.getsize(os.path.normpath(local_file_path))
        num_parts = int(s3_etag.split('-')[1])

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
    Handles storage operations for the WEED application, including managing access
    to Google Drive and Amazon S3 using credentials retrieved from the Terrascope
    VAULT. This class provides methods to initialize necessary file systems, retrieve
    files, and download data as GeoDataFrames.

    The class leverages user authentication for secure access, offering the ability
    to interact with both Google Drive and S3 storage.

    :ivar username: Username utilized for LDAP authentication during credential
        retrieval.
    :ivar credentials: Dictionary containing storage credentials fetched from the
        Terrascope VAULT.
    :ivar s3_credentials: AWS S3-specific credentials retrieved from the stored
        data.
    :ivar gdrive_credentials: Google Drive-specific credentials required for
        accessing files and folders.
    :ivar gdrive_entry_point: Entry point directory in Google Drive where
        operations begin.
    :ivar gdrive_fs: Google Drive file system object for handling file interactions.
    :ivar s3_client: AWS S3 client for storage operations.
    :ivar s3_bucket: AWS S3 bucket name derived from the fetched credentials.
    """
    def __init__(self, username: str = 'buchhornm', gdrive_entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu"):
        """
        Initializes an object that manages credentials and handles access to GDrive and S3
        services for processing data. The instance stores the provided username and Google
        Drive entry point to establish connections with respective services and authenticate
        through credentials.

        :param username: The username to associate with this instance, which is also used
            to fetch credentials. Defaults to 'buchhornm'.
        :param gdrive_entry_point: The entry point ID for accessing Google Drive.
            Defaults to "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu".
        """
        super().__init__()
        self.username = username
        self.credentials = self._get_credentials()
        self.s3_credentials = string_to_dict(self.credentials['S3-auth'])
        self.gdrive_credentials = self.credentials['gdrive-access']
        self.gdrive_entry_point = gdrive_entry_point
        self.gdrive_fs: Optional[GDriveFileSystem] = None
        self.s3_client: Optional[boto3.client] = None
        self.s3_bucket = self.s3_credentials['bucket_name']

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
    Tracks the progress of a download and displays a progress bar via the tqdm library.

    This class is designed to monitor the amount of data downloaded for a given file and
    update a tqdm-based progress bar accordingly. It is callable, meaning that an
    instance of the class can be called with the amount of bytes downloaded to update progress.

    :ivar filename: The name of the file being downloaded.
    :ivar _total_size: The total size of the file being downloaded in bytes.
    :ivar _downloaded: The amount of bytes downloaded so far.
    :ivar _progress_bar: The tqdm progress bar instance for displaying download progress.
    """
    def __init__(self, filename, total_size):
        self.filename = filename
        self._total_size = total_size
        self._downloaded = 0
        self._progress_bar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {self.filename}")

    def __call__(self, bytes_amount):
        self._downloaded += bytes_amount
        self._progress_bar.update(bytes_amount)
        if self._downloaded >= self._total_size:
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

