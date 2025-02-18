from pydrive2.fs import GDriveFileSystem
import hvac
from getpass import getpass
import tempfile
from typing import Union, Dict, Tuple, List
import geopandas as gpd
import os
import boto3
import time
from eo_processing.utils.helper import string_to_dict

class WEED_storage:
    def __init__(self, username: str = 'buchhornm', gdrive_entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu"):
        self.username = username
        self.credentials = self._get_credentials()
        self.s3_credentials = string_to_dict(self.credentials['S3-auth'])
        self.gdrive_credentials = self.credentials['gdrive-access']
        self.gdrive_entry_point = gdrive_entry_point
        self.gdrive_fs: GDriveFileSystem = None
        self.s3_client: boto3.client = None
        self.s3_bucket = self.s3_credentials['bucket_name']
        #self.s3_prefix = None

    def _get_credentials(self) -> Dict[str, str]:
        """
        Retrieves WEED access credentials from Terrascope VAULT using LDAP authentication.

        This method prompts the user to enter their password for Terrascope VAULT, authenticates
        with the VAULT using LDAP, and fetches credentials from the WEED KV storage path.

        :return: credentials as a dictionary
        """
        password_prompt = 'Please enter your password for the Terrascope VAULT: '
        service_account_password = getpass(prompt=password_prompt)

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
        self.gdrive_fs = GDriveFileSystem(self.gdrive_entry_point, use_service_account=True,
                                          client_json=self.gdrive_credentials)

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
        session = boto3.session.Session()
        self.s3_client = session.client(
            service_name='s3',
            endpoint_url=self.s3_credentials['s3_endpoint'],
            aws_access_key_id=self.s3_credentials['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=self.s3_credentials['AWS_SECRET_ACCESS_KEY']
        )

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

    def download_s3_content(self, s3_objects: str, out_dir: str, retry: int = 0) :
        """
        Downloads all files (not jsons) with a specific prefix on S3. This can be as single file or mulitple files

        This function connects to the configured S3 bucket and fetches data
        for all objects matching the specified S3_objects. It initializes the
        S3 client if it has not already been initialized.

        :param s3_objects: This is the exact s3 object name or the s3 prefix (standaard S3 prefix + filename prefix)
        :param out_dir: directory for the download. the function will create subdirectories if needed
        :param retry: amount of retries alreaday done
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
                if not element.endswith('/') and not element.endswith('.json'):
                    dirname = os.path.dirname(s3_objects)
                    # basename = os.path.basename(s3_objects)
                    outname = os.path.join(out_dir, os.path.relpath(element, dirname))
                    if os.path.exists(outname):
                        continue
                    if not os.path.exists(os.path.dirname(outname)):
                        os.makedirs(os.path.dirname(outname), exist_ok=True)

                    self.s3_client.download_file(self.s3_bucket, element, outname)
                    #print(f"Successfully copied {element} to {outname}")

        except:
            if retry < 5:
                time.sleep(10)
                self.download_s3_content(s3_objects, out_dir, retry=retry+1)
            else:
                raise Exception('Copying data from S3 failed: ' + str(self.s3_bucket + '/' + s3_objects))



    def get_onnx_urls(self, s3_directory: str = 'models') -> List[str]:
        """
        Get a list of ONNX model URLs from the given S3 directory.

        :param s3_directory: The S3 directory to search for ONNX models. Defaults to 'models'.
        :return: A list of ONNX model URLs.
        """
        # define the base URL to the specified S3 Model storage
        base_url = f"{self.s3_credentials['s3_endpoint']}/swift/v1/{self.s3_bucket}/{s3_directory}/"
        # get all content of the model subfolder in the bucket
        response = self.get_s3_content(s3_directory)

        if 'Contents' not in response:
            print('No models found in the selected folder')
            return []

        return self._extract_onnx_urls(response['Contents'], base_url)

    def _extract_onnx_urls(self, contents: List[Dict], base_url: str) -> List[str]:
        """
        Extract ONNX model URLs from the response contents.

        :param contents: S3 content dictionary with keys and metadata.
        :param base_url: The base URL to prepend to model keys.
        :return: A list of ONNX model URLs.
        """
        return [
            f"{base_url}{obj['Key'].split('/')[-1]}"
            for obj in contents if obj['Key'].endswith('.onnx')
        ]
