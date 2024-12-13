from pydrive2.fs import GDriveFileSystem
import hvac
from getpass import getpass
import tempfile
from typing import Union
import geopandas as gpd
import pandas as pd
import os

def get_WEED_credentials(username: str = 'buchhornm', key: str = 'gdrive-access') -> str:
    """
    Retrieves WEED access credentials from Terrascope VAULT using LDAP authentication.

    This method prompts the user to enter their password for Terrascope VAULT, authenticates
    with the VAULT using LDAP, and fetches credentials from the WEED KV storage path.

    :param username: LDAP username used to authenticate with the VAULT, defaults to 'buchhornm'
    :param key: Key in the KV WEED storage to get value from, defaults to 'gdrive-access'
    :return: credentials as a string
    """
    password_prompt = 'Please enter your password for the Terrascope VAULT: '
    service_account_password = getpass(prompt=password_prompt)

    client = hvac.Client(url='https://vault.vgt.vito.be')
    client.auth.ldap.login(
        username=username,
        password=service_account_password,
        mount_point='ldap'
    )
    secret_version_response = client.secrets.kv.v2.read_secret_version(mount_point='kv',
                                                                       path='TAP/apps/WEED',
                                                                       raise_on_deleted_version=True)
    client.logout()
    return secret_version_response['data']['data'][key]

def WEED_GDRIVE_Access(username: str = 'buchhornm',
                       entry_point: str = "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu") -> GDriveFileSystem:
    """
    Access the Google Drive filesystem with given credentials and entry point.

    This function retrieves the credentials for the Google Drive service account
    access from the VITO TERRASCOPE vault and returns a fsspec filesystem that
    allows access to the files and folders available for the service account.

    :param username: Username for accessing the credentials vault. Defaults to 'buchhornm'.
    :param entry_point: The identifier for the entry point in Google Drive. Defaults to
        "1k27bitdRp41AtHq1xupyqwKaTLzrMUMu".
    :return: GDriveFileSystem instance configured with the specified entry point and
        service account credentials.
    """
    # get the credentials for the GDrive service account access from the VITO TERRASCOPE vault
    gdrive_credentials = get_WEED_credentials(username=username)

    # return the fsspec filesystem to access the files & folders available for the service account credentials
    return GDriveFileSystem(entry_point, use_service_account=True, client_json=gdrive_credentials)

def print_gdrive_overview(gdrive: GDriveFileSystem) -> None:
    """
    Prints an overview of the Google Drive filesystem structure starting from
    the root directory. It iterates through each directory and subdirectory,
    listing all files found.

    This function walks through the file system using the provided GDriveFileSystem
    instance, and prints the name of each directory and files contained therein.

    :param gdrive: Instance of GDriveFileSystem representing the Google Drive
                   file system.

    :return: None
    """
    for dirName, subdirList, fileList in gdrive.walk(gdrive.root):
        print('Found directory: %s' % dirName)
        for fname in fileList:
            print('\t%s' % fname)

def get_gdrive_gdf(gdrive: GDriveFileSystem, gdrive_path: str,
                      filter_bbox: Union[tuple, gpd.GeoDataFrame, None] = None) -> gpd.GeoDataFrame:
    """
    Downloads a file from Google Drive and reads it into a GeoDataFrame, with an optional bounding box filter.

    This function creates a temporary directory to store the downloaded file, which is automatically deleted
    after the function execution. It can also filter the GeoDataFrame with the provided bounding box.

    :param gdrive: Google Drive FileSystem object used to handle download operations
    :param gdrive_path: Path to the file on Google Drive
    :param filter_bbox: Optional bounding box to filter the GeoDataFrame, could be a tuple or GeoDataFrame
    :return: A GeoDataFrame containing the data from the downloaded file, optionally filtered by the bounding box
    """
    # Create a temporary directory that will be automatically deleted
    with tempfile.TemporaryDirectory() as temp_dir:
        gdrive.download(gdrive_path, os.path.join(temp_dir, 'temp.gpkg'))

        if filter_bbox:
            return gpd.read_file(os.path.join(temp_dir, 'temp.gpkg'), bbox=filter_bbox)
        else:
            return gpd.read_file(os.path.join(temp_dir, 'temp.gpkg'))
