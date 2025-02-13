# -*- coding: utf-8 -*-
# !/usr/bin/env python
import openeo
import requests
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import box
import importlib.resources as importlib_resources
import eo_processing.resources
from os.path import normpath
from eo_processing.utils.geoprocessing import openEO_bbox_format
import uuid
import hashlib
from datetime import datetime
import json
from ast import literal_eval
from typing import Union

def init_connection(provider: str) -> openeo.Connection :
    """
    Initializes a connection to the specified OpenEO backend provider and
    authenticates the connection using OpenID Connect. Supports connections
    to Terrascope, Development, CDSE, CDSE-Staging, and a standard OpenEO
    entry point.

    :param provider: The name of the OpenEO backend provider. Supported values
                     are 'terrascope', 'development', 'cdse', 'cdse-stagging',
                     or other for default OpenEO connection.
    :return: An authenticated OpenEO connection to the specified provider.

    :raises ValueError: If the provider specified does not match the supported
                        categories and a backend-specific connection setup is
                        unavailable.
    """
    if provider == 'terrascope':
        connection = openeo.connect("https://openeo.vito.be").authenticate_oidc()
    elif provider == 'development':
        connection = openeo.connect("https://openeo-dev.vito.be").authenticate_oidc()
    elif provider == 'cdse':
        connection = openeo.connect(url="openeo.dataspace.copernicus.eu").authenticate_oidc()
    elif provider == 'cdse-stagging':
        connection = openeo.connect(url='openeo-staging.dataspace.copernicus.eu').authenticate_oidc()
    else:
        print('currently no specific connections to backends like creodias and sentinelhub are setup.')
        print('use standard entry point')
        connection = openeo.connect("https://openeo.cloud").authenticate_oidc()
    return connection


def location_visu(aoi_object: openEO_bbox_format | gpd.GeoDataFrame, zoom: bool = False, region: str = 'EU',
                  label: bool = True) -> None:
    """
    Creates and visualizes a geographical representation of the Area of Interest (AOI)
    on a specified region, with options to zoom and include labels for countries.
    The function supports rendering AOI as either a bounding box dictionary or a
    GeoPandas GeoDataFrame, and can display results for Europe or the globe.

    :param aoi_object: The Area of Interest (AOI) to visualize. It can be specified as
        a dictionary containing keys 'south', 'west', 'north', 'east', and 'crs', or
        as a `GeoDataFrame` object.
    :param zoom: A boolean indicating whether to zoom into the region of interest
        (default is False).
    :param region: A string representing the region to visualize. Available options
        are 'EU' for Europe and 'globe' for the entire world (default is 'EU').
    :param label: A boolean indicating whether to include country or region labels
        on the map (default is True).
    :return: None. The function renders and displays the AOI on a map relative to
        the specified region.
    """
    # evaluate the input
    if isinstance(aoi_object, dict):
        # bring AOI dictionary into GeoDataFrame
        aoi = gpd.GeoDataFrame({"id": 1, "geometry": [box(aoi_object['west'], aoi_object['south'],
                                                          aoi_object['east'], aoi_object['north'])]})
        aoi.crs = aoi_object['crs']
    elif isinstance(aoi_object, gpd.GeoDataFrame):
        aoi = aoi_object
    else:
        print('the aoi has to be either a bounding box dictionary with south,west,north,east and crs '
              'OR a GeoPandas GeoDataFrame')
        return

    # get vector file of region
    if region == 'EU':
        url = 'https://raw.githubusercontent.com/leakyMirror/map-of-europe/master/GeoJSON/europe.geojson'
    elif region == 'globe':
        url = 'https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson'
    else:
        print(f'currently no specific region to show the AOI exists for given parameter: {region}')
        return
    response = requests.get(url)
    data = response.json()
    # convert in GeoDataFrame
    region_df = gpd.GeoDataFrame.from_features(data).set_crs(epsg=4326, allow_override=True)

    # zoom in
    if zoom:
        region_df = region_df[region_df.intersects(aoi.to_crs(4326).geometry.union_all(method='coverage'))]

    # create the figure
    fig, ax = plt.subplots()
    if region == 'EU':
        region_df.to_crs(3035).plot(color='grey', edgecolor='black', ax=ax)
        aoi.to_crs(3035).plot(facecolor='red', ax=ax, edgecolor='blue')
    elif region == 'globe':
        region_df.plot(color='grey', edgecolor='black', ax=ax)
        aoi.to_crs(4326).plot(facecolor='red', ax=ax, edgecolor='blue')

    #prepare country labels
    if label:
        # select the right dataframe column for labeling
        if region == 'EU':
            if zoom:
                label_field = 'NAME'
            else:
                label_field = 'ISO3'
            # also prepare dataframe for coordinate calculation
            region_df = region_df.to_crs(epsg=3035)
        elif region == 'globe':
            if zoom:
                label_field = 'SOVEREIGNT'
            else:
                label_field = 'SOV_A3'

        # create country label position
        region_df['coords'] = region_df['geometry'].apply(lambda x: x.representative_point().coords[:])
        region_df['coords'] = [coords[0] for coords in region_df['coords']]
        # label
        for idx, row in region_df.iterrows():
            plt.annotate(text=row[label_field], xy=row['coords'], horizontalalignment='center', color='y')

    plt.show()

def getUDFpath(UDFname: str | None = None) -> str:
    """
    Retrieve the normalized file path for a specified User-Defined Function (UDF). This function is designed
    to locate a UDF file within the `eo_processing.resources` package and return its normalized path. If the
    specified UDF is not provided or does not exist, appropriate exceptions will be raised.

    :param UDFname: The name of the User-Defined Function file to locate within the resources package.
    :return: The normalized string representation of the UDF file's path.
    :raises ValueError: If the UDF name is not provided (i.e., `None`).
    :raises FileNotFoundError: If the specified UDF file does not exist within the resources package.
    """
    if UDFname is None:
        raise ValueError('The UDF name must be provided')

    try:
        file_path = importlib_resources.files(eo_processing.resources).joinpath(UDFname)
        return normpath(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f'UDF {UDFname} does not exist')

def generate_unique_id(length: int = 25) -> str:
    """
    Generates a unique identifier (UUID) with a specified length. The UUID consists of
    a timestamp in the format YYYYMMDD, followed by a unique hashed suffix. The generated
    identifier length is adjustable within a specified range. Additionally, the suffix
    portion includes hyphens for better readability.

    Note: the standard 25 digit length (8 fixed and 14 variable hex digits) offers a virtually
    collision-proof design. A collision probability of ( ~0.00000324% ) means you don’t
    need to worry about collisions for practical purposes.

    :param length: The length of the resulting unique identifier, which includes the
        timestamp and the suffix. The value must be between 25 and 88, inclusive.
    :raises ValueError: If the length is not within the valid range of 25 to 88.
    :return: A string representing the unique identifier with the given length.
    """
    if length < 25 or length > 88:
        raise ValueError("length must be between 25 and 88, inclusive.")

    # Get current timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    # Calculate remaining length for unique suffix after timestamp (including hyphens)
    suffix_length = max(1, length - len(timestamp) - 1)
    # Generate UUID, hash it using SHA256
    raw_suffix = hashlib.sha256(uuid.uuid4().hex.encode()).hexdigest()
    # Add hyphens every 6 characters to format the suffix
    formatted_suffix = '-'.join([raw_suffix[i:i+6] for i in range(0, len(raw_suffix), 6)])[:suffix_length]
    # Combine timestamp and formatted suffix
    unique_id = f"{timestamp}-{formatted_suffix}"
    # Check for trailing hyphen and correct if necessary
    if unique_id.endswith('-'):
        # Replace trailing hyphen with the next character from raw_suffix
        replacement_character = raw_suffix[len(formatted_suffix.replace('-', ''))]
        unique_id = unique_id[:-1] + replacement_character
    return unique_id

def string_to_dict(string: str) -> dict:
    """
    Converts a string representation of a dictionary back into a dictionary.

    :param string: The string to convert.
    :return: A dictionary object.
    """
    try:
        # Attempt to load as JSON
        return json.loads(string)
    except json.JSONDecodeError:
        # Fallback to evaluating as a Python literal
        return literal_eval(string)

def convert_to_list(input_value: Union[str, list[str]]) -> list[str]:
    """
    Parse a string representation of a list or a list object into a Python list object.

    This function takes an input that can either be a string representing a list
    or an already existing list object. If the input is a string, it utilizes
    `string_to_dict` function to convert it into a Python list. If the input is already
    a list, it simply returns the input without any modifications.

    :param input_value: The input value to parse. It can be a string that represents
        a list or a list object.
    :return: A Python list object resulting from parsing the string or directly returning
        the input list if the input is already of list type.
    """
    if not isinstance(input_value, list):
        return string_to_dict(input_value)
    else:
        return input_value
