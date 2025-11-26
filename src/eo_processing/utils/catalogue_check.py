from __future__ import annotations
import requests
import json
import pandas as pd
from eo_processing.utils.geoprocessing import reproj_bbox_to_ll
import geojson
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def catalogue_check_S1(orbit_direction: str, start: str, end: str, bbox: openEO_bbox_format) -> str | None:
    """
    Checks the availability of Sentinel-1 images based on the specified orbit direction, date range,
    and bounding box. Validates the amount of data available against a predefined minimum threshold.

    :param orbit_direction: The direction of the orbit, either 'ASCENDING' or 'DESCENDING'. If not
        specified, the function will check data for both orbit directions. Must comply with the
        specified format.
    :param start: The start date of the desired time range in ISO 8601 date format (YYYY-MM-DD) or 'YYYY-MM-DDThh:mm:ss.SSZ'..
    :param end: The end date of the desired time range in ISO 8601 date format (YYYY-MM-DD) or 'YYYY-MM-DDThh:mm:ss.SSZ'..
    :param bbox: The bounding box of the area of interest in openEO_bbox_format. It will be reprojected
        to a latitude and longitude format for API queries.
    :return: Returns the specified orbit direction if sufficient Sentinel-1 images for the given
        direction are available. Returns `None` if the checks for both orbit directions combined are
        sufficient or if orbit direction was not specified. Raises an error if the amount of data
        does not meet the threshold.
    """
    #standard settigns for amount of expected files per day
    #quickfix on dates that are in date format
    if not 'Z' in start:
        start = start + "T00:00:00.00Z"
    if not 'Z' in end:
        end = end + "T00:00:00.00Z"

    MIN_VALUE_S1 = 1./12.
    percentage = 0.8
    latlon_box = reproj_bbox_to_ll(bbox)
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days
    if orbit_direction is not None:
        if orbit_direction not in ['ASCENDING', 'DESCENDING']:
            raise ValueError(
                f'`orbit_direction` value `{orbit_direction}` not recognized.')

        url=  (f"https://datahub.creodias.eu/odata/v1/Products?$filter=Collection/Name eq 'SENTINEL-1' and "
               f"OData.CSC.Intersects(area=geography'SRID=4326;{latlon_box}') and ContentDate/Start gt "
               f"{start} and ContentDate/Start lt {end} and "
               f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' and "
               f"att/OData.CSC.StringAttribute/Value eq '{orbit_direction}')&$top={1000}")
        results = requests.get(url)
        json_data = json.loads(results.text)

        if len(json_data["value"]) < MIN_VALUE_S1*percentage*temp_extent_days:
            print(f'Not enough S1 images with orbit {orbit_direction}. \n' + \
                  f'Found {len(json_data["value"])} images.')
        else: return orbit_direction
    #use both orbits
    #check with both directions.
    nbr_files = count_amount_of_files('S1', latlon_box, start, end)
    if nbr_files < MIN_VALUE_S1*percentage*temp_extent_days:
        raise ValueError(f'not enough S1 without orbit direction selection. \n'+ \
                         f'Found {nbr_files} images.')

    return None

def catalogue_check_S2(start: str, end: str, bbox: openEO_bbox_format) -> None:
    """
    Check the availability of Sentinel-2 (S2) satellite images for a given time period
    and bounding box. The function calculates the expected minimum number of images
    and raises a ValueError if the actual count is insufficient.

    :param start: The start date of the time period, in the format 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ss.SSZ'.
    :param end: The end date of the time period, in the format 'YYYY-MM-DD'or 'YYYY-MM-DDThh:mm:ss.SSZ'..
    :param bbox: The bounding box defining the spatial extent, must be in openEO
        bounding box format.
    :return: None
    :raises ValueError: If the number of available Sentinel-2 images is less than the
        required minimum threshold.
    """
    MIN_VALUE_S2 = 1./5.
    percentage = 0.8
    latlon_box = reproj_bbox_to_ll(bbox)
    #in 2017 S2B started in june/july so than only S2A sattelite
    if pd.to_datetime(start).year == '2017':
        MIN_VALUE_S2 = 1./10.
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days

    #quickfix on dates that are in date format
    if not 'Z' in start:
        start = start + "T00:00:00.00Z"
    if not 'Z' in end:
        end = end + "T00:00:00.00Z"


    nbr_files = count_amount_of_files('S2', latlon_box, start, end)
    if nbr_files < MIN_VALUE_S2*percentage*temp_extent_days:
        raise ValueError(f'not enough S2 images. Found {nbr_files} images.')

mece_sequence = [[[9], [15], [21], [27], [34], [40], [49], [54], [57], [62], [69], [73], [82]],
                 [[86], [89], [93], [99], [102], [110], [113], [117], [122], [126], [130], [135], [138]],
                 [[142], [146], [150], [155], [31], [162], [158], [171], [175], [179], [183], [186], [190]],
                 [[194], [198], [201], [205], [208], [214], [218], [222], [226], [230], [233], [237], [240]]]
mece_shape = [(26,2), (0,19)]

def count_amount_of_files(sentinel: str, latlon_box: geojson.Feature, start: str, end: str) -> int | None:
    """
    Counts the number of files available for a given satellite, within a specified
    geographic area, and between given start and end dates. The function queries
    the CREODIAS OData API based on the satellite type, provided geographical box,
    and the time period of interest. It interprets the server's JSON response to
    count and return the total number of available files.

    :param sentinel: Identifier for the satellite, either 'S1' for SENTINEL-1 or
        'S2' for SENTINEL-2. Raises a ValueError for unsupported satellite types.
    :param latlon_box: A GeoJSON Feature specifying the bounding box for the query.
    :param start: The start date of the time range for querying, formatted as a
        string (YYYY-MM-DD).
    :param end: The end date of the time range for querying, formatted as a
        string (YYYY-MM-DD).
    :return: The count of available files matching the criteria or None if the
        request does not return valid data.
    """
    if sentinel == 'S1': satelite = "SENTINEL-1"
    elif sentinel == 'S2': satelite = "SENTINEL-2"
    else: raise ValueError(f"{sentinel} is not satellite for which this has been implemented")

    url=  (f"https://datahub.creodias.eu/odata/v1/Products?$filter=Collection/Name eq '{satelite}' and "
           f"OData.CSC.Intersects(area=geography'SRID=4326;{latlon_box}') and ContentDate/Start "
           f"gt {start} and ContentDate/Start lt {end}&$top={1000}")
    results = requests.get(url)
    json_data = json.loads(results.text)
    return len(json_data["value"])
