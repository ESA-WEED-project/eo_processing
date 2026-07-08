from __future__ import annotations
import requests
import json
import pandas as pd
from eo_processing.utils.geoprocessing import reproj_bbox_to_ll, bbox_of_PointsFeatureCollection
import geojson
from typing import TYPE_CHECKING
import pystac_client

if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def catalogue_check_S1(orbit_direction: str, start: str, end: str, bbox: openEO_bbox_format,
                       messages: bool=True, stop_processing: bool=True) -> str | None:
    """
    Checks the availability of Sentinel-1 imagery within a specified spatiotemporal extent and orbit
    direction constraints in the given catalog.

    This function determines if the number of Sentinel-1 images available in a specified bounding
    box and temporal range meets an expected threshold. It supports restricting results based on
    an orbit direction and provides summary messages or raises exceptions if conditions are not met.

    :param orbit_direction: (str) The orbit direction for filtering imagery. Must be 'ASCENDING'
                            or 'DESCENDING'. If None, both directions are checked.
    :param start: (str) The start date of the query in ISO 8601 format. Time is appended as 'T00:00:00.00Z'
                  if not provided.
    :param end: (str) The end date of the query in ISO 8601 format. Time is appended as 'T00:00:00.00Z'
                if not provided.
    :param bbox: (openEO_bbox_format) A bounding box specifying the geographic extent of the query
                 in the format supported by OpenEO.
    :param messages: (bool) If True, print messages summarizing the results. Default is True.
    :param stop_processing: (bool) If True, raises a ValueError when the imagery availability criteria
                            are not met. Default is True.

    :return: The specific orbit direction ('ASCENDING' or 'DESCENDING') if enough images are found
             for that orbit direction, otherwise None.
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
               f"att/OData.CSC.StringAttribute/Value eq '{orbit_direction}')&$top={100}")
        results = requests.get(url)
        json_data = json.loads(results.text)

        if len(json_data["value"]) < MIN_VALUE_S1*percentage*temp_extent_days:
            if messages:
                print(f'Not enough S1 images with orbit {orbit_direction}. \n' + \
                      f'Found {len(json_data["value"])} images.')
        else:
            if messages:
                print(f'Found {len(json_data["value"])} images with orbit direction {orbit_direction}.')
            return orbit_direction
    #use both orbits
    #check with both directions.
    nbr_files = count_amount_of_files('S1', latlon_box, start, end)
    if nbr_files < MIN_VALUE_S1*percentage*temp_extent_days:
        if stop_processing:
            raise ValueError(f'not enough S1 without orbit direction selection. \n'+ \
                             f'Found {nbr_files} images.')
    if messages:
        print(f'Found {nbr_files} images with orbit direction BOTH.')

    return None

def catalogue_check_S2(start: str, end: str, bbox: openEO_bbox_format,
                       messages: bool=True, stop_processing: bool=True) -> None:
    """
    Checks the availability of Sentinel-2 (S2) satellite images within a given time range and bounding box.
    The function determines if the number of available images meets the minimum threshold based on the temporal
    extent and year-specific conditions. If the threshold is not met, it raises an error or optionally prints
    the count of found images.

    :param start: (str) The start date of the search range in ISO 8601 format.
    :param end: (str) The end date of the search range in ISO 8601 format.
    :param bbox: (openEO_bbox_format) The bounding box in a geographical coordinate system.
    :param messages: (bool) Optional flag to print a message with the number of found images. Default is True.
    :param stop_processing: (bool) Optional flag to raise an exception if the number of images is below the threshold.
                            Default is True.

    :raises ValueError: If the number of Sentinel-2 images does not meet the minimum threshold and stop_processing is True.
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
        if stop_processing:
            raise ValueError(f'not enough S2 images. Found {nbr_files} images.')
    if messages:
        print(f'Found {nbr_files} S2 images.')

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
           f"gt {start} and ContentDate/Start lt {end}&$top={100}")
    results = requests.get(url)
    json_data = json.loads(results.text)
    return len(json_data["value"])

def catalogue_check_CDSE_S1(orbit_direction: str, start: str, end: str, bbox: openEO_bbox_format,
                            messages: bool=True, stop_processing: bool=True) -> str | None:
    """
    Checks the availability of Sentinel-1 satellite images for a given temporal and spatial extent
    and optionally filters them by orbit direction. If the specified criteria are not met, the
    function can optionally raise errors or display messages. VErsion to use with CDSE and pySTAC.

    :param orbit_direction: str. The orbit direction to filter on. Acceptable values are
                            'ASCENDING' or 'DESCENDING'. If None, both directions are considered.
    :param start: str. Start date for the temporal extent in ISO 8601 format (e.g., 'YYYY-MM-DD').
    :param end: str. End date for the temporal extent in ISO 8601 format (e.g., 'YYYY-MM-DD').
    :param bbox: openEO_bbox_format. Bounding box defining the spatial extent of the search region
                 in openEO format.
    :param messages: bool (default=True). Whether to print messages about the results of the search.
    :param stop_processing: bool (default=True). Whether to raise an error if the number of images
                            found does not meet the required criteria.

    :return: str | None. The orbit direction that satisfies the criteria if applicable. Returns
             None if no specific orbit direction was found to meet the requirements or if no orbit
             direction was specified.

    :raises ValueError: If `orbit_direction` is not one of the acceptable values ('ASCENDING' or
                        'DESCENDING'), or if not enough Sentinel-1 images are found to meet the
                        required criteria when `stop_processing` is True.
    """
    #quickfix on dates that are in date format
    if not 'Z' in start:
        start = start + "T00:00:00.00Z"
    if not 'Z' in end:
        end = end + "T00:00:00.00Z"

    # set the minimum number of S1 images with two satellites
    MIN_VALUE_S1 = 1./12.
    # the percentage of observations we want to have at least
    percentage = 0.8
    # convert the openEO bbox format to a shapely Polygon
    latlon_box = reproj_bbox_to_ll(bbox)
    # number of days in the temporal extent
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days

    # run the PySTAC-client search
    # Connect to the Copernicus Data Space Ecosystem STAC API
    #catalog_url = "https://catalogue.dataspace.copernicus.eu/stac"
    catalog_url = "https://stac.dataspace.copernicus.eu/v1/"
    client = pystac_client.Client.open(catalog_url)

    # if we have an orbit_direction given we have to test that first
    if orbit_direction is not None:
        if orbit_direction not in ['ASCENDING', 'DESCENDING']:
            raise ValueError(
                f'`orbit_direction` value `{orbit_direction}` not recognized.')

        search = client.search(
            collections=['sentinel-1-grd'],
            bbox=list(latlon_box.bounds),
            datetime=f"{start}/{end}",
            fields=["id", "properties.datetime"],
            query={"sat:orbit_state": {"eq": f"{orbit_direction.lower()}"},
                   "sar:polarizations": {"eq": ["VV", "VH"]},
                   },
        )

        # get the dates of all found matches
        results = []
        for item in search.items_as_dicts():
            results.append(item['properties']['datetime'])

        # count the number of unique dates on which we have observations (resolved tile overlap)
        df = pd.DataFrame(results, columns=['date'])
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].apply(lambda x: x.date())
        nbr_files = df['date'].nunique()

        if nbr_files < MIN_VALUE_S1*percentage*temp_extent_days:
            if messages:
                print(f'Not enough S1 images with orbit {orbit_direction}. \n' + \
                      f'Found {nbr_files} images.')
            # jump back to check with BOTH orbits
            pass
        else:
            if messages:
                print(f'Found {nbr_files} images with orbit direction {orbit_direction}.')
            return orbit_direction
    #use both orbits -> check with both directions.

    search = client.search(
        collections=['sentinel-1-grd'],
        bbox=list(latlon_box.bounds),
        datetime=f"{start}/{end}",
        fields=["id", "properties.datetime"],
        query={"sar:polarizations": {"eq": ["VV", "VH"]} },
    )

    # get the dates of all found matches
    results = []
    for item in search.items_as_dicts():
        results.append(item['properties']['datetime'])

    # count the number of unique dates on which we have observations (resolved tile overlap)
    df = pd.DataFrame(results, columns=['date'])
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].apply(lambda x: x.date())
    nbr_files = df['date'].nunique()

    if nbr_files < MIN_VALUE_S1*percentage*temp_extent_days:
        if stop_processing:
            raise ValueError(f'not enough S1 without orbit direction selection. \n'+ \
                             f'Found {nbr_files} images.')
    if messages:
        print(f'Found {nbr_files} images with orbit direction BOTH.')

    return None

def catalogue_check_CDSE_S2(start: str, end: str, bbox: openEO_bbox_format,
                            messages: bool=True, stop_processing: bool=True) -> None:
    """
    Checks the availability of Sentinel-2 images for a specified temporal and spatial extent
    using the Copernicus Data Space Ecosystem STAC API.

    This function evaluates whether the available number of Sentinel-2 (S2) images falls
    below a minimum threshold for an expected observation frequency based on the specified
    temporal extent and spatial bounding box. The function can optionally stop further processing
    if the images are insufficient or print a message about the number of images found.

    :param start: (str) The start date of the temporal extent in "YYYY-MM-DD" format.
    :param end: (str) The end date of the temporal extent in "YYYY-MM-DD" format.
    :param bbox: (openEO_bbox_format) The spatial bounding box in an openEO-compatible format.
    :param messages: (bool) A flag to indicate whether to print the number of images found. Defaults to True.
    :param stop_processing: (bool) A flag to indicate whether to raise an error and halt processing if
        the number of images is insufficient. Defaults to True.

    :raises ValueError: If the number of available S2 images is less than the required threshold and
        `stop_processing` is set to True.
    """
    #quickfix on dates that are in date format
    if not 'Z' in start:
        start = start + "T00:00:00.00Z"
    if not 'Z' in end:
        end = end + "T00:00:00.00Z"

    # set the minimum number of S2 images with two satellites (5 daily observation)
    MIN_VALUE_S2 = 1./5.
    #in 2017 S2B started in june/july so than only S2A sattelite
    if pd.to_datetime(start).year == '2017':
        MIN_VALUE_S2 = 1./10.

    # the percentage of observations we want to have at least
    percentage = 0.8
    # convert the openEO bbox format to a shapely Polygon
    latlon_box = reproj_bbox_to_ll(bbox)
    # number of days in the temporal extent
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days

    # run the PySTAC-client search
    # Connect to the Copernicus Data Space Ecosystem STAC API
    #catalog_url = "https://catalogue.dataspace.copernicus.eu/stac"
    catalog_url = "https://stac.dataspace.copernicus.eu/v1/"
    client = pystac_client.Client.open(catalog_url)

    search = client.search(
        collections=['sentinel-2-l2a'],
        bbox=list(latlon_box.bounds),
        datetime=f"{start}/{end}",
        fields=["id", "properties.datetime"],
        #query={"eo:cloud_cover": {"lt": 95}},
    )

    # get the dates of all found matches
    results = []
    for item in search.items_as_dicts():
        results.append(item['properties']['datetime'])

    # count the number of unique dates on which we have observations (resolved tile overlap)
    df = pd.DataFrame(results, columns=['date'])
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].apply(lambda x: x.date())
    nbr_files = df['date'].nunique()

    # run the test
    if nbr_files < MIN_VALUE_S2*percentage*temp_extent_days:
        if stop_processing:
            raise ValueError(f'not enough S2 images. Found {nbr_files} images.')
    if messages:
        print(f'Found {nbr_files} S2 images.')

def catalogue_check_CDSE_S1_FeatureCollection(points_geometry: geojson.FeatureCollection, start_orbit: str,
                                              start: str, end: str) -> str | None:
    """
    Checks the availability of satellite data within a defined geometry and time range.

    This function takes a geography feature collection and a time range, then determines
    satellite data availability by performing an orbit check. It calculates a bounding
    box from the given feature collection to perform the check.

    :param points_geometry: (geojson.FeatureCollection) The geometry as a feature
        collection used to define the spatial extent.
    :param start_orbit: (str) The starting orbit identifier for the data query.
    :param start: (str) The start of the time range for the data query in ISO 8601 format.
    :param end: (str) The end of the time range for the data query in ISO 8601 format.

    :return: Returns a string containing the result of the orbit check if data is available.
        Returns None if no data is found or the check fails.
    """
    # need openEO BBOX for orbit check
    bbox = bbox_of_PointsFeatureCollection(points_geometry)
    # now we run the orbit check and give result back
    return catalogue_check_CDSE_S1(start_orbit, start, end, bbox, messages=False, stop_processing=False)
