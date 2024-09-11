import requests
import json
import pandas as pd
from eo_processing.utils.geoprocessing import reproj_bbox_to_ll


def catalogue_check_S1(orbit_direction, start, end, bbox):
    '''function to check the S1 catalogue to ensure that enough data is available if not enough data is available it will
    first try to also add other orbit direction and than does an new test
    param orbit_direction: orbit_direction
    param start: startdate of input data
    param start: enddata of inputdata
    param bbox: bbox in laea projection of extent
    return the orbit_direction that has enough data
    '''
    #standard settigns for amount of expected files per day
    MIN_VALUE_S1 = 1./12.
    percentage = 0.8
    latlon_box = reproj_bbox_to_ll(bbox)
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days
    if orbit_direction is not None:
        if orbit_direction not in ['ASCENDING', 'DESCENDING']:
            raise ValueError(
                f'`orbit_direction` value `{orbit_direction}` not recognized.')

        url=  f"https://datahub.creodias.eu/odata/v1/Products?$filter=Collection/Name eq 'SENTINEL-1' and OData.CSC.Intersects(area=geography'SRID=4326;{latlon_box}') "+ \
              f"and ContentDate/Start gt {start}T00:00:00.000Z and ContentDate/Start lt {end}T00:00:00.000Z and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' "+ \
              f"and att/OData.CSC.StringAttribute/Value eq '{orbit_direction}')&$top={100}"
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


def catalogue_check_S2(start, end, bbox):
    '''function to check the S2 catalogue to ensure that enough data is available
    param start: startdate of input data
    param start: enddata of inputdata
    param bbox: bbox in laea projection of extent
    return
    '''
    MIN_VALUE_S2 = 1./5.
    percentage = 0.8
    latlon_box = reproj_bbox_to_ll(bbox)
    #in 2017 S2B started in june/july so than only S2A sattelite
    if pd.to_datetime(start).year == '2017':
        MIN_VALUE_S2 = 1./10.
    temp_extent_days = (pd.to_datetime(end)-pd.to_datetime(start)).days

    nbr_files = count_amount_of_files('S2', latlon_box, start, end)
    if nbr_files < MIN_VALUE_S2*percentage*temp_extent_days:
        raise ValueError(f'not enough S2 images. Found {nbr_files} images.')

def count_amount_of_files(sentinel, latlon_box, start, end):
    '''function to count amount of files in the S2 and S1 catalogue creodias
    param sentinel: which sentinel data (S1 or S2)
    param latlon_box: polygon of bounding box in lat lon of extent
    param start: startdate of input data
    param start: enddata of inputdata
    return the amount images found in the creodias catalogue
    '''
    if sentinel == 'S1': satelite = "SENTINEL-1"
    elif sentinel == 'S2': satelite = "SENTINEL-2"
    else: raise ValueError(f"{sentinel} is not satellite for which this has been implemented")

    url=  f"https://datahub.creodias.eu/odata/v1/Products?$filter=Collection/Name eq '{satelite}' and OData.CSC.Intersects(area=geography'SRID=4326;{latlon_box}') and ContentDate/Start gt {start}T00:00:00.000Z and ContentDate/Start lt {end}T00:00:00.000Z&$top={100}"
    results = requests.get(url)
    json_data = json.loads(results.text)
    return len(json_data["value"])
