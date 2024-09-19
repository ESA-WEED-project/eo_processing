# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
some functions to easy connection and pipeline work
"""
import openeo
import requests
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import box
import importlib.resources as importlib_resources
import eo_processing.resources
from os.path import normpath

def init_connection(provider: str) -> openeo.Connection :
    """ Warper to select the correct entry point based on the provider

    :param provider: str, backend like CDSE, terrascope, terascope-development
    :return: openeo.Connection object
    """
    if provider == 'terrascope':
        connection = openeo.connect("https://openeo.vito.be").authenticate_oidc()
    elif provider == 'development':
        connection = openeo.connect("https://openeo-dev.vito.be").authenticate_oidc()
    elif provider == 'cdse':
        connection = openeo.connect(url="openeo.dataspace.copernicus.eu").authenticate_oidc()
    else:
        print('currently no specific connections to backends like creodias and sentinelhub are setup.')
        print('use standard entry point')
        connection = openeo.connect("https://openeo.cloud").authenticate_oidc()
    return connection

def location_visu(bbox, zoom: bool = False, region: str = 'EU', label: bool = True):
    """ creates a figure of the location of the AOI in Europe/Globe

    :param bbox: a bounding box dictionary with south,west,north,east and crs
    :param zoom: zoom in to intersected countries
    :param region: str, in which context to show the AOI (EU, globe)
    :param label: bool, if True will label the figure with country names
    :return: plt object (directly viewable in jupyter notebook)
    """
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

    # bring AOI into GeoDataFrame
    aoi = gpd.GeoDataFrame({"id": 1, "geometry": [box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])]})
    aoi.crs = bbox['crs']

    # zoom in
    if zoom:
        region_df = region_df[region_df.intersects(aoi.to_crs(4326).iloc[0].geometry)]

    # create the figure
    fig, ax = plt.subplots()
    if region == 'EU':
        region_df.to_crs(3035).plot(color='grey', edgecolor='black', ax=ax)
        aoi.to_crs(3035).plot(color='red', ax=ax)
    elif region == 'globe':
        region_df.plot(color='grey', edgecolor='black', ax=ax)
        aoi.to_crs(4326).plot(color='red', ax=ax)

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


def getUDFpath(UDFname: str = None) -> normpath:
    """ hands back the absolute path for a UDF script stored in the resources folder
    :param UDFname: name of the UDF to get the path
    :return: absolute file path to the UDF
    """
    if UDFname is None:
        raise ValueError('The UDF name must be provided')

    try:
        file_path = importlib_resources.files(eo_processing.resources).joinpath(UDFname)
        return normpath(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f'UDF {UDFname} does not exist')