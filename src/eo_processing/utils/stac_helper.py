import pystac_client
import os
import logging
from urllib.request import urlopen
from io import BytesIO
from typing import Optional, List
import geopandas as gpd
import pandas as pd
import re

def get_stac_collection_url(collection_id: str, catalog_url: str = "https://catalogue.weed.apex.esa.int/") -> str:
    """
    Fetches the URL of a STAC (SpatioTemporal Asset Catalog) collection based on the provided 
    collection ID and catalog URL.

    This function utilizes the pystac_client library to interact with a STAC catalog and extract 
    the URL of a specific collection by its ID. If the collection cannot be found, the function 
    returns None.

    :param collection_id (str): The unique identifier of the STAC collection to retrieve.
    :param catalog_url (str): The URL of the STAC catalog to query. Defaults to 
                              'https://catalogue.weed.apex.esa.int/'.

    :return: The URL (as a string) of the requested STAC collection if found, or None if the 
             collection does not exist in the catalog.
    """
    client = pystac_client.Client.open(catalog_url)

    try: return client.get_collection(collection_id).self_href
    except:
        return None

def query_modelID_asset_url(model_id: str,
                    catalog_url: str ="https://catalogue.weed.apex.esa.int/", 
                    collection_id: str ="model-STAC") -> str:
    """
    Queries the URL of a specific asset for a given modelID from a STAC catalog.

    This function connects to the specified STAC catalog URL and searches for metadata
    associated with the given modelID and collectionID using CQL2 filters. It returns
    the URL of the asset identified as `model_valid_geometry`.

    :param model_id: (str) The unique identifier of the model to search for.
    :param catalog_url: (str) [Optional] The URL of the STAC catalog to query.
                        Defaults to "https://catalogue.weed.apex.esa.int/".
    :param collection_id: (str) [Optional] The ID of the collection in the STAC catalog
                          to search within. Defaults to "model-STAC-v2".

    :return: (str) The URL of the asset associated with the searched model ID.
    """
    client = pystac_client.Client.open(catalog_url)

    search = client.search(
        limit=20,
        collections=[collection_id],
        filter={"op": "=", "args": [{"property": "properties.modelID"}, model_id]},
        filter_lang="cql2-json",
    )
    item_collection = search.item_collection()
    if not item_collection.items:
        logging.error(f"No items found for model_id: {model_id}")
        raise ValueError(f"No items found for model_id: {model_id}")

    return item_collection.items[0].assets["model_valid_geometry"].href

def query_modelID_output_bands(model_id: str,
                               catalog_url: str ="https://catalogue.weed.apex.esa.int/",
                               collection_id: str ="model-STAC") -> List[str]:
    """
    Queries the output bands associated with the specified model ID from a STAC catalog.

    This function communicates with a STAC catalog to retrieve the model's output
    bands by searching for items using their model ID. It uses CQL2 filtering
    to perform the search and returns the output band names from the first matching
    item's properties.

    :param model_id: (str) The unique identifier for the model to query.
    :param catalog_url: (str) The URL of the STAC catalog to connect to. Defaults to
        "https://catalogue.weed.apex.esa.int/".
    :param collection_id: (str) The ID of the STAC collection to search within. Defaults
        to "model-STAC".
    :return: A list of output band names (List[str]) associated with the given
        model ID.
    :raises ValueError: If no items are found in the catalog for the provided model ID.
    """
    client = pystac_client.Client.open(catalog_url)

    search = client.search(
        limit=20,
        collections=[collection_id],
        filter={"op": "=", "args": [{"property": "properties.modelID"}, model_id]},
        filter_lang="cql2-json",
    )
    item_collection = search.item_collection()
    if not item_collection.items:
        logging.error(f"No items found for model_id: {model_id}")
        raise ValueError(f"No items found for model_id: {model_id}")

    return item_collection.items[0].properties["output_band_names"]

def query_proba_results(df_AOI: gpd.GeoDataFrame, collection_id:str, processing_year:int,
                         stac_url:str = 'https://catalogue.weed.apex.esa.int',
                         info_debug: bool = True, postprocess: bool = True) -> gpd.GeoDataFrame:
    """
    Queries and retrieves PROBA results intersecting with a given Area of Interest (AOI) from a STAC catalog.

    This function searches for PROBA results within the bounding box of the provided AOI and retrieves metadata
    and asset information from the specified STAC catalog. The retrieved data is reformatted and returned
    as a GeoDataFrame containing details about the intersecting PROBA tiles.

    Arguments:
    :param df_AOI: A GeoDataFrame representing the Area of Interest (AOI). The GeoDataFrame should contain geometry
        information and a coordinate reference system. If the coordinate reference system is not EPSG:4326,
        the function will reproject it to EPSG:4326.
    :param collection_id: A string specifying the collection identifier to search within the STAC catalog.
    :param processing_year: An integer specifying the year for which to retrieve PROBA results.
    :param stac_url: A string specifying the URL of the STAC catalog to query. Defaults to 'https://catalogue.weed.apex.esa.int'.
    :param info_debug: A boolean flag to enable or disable debug logging information. Defaults to True.
    :param postprocess: A boolean flag to determine whether to postprocess the retrieved data. Defaults to True.

    Returns:
    A GeoDataFrame containing metadata and details about the PROBA results that intersect with the AOI. The GeoDataFrame
    includes additional columns extracted from the metadata and asset information of the intersecting tiles, such as
    datetime, bounding box, tile ID, and others.

    Raises:
    ValueError: If no intersecting PROBA tiles are found in the STAC catalog for the specified collection.
    """
    if info_debug: print(f"get_modelID_asset_geometry_from_STAC")
    # convert AOI into BBOX in 4326
    if df_AOI.crs != "EPSG:4326":
        df_AOI_4326 = df_AOI.to_crs("EPSG:4326")
    else:
        df_AOI_4326 = df_AOI.copy()

    bbox_4326 = df_AOI_4326.total_bounds

    if info_debug: print(f"- searching for PROBA results in {bbox_4326}")
    if info_debug: print(f"- using STAC url: {stac_url}")
    if info_debug: print(f"- using collection id: {collection_id}")

    client = pystac_client.Client.open(stac_url)

    search = client.search(
        collections=[collection_id],
        bbox=bbox_4326,
        fields=["properties", "assets.openEO.href"],
    )

    results = []
    for item in search.items_as_dicts():
        results.append(
            [item['properties']['datetime'], item['properties']['proj:bbox'], item['properties']['proj:shape'],
             item['properties']['proj:code'], item['assets']['openEO']['href']])

    # build dataframe
    df_result = pd.DataFrame(results, columns=['datetime', 'file_bbox', 'file_shape', 'file_epsg', 'file_url'])
    if info_debug: print(f"- found {len(df_result)} intersecting PROBA tiles")
    # check if there are any results
    if df_result.empty:
        ValueError(f"No intersecting PROBA tiles found in the STAC ({collection_id}).")

    if postprocess:
        if info_debug: print(f"- postprocessing PROBA tiles")
        # split out from file_url important parts (file_name, tile_id, etc)
        df_result['basename'] = df_result['file_url'].apply(lambda x: os.path.basename(x))
        df_result[['project_typology', 'type', 'processing_year', 'tileID', 'model_short', 'inference_run_version',
                   'procesisng_start']] = df_result['basename'].str.split('_', expand=True)
        df_result['processing_year'] = df_result['processing_year'].str[-4:].astype(int)

        # first limit results to processing year
        df_result = df_result[df_result['processing_year'] == processing_year]

        # check if we have tiles smaller than our standard 20x20km grid - yes then make sure tile name is correct
        def extract_real_tileid(tileid_variant):
            """
            Extract the real tileID by removing trailing letter suffixes.
            Ensures the tileID ends with a number.
            Example: '48πXH34a' -> '48πXH34'
            """
            # Remove any trailing letters after the last digit
            return re.sub(r'[a-zA-Z]+$', '', tileid_variant)

        for idx, row in df_result.iterrows():
            if row.file_shape != [2000, 2000]:
                df_result.at[idx, 'tileID'] = extract_real_tileid(row.tileID)

        # now we can filter out spatial duplicates for same used modelID_short name
        # NOTE: that assumes that NEVER different inference runs of smae modelID were saved in same STAC catalog
        df_result = df_result.drop_duplicates(subset=['tileID', 'model_short'], keep='first')

        # last step. we have to prepare the output file_name.
        # Step 1: Check if we have duplicate tileIDs with different model_short values
        duplicate_tiles = df_result.groupby('tileID')['model_short'].apply(lambda x: list(x.unique())).to_dict()
        tiles_with_multiple_models = {k: v for k, v in duplicate_tiles.items() if len(v) > 1}

        if tiles_with_multiple_models:
            if info_debug: print(f" -- Found {len(tiles_with_multiple_models)} tiles with multiple model_short values")

            # Step 2: For tiles with multiple models, condense model_short names
            # Create a condensed model_short by combining unique values
            for tile, models in tiles_with_multiple_models.items():
                # Sort models to ensure consistent naming
                strata = [x.split('-')[0] for x in models]
                condensed_name = '-'.join(sorted(strata)) + '-' + '-'.join(models[0].split('-')[1:])
                # Update all rows for this tileID with the condensed name
                df_result.loc[df_result['tileID'] == tile, 'model_short'] = condensed_name

            # Step 3: Now remove duplicate tileIDs (keeping first occurrence)
            df_result = df_result.drop_duplicates(subset=['tileID'], keep='first')
        else:
            if info_debug: print(" -- No duplicate tileIDs found with different model_short values")
            # Still remove any exact duplicates
            df_result = df_result.drop_duplicates(subset=['tileID'], keep='first')

        # Step 4: Create the file_prefix column properly
        df_result['file_prefix'] = df_result.apply(
            lambda
                row: f"{row['project_typology']}_mece-cube_year{row['processing_year']}_{row['tileID']}_{row['model_short']}_{row['inference_run_version']}",
            axis=1
        )
        # filter to final needed
        df_result = df_result[['tileID', 'file_prefix']]

    return df_result

def get_modelID_asset_geometry_from_STAC(df_AOI: gpd.GeoDataFrame, typology_schema: str = 'IUCNGET',
                                     model_version: Optional[str]=None,
                                     stac_url:str = 'https://catalogue.weed.apex.esa.int',
                                     collection_id:str = 'model-STAC', info_debug: bool = True) -> gpd.GeoDataFrame:
    """
    Retrieves geometries associated with model IDs from a STAC (SpatioTemporal Asset Catalog) collection.

    This function queries a STAC catalog to find model records based on an input area of interest (AOI),
    typology schema, and optionally a specific model version. The geometries associated with the
    models are retrieved and returned as a GeoDataFrame.

    Parameters
    ----------
    df_AOI : gpd.GeoDataFrame
        The area of interest represented as a GeoDataFrame. Must have a valid coordinate reference
        system (CRS). If not in EPSG:4326, it will be converted to this CRS.
    typology_schema : str
        A typology schema filter (e.g., "IUCNGET") to apply to the STAC search. This helps filter
        results by their topology type. Default is "IUCNGET".
    model_version : Optional[str]
        A specific model version (e.g., "1.0") to filter the search results. If None, all versions
        are considered. Default is None.
    stac_url : str
        The URL of the STAC catalog to query. Default is 'https://catalogue.weed.apex.esa.int'.
    collection_id : str
        The ID of the STAC collection to search within. Default is 'model-STAC'.
    info_debug : bool
        if additional debug messages should be printed. Default is True.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoDataFrame containing the retrieved model IDs, their properties, and associated geometries.
        The GeoDataFrame is in EPSG:4326 CRS.

    Raises
    ------
    ValueError
        If no intersecting model IDs are found in the specified STAC collection.
    """
    if info_debug: print(f"get_modelID_asset_geometry_from_STAC")
    # convert AOI into BBOX in 4326
    if df_AOI.crs != "EPSG:4326":
        df_AOI_4326 = df_AOI.to_crs("EPSG:4326")
    else:
        df_AOI_4326 = df_AOI.copy()

    bbox_4326 = df_AOI_4326.total_bounds

    # init the pySTAC search
    if info_debug: print(f"- searching for modelIDs in {bbox_4326} with typology schema {typology_schema}")
    if info_debug: print(f"- using STAC url: {stac_url}")
    if info_debug: print(f"- using collection id: {collection_id}")
    client = pystac_client.Client.open(stac_url)

    search = client.search(
        collections=[collection_id],
        bbox=bbox_4326,
        filter={"op": "=", "args": [{"property": "properties.topology"}, typology_schema]},
        filter_lang="cql2-json",
        fields=["properties.modelID", "properties.topology", "properties.training_year", "properties.model_version", "properties.name_spatial_region", "properties.name_spatial_zone", "assets.model_valid_geometry"],
    )

    # run the search
    results = []
    for item in search.items_as_dicts():
        if model_version is None:
            results.append([item['properties']['modelID'], item['properties']['topology'],
                            item['properties']['training_year'], item['properties']['model_version'],
                            item['properties']['name_spatial_region'], item['properties']['name_spatial_zone'], item['assets']['model_valid_geometry']['href']])
        else:
            if str(item['properties']['model_version']) == model_version:
                results.append([item['properties']['modelID'], item['properties']['topology'],
                                item['properties']['training_year'], item['properties']['model_version'],
                                item['properties']['name_spatial_region'], item['properties']['name_spatial_zone'], item['assets']['model_valid_geometry']['href']])
    # build dataframe
    df_result = pd.DataFrame(results, columns=['modelID', 'typology', 'training_year', 'model_version', 'spatial_region', 'spatial_zone', 'asset'])
    if info_debug: print(f"- found {len(df_result)} intersecting modelIDs")
    # check if there are any results
    if df_result.empty:
        ValueError("No intersecting modelIDs found in the modelSTAC.")

    ## load the modelIS asset geometry and add
    if info_debug: print(f"- loading modelIDS geometries from STAC assets")
    # Initialize an empty list to store geometries
    geometries = []

    # Loop over each row in df_result
    for idx, row in df_result.iterrows():
        parquet_url = row['asset']

        try:
            # Download and read parquet file in memory
            with urlopen(parquet_url) as response:
                parquet_data = response.read()

            # Read from bytes
            temp_gdf = gpd.read_parquet(BytesIO(parquet_data))

            # Extract the geometry (assuming there's only one geometry per file)
            # If there are multiple geometries, you can use .union_all() or take the first one
            if len(temp_gdf) > 0:
                geometry = temp_gdf.geometry.iloc[0]
            else:
                geometry = None

            geometries.append(geometry)

            if info_debug: print(f"- Successfully loaded geometry for {row['modelID']}")

        except Exception as e:
            print(f"Error loading {row['modelID']}: {e}")
            geometries.append(None)

    # Add geometries as a new column to df_result
    df_result['geometry'] = geometries

    # Convert df_result to a GeoDataFrame
    return gpd.GeoDataFrame(df_result, geometry='geometry', crs='EPSG:4326')
