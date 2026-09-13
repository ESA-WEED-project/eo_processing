import pystac_client
import logging
from urllib.request import urlopen
from io import BytesIO
from typing import Optional
import geopandas as gpd
import pandas as pd

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
