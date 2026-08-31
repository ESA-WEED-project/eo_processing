import pystac_client


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
                    collection_id: str ="model-STAC-v2") -> str:
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
    return item_collection.items[0].assets["model_valid_geometry"].href