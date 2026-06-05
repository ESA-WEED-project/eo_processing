import pystac_client


def get_stac_collection_url(collection_id):
    """Fetches the url of the STAC collection with the given collection_id. And returns None if not found."""
    client = pystac_client.Client.open("https://catalogue.weed.apex.esa.int/")

    try: return client.get_collection(collection_id).self_href
    except:
        return None


def query_asset_url(model_id):
    """Use model-id to get the parquet asset link"""
    client = pystac_client.Client.open("https://catalogue.weed.apex.esa.int/")

    search = client.search(
        limit=20,
        collections=["model-STAC-v2"],
        filter={"op": "=", "args": [{"property": "properties.modelID"}, model_id]},
        filter_lang="cql2-json",
    )
    item_collection = search.item_collection()
    return item_collection.items[0].assets["model_valid_geometry"].href