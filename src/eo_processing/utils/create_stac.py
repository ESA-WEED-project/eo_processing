import os
from datetime import datetime, timezone
import logging

import geopandas as gpd
import onnxruntime as ort
import pystac_client
from eo_processing.utils.helper import convert_to_list
from pystac import Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac import Asset, MediaType


def create_geometry_bbox(geom):
    """
    Simplify the geometry of a polygon by computing its minimum rotated bounding box.

    :param geom: Shapely geometry object, can be Polygon or MultiPolygon.
    :return: Shapely geometry object representing the bounding box, buffered by 0.
    """
    if geom.geom_type == "MultiPolygon":
        # Split multipolygon into single polygons and get their bounding boxes
        bbox_polygons = [poly.minimum_rotated_rectangle for poly in geom.geoms]
        # Merge bounding boxes and dissolve overlaps
        merged = gpd.GeoSeries(bbox_polygons).union_all()
        return merged.buffer(0)
    else:
        # If single polygon, just return its bounding box
        merged = geom.minimum_rotated_rectangle
        return merged.buffer(0)


def set_base_item_properties(row, model_region):
    """
    Set base item properties for a given row of the dataframe and model region.

    :param row: Row of the dataframe containing model information.
    :param model_region: Name of the model region.

    :return: Tuple containing the properties dictionary and temporal extent list.
    """
    # Convert temporal extent to list
    start_time = datetime(row.training_year, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(row.training_year + 1, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    properties = {
        "modelID": row.modelID,
        "topology": row.typology,
        "training_year": int(row.training_year),
        "model_version": row.version,
        "hierarchical_model": True,
        "ensemble_member_ids": convert_to_list(row.models),
        "name_spatial_region": model_region,
        "name_spatial_zone": row.zone,
        "temporal_extent": [
            start_time.isoformat(),
            end_time.isoformat(),
        ],
    }
    return properties, [start_time, end_time]


def get_model_properties(metadata_dict, model_urls, output_band_names) -> dict:
    """
    Get model properties from the metadata dictionary, including input features and covered classes.

    :param metadata_dict: Dictionary containing model metadata.
    :param model_urls: List of model URLs.
    :param output_band_names: List of output band names.

    :return: Dictionary containing model properties, including number of input features,
        names of input features, names of processed habitats, model URLs, and output band names.
    """
    first_with_status = None
    models_with_output = [
        (model_name, model_data)
        for model_name, model_data in metadata_dict.get("models", {}).items()
        if "model_loc" in model_data
    ]
    for model_name, model_data in models_with_output:
        if "status" in model_data:
            first_with_status = model_name
            _onnxpath = metadata_dict["models"][first_with_status]["model_loc"]
            # Create a Path object (handles both Windows and Linux)
            onnx_path = os.path.normpath(_onnxpath).replace("\\", "/")
            ort_session = ort.InferenceSession(
                onnx_path, providers=["CPUExecutionProvider"]
            )
            model_meta = ort_session.get_modelmeta()
            break
    else:
        logging.error("No model with status found in metadata_dict")
        raise ValueError("No model with status found in metadata_dict")

    custom_meta = model_meta.custom_metadata_map
    input_features = set(custom_meta.get("input_features", ""))
    covered_classes = set(custom_meta.get("covered_classes", ""))
    return {
        "number_total_input_features": len(input_features),
        "name_total_input_features": sorted(input_features),
        "name_processed_habitats": sorted(covered_classes),
        "model_urls": model_urls,
        "output_band_names": output_band_names,
    }


class InitializeCollection:
    """
    Class to add or update a stac collection and items.
    - If collection does not exist, it will be created with the provided title and description.
    - If collection exists, items will be added or updated based on their properties and spatial/temporal extents.
        - The class keeps track of which items need to be uploaded or edited in the collection,
          and whether the collection itself needs to be updated due to changes in spatial or temporal extents.
    """

    def __init__(
        self,
        collection_name: str,
        catalog_url: str,
        title: str = "ensemble ML models for VITO's hierarchical habitat mapping framework",
        description: str = "STAC catalog containing the ensemble models for the VITO hierarchical habitat mapping framework",
        license: str = "proprietary",
    ) -> None:
        """
        Initialize the collection.

        Check if collection exists in the catalog and creating it if it does not exist. Also
          initializes the list of items to upload and a flag to indicate if the collection needs to be updated.

        :param collection_name: Name of the collection to create or update.
        :param catalog_url: URL of the STAC catalog to connect to.
        :param title: Title of the collection
        :param description: Description of the collection.

        """
        self.catalog_url = catalog_url
        self.collection_name = collection_name
        self.collection_exists_in_catalog = False
        self.init_collection(collection_name, title, description, license)
        self.items_to_upload = []  # [item_id, upload/edit]  to stac
        self.upload_collection = False  # Should collection be updated in the stac

    def init_collection(
        self,
        collection_name,
        title: str,
        description: str,
        license: str,
    ):
        try:
            weed_catalog = pystac_client.Client.open(self.catalog_url)
            self.collection_client = weed_catalog.get_collection(collection_name)
            self.collection = Collection.from_dict(self.collection_client.to_dict())
            logging.info(f"collection {collection_name} exists in the weed-catalog")
            self.collection_exists_in_catalog = True
        except Exception as e:
            logging.warning(f"{collection_name} is {e}")
            if not title:
                logging.error("No title is defined for the collection")
                raise ValueError("No title is defined")
            logging.info("Creating Collection with random extents")
            extent = Extent(
                spatial=SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
                temporal=TemporalExtent(
                    [
                        [
                            datetime(
                                2017,
                                1,
                                1,
                                tzinfo=timezone.utc,
                            ),
                            None,
                        ]
                    ]
                ),
            )
            # Create a STAC Collection with extra fields
            self.collection = Collection(
                id=collection_name,
                title=title,
                description=description,
                extent=extent,
                license=license,
            )

    def create_item(
        self, item_id: str, geometry, bbox: list, temporal_extent: list, item_properties: dict
    ):
        """
        Create a STAC Item with the given parameters.
        :param item_id: ID of the item to create.
        :param geometry: Geometry of the item to create.
        :param bbox: Bounding box of the item to create.
        :param temporal_extent: Temporal extent of the item to create, as a list of two
            datetime objects (start and end).
        :param item_properties: Properties of the item to create, as a dictionary.

        :return: Item object with the given parameters.
        """
        return Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=None,
            start_datetime=temporal_extent[0],
            end_datetime=temporal_extent[1],
            properties=item_properties,
        )

    def create_asset(self, asset_href: str, media_type=MediaType.PARQUET):
        """Create an asset with the given href and media type.
        :param asset_href: Href of the asset to create.
        :param media_type: Mediatype of the asset, default is MediaType.PARQUET.

        :return: Asset object with the given href and media type.
        """
        return Asset(href=asset_href, media_type=media_type)

    def items_are_different(self, item, item_in_collection):
        """
        Compare two items to determine if they are different.

        :param item: Item object to compare.
        :param item_coll: Item object from the collection to compare against.

        :return: Tuple of two booleans:
             - First boolean indicates if the items are different.
             - Second boolean indicates if the collection should be updated due to spatial or temporal changes.
        """
        # Compare spatial extent using bounding box
        if not (item.bbox == item_in_collection.bbox):
            logging.info("Spatial extents may not be the same")
            return True, True

        # Compare properties
        for key, value in item.properties.items():
            if key in item_in_collection.properties:
                if not (item_in_collection.properties[key] == value):
                    if "datetime" in key:
                        logging.info(f"Temporal extents for key {key} are not the same")
                        return True, True  # Temporal extents can be different
                    else:
                        logging.info(f"Property values for key {key} are not the same")
                        return True, False
            else:
                logging.info(f"Property key is missing: {key}")
                return True, False  # Key does not exist
        return False, False

    def add_item_to_existing_collection(self, new_item: Item):
        """
        Add or update item in an existing collection.

        :param new_item: item to add or update in the collection.
        """
        if self.collection_exists_in_catalog:
            existing_item = self.collection_client.get_item(id=new_item.id)
            # item not in collection
            if not existing_item:
                self.collection.add_item(new_item)
                self.items_to_upload.append([new_item.id, "upload"])
                self.upload_collection = True
            else:
                # Check if the item is different
                item_different, collection_change = self.items_are_different(
                    new_item, existing_item
                )
                if collection_change:
                    logging.info("Collection needs to be changed")
                    self.upload_collection = True
                if item_different:
                    logging.info(f"{new_item.id} is different from the collection")
                    self.collection.remove_item(item_id=new_item.id)  # remove old item
                    self.collection.add_item(new_item)  # add new item
                    self.items_to_upload.append([new_item.id, "edit"])
                else:
                    logging.info(f"Item {new_item.id} is already present in the collection")
        else:
            self.collection.add_item(new_item)  # add new item
            self.items_to_upload.append([new_item.id, "upload"])
            self.collection.update_extent_from_items()

    def update_collection_extents(self):
        """update extents of the collection based on all items"""
        if not self.collection_exists_in_catalog or self.upload_collection:
            self.collection.update_extent_from_items()


def query_asset_based_on_model(catalog_url: str, collection_name: str, model_id: str):
    """
    Query the STAC catalog for an asset based on the model ID.
    :param catalog_url: URL of the STAC catalog to query.
    :param collection_name: Name of the collection to query within the catalog.
    :param model_id: Model ID to filter the items in the collection.
    :return: Href of the asset corresponding to the model ID.
    """
    catalog = pystac_client.Client.open(catalog_url)
    search = catalog.search(
        limit=20,
        collections=[collection_name],
        filter={"op": "=", "args": [{"property": "properties.modelID"}, model_id]},
        filter_lang="cql2-json",
    )
    item_collection = search.item_collection()
    if not item_collection.items:
        logging.error(f"No items found for model_id: {model_id}")
        raise ValueError(f"No items found for model_id: {model_id}")
    asset_href = item_collection.items[0].assets["model_valid_geometry"].href
    return asset_href
