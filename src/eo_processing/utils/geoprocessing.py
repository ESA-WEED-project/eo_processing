from __future__ import annotations
import os.path
from os import PathLike
import pyproj
from shapely.geometry import Polygon
from shapely.geometry import box
from shapely.geometry import shape
from shapely.ops import unary_union
import geopandas as gpd
import numpy as np
import geojson
import json
from typing import Union, Tuple, Optional, TYPE_CHECKING
from eo_processing.utils.mgrs import LL_2_UTM, floor_to_nearest_5, UTM_2_LL, UTM_2_MGRSid10, UTM_2_grid20id
from urllib3.util.url import parse_url
import eo_processing.resources
import fsspec
try:
    import importlib.resources as importlib_resources
except:
    import importlib_resources

if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format
    from eo_processing.utils.storage import WEED_storage

def laea20km_id_to_extent(laea_id: str) -> openEO_bbox_format:
    """
    Converts an LAEA 20km grid cell identifier to its spatial extent.

    This function interprets the given LAEA 20km grid cell identifier, which
    provides geographical location information in the form of coordinates in
    meters divided into cells. It calculates and returns the bounding box
    (spatial extent) of the specified grid cell in the LAEA (EPSG:3035) projection
    coordinate reference system. The identifier must follow the specific format
    starting with 'E', containing coordinates split by 'N'.

    :param laea_id: The LAEA grid cell identifier string in the form 'E<value>N<value>',
        where <value> represents numeric coordinates in the grid system.
    :return: A dictionary representing the spatial extent of the identified grid cell
        with keys `east`, `south`, `west`, `north`, and `crs`.
    """

    assert laea_id[0] == 'E'
    assert 'N' in laea_id
    parts = laea_id.lstrip('E').split('N')
    west = int(parts[0]) * 10000
    south = int(parts[1]) * 10000
    # we now have lower-left corner
    return {
        'east': west + 20000,
        'south': south,
        'west': west,
        'north': south + 20000,
        'crs': 'EPSG:3035'
    }

def AOI_tiler(AOI: Union[gpd.GeoDataFrame, openEO_bbox_format, geojson.GeoJSON, Polygon, PathLike[str]],
              tiling_grid: Union[str, PathLike[str], gpd.GeoDataFrame] = 'global',
              merge_columns: Optional[list] = None,
              storage: Optional[WEED_storage] = None) -> gpd.GeoDataFrame:
    """
    Splits an Area of Interest (AOI) into grid tiles using a provided tiling grid and returns the resulting
    geospatial dataframe compatible with the AOI bounds. The function takes various formats for AOI and tiling grid inputs,
    handles reprojection, and can merge additional columns from the AOI if requested. This method is designed to support
    EU or global tiling grids or custom grid files, with an optional storage object for remote data access.

    :param AOI: The Area of Interest to be tiled, which can be provided as various formats including a
        GeoDataFrame, openEO bounding box dictionary, a geojson object, a Path to local file on disk (geoparquet,
        GeoJson, or other GeoPandas format), or a shapely Polygon.
    :param tiling_grid: The tiling grid to be used for generating tiles within the AOI bounds. This can be a string such as
        "EU" or "global", a file path to local geospatial data in geoparquet, geoJSON or geopandas file,
        a URL pointing to a geoparquet resource, or a GeoDataFrame.
    :param merge_columns: An optional list of column names in the AOI GeoDataFrame that will be merged with the grid tiles.
    :param storage: An optional storage object (e.g., WEED_storage) required when loading a global tiling grid using remote resources.

    :return: The resulting GeoDataFrame containing the intersected and processed tiling grid for the AOI.
    """
    from eo_processing.config.data_formats import openEO_bbox_format

    # load the AOI and make sure it is in EPSG: 4326
    if isinstance(AOI, gpd.GeoDataFrame):
        gdf_aoi = AOI.copy()
        gdf_aoi = gdf_aoi.to_crs('EPSG:4326')
    elif (isinstance(AOI, dict)) and (set(AOI.keys()) == set(openEO_bbox_format.__annotations__.keys())):
        gdf_aoi = gpd.GeoDataFrame(geometry=[reproj_bbox_to_ll(AOI, densify=True)])
        gdf_aoi.crs = 'EPSG:4326'
    elif is_geojson(AOI):
        gdf_aoi = gpd.GeoDataFrame.from_features(AOI['features'])
        gdf_aoi.crs = 'EPSG:4326'
    elif isinstance(AOI, Polygon):
        gdf_aoi = gpd.GeoDataFrame(geometry=[AOI])
        gdf_aoi.crs = 'EPSG:4326'
    elif isinstance(os.fspath(AOI), str):
        # we load geoparquet or geopandas or geoJSON from disk
        try:
            gdf_aoi = gpd.read_parquet(AOI)
            gdf_aoi = gdf_aoi.to_crs('EPSG:4326')
        except:
            try:
                gdf_aoi = gpd.read_file(AOI)
                gdf_aoi = gdf_aoi.to_crs('EPSG:4326')
            except:
                raise ValueError('If a path to a local file was supplied, it must be either a geoparquet or '
                                 'geopandas file or a geoJSON file. ')
    else:
        raise ValueError('AOI must be either a geopandas GeoDataFrame, openEO bbox dict, GeoJSON, GeoJSON Path '
                         'or shapely Polygon')

    # get the total bounds of the AOI for spatial filtering of tiling grid
    total_bbox = tuple(gdf_aoi.total_bounds)
    minx, miny, maxx, maxy = gdf_aoi.total_bounds
    # Create a Polygon from the bounding box coordinates
    bbox_polygon = Polygon([
        (minx, miny),
        (minx, maxy),
        (maxx, maxy),
        (maxx, miny),
        (minx, miny)  # Close the polygon
    ])

    #load the correct tiling_grid
    if isinstance(tiling_grid, gpd.GeoDataFrame):
        tiling_grid_gdf = tiling_grid.copy()
        tiling_grid_gdf = tiling_grid_gdf.to_crs('EPSG:4326')
        tiling_grid_gdf = tiling_grid_gdf[tiling_grid_gdf.geometry.intersects(bbox_polygon)]
    elif (isinstance(tiling_grid, str)) and (tiling_grid in ['EU', 'global']):
        if tiling_grid == 'EU':
            grid_path = importlib_resources.files(eo_processing.resources).joinpath('LAEA-20km_add-info.gpkg')
            tiling_grid_gdf = gpd.read_file(os.path.normpath(grid_path), bbox=total_bbox)
        elif tiling_grid == 'global':
            from eo_processing.utils.storage import WEED_storage
            if isinstance(storage, WEED_storage):
                # load
                tiling_grid_gdf = storage.get_gdrive_gdf('global_terrestrial_UTM20k_grid_v2.gpkg',
                                                       filter_bbox=total_bbox)
            else:
                raise ValueError('For loading "global" tiling grid you need to provide a storage object')
    elif isinstance(tiling_grid, str):
        # check if we have a string, a path to file or a link to file
        if parse_url(tiling_grid).scheme in ['http', 'https']:
            # we load a geoparquet into geopandas
            with fsspec.open(tiling_grid, mode='rb') as f:
                tiling_grid_gdf = gpd.read_parquet(f)
            tiling_grid_gdf = tiling_grid_gdf.to_crs('EPSG:4326')
            tiling_grid_gdf = tiling_grid_gdf[tiling_grid_gdf.geometry.intersects(bbox_polygon)]
        elif isinstance(os.fspath(tiling_grid), str):
            # we load geoparquet or geopandas or geoJSON from disk
            try:
                tiling_grid_gdf = gpd.read_parquet(tiling_grid)
                tiling_grid_gdf = tiling_grid_gdf.to_crs('EPSG:4326')
                tiling_grid_gdf = tiling_grid_gdf[tiling_grid_gdf.geometry.intersects(bbox_polygon)]
            except:
                try:
                    tiling_grid_gdf = gpd.read_file(tiling_grid)
                    tiling_grid_gdf = tiling_grid_gdf.to_crs('EPSG:4326')
                    tiling_grid_gdf = tiling_grid_gdf[tiling_grid_gdf.geometry.intersects(bbox_polygon)]
                except:
                    raise ValueError('If a path to a local file was supplied, it must be either a geoparquet or '
                                     'geopandas file or a geoJSON file. ')
        else:
            raise ValueError('tiling grid must be a valid URL to geoparquet file or a path to local geoparquet, '
                             'geoparquet or geoJSON file.')
    else:
        raise ValueError('I tried my best. tiling_grid must be either "EU" or "global" string. '
                         'Or a valid url to a geoparquet; or path to local geopandas, geoparquet or geoJSON file')

    # intersect to get AOI tiles dataframe
    # ToDO: the filtering via the CLIP is way faster then to do an intersection in GeoPandas. optimize this behavior.
    if 'grid20id' in tiling_grid_gdf.columns:
        result_gdf = tiling_grid_gdf[tiling_grid_gdf.grid20id.isin(tiling_grid_gdf.clip(gdf_aoi).grid20id.unique().tolist())]
    elif 'name' in tiling_grid_gdf.columns:
        result_gdf = tiling_grid_gdf[tiling_grid_gdf.name.isin(tiling_grid_gdf.clip(gdf_aoi).name.unique().tolist())]
    else:
        try:
            print('WARNING: the column "grid20id" or "name" was not found in the tiling grid. '
                  'slower workflow for intersection is used.')
            result_gdf = tiling_grid_gdf[tiling_grid_gdf.intersects(gdf_aoi.union_all())]
        except:
            raise ValueError('tiling_grid must contain either "grid20id" or "name" column for proper filtering.')

    # adding data columns from AOI
    if merge_columns is not None:
        # now we check if the needed columns even exist in the AOI dataframe
        existing_columns = [col for col in merge_columns if col in gdf_aoi.columns]
        if existing_columns:
            # spatial join
            result_gdf = gpd.sjoin(result_gdf, gdf_aoi[existing_columns + ['geometry']],
                                   how="left", predicate="intersects")
        else:
            print(f'WARNING: non of the requested columns to merge from the AOI to job dataframe exist')

    if 'bbox_dict' not in result_gdf.columns:
        print('WARNING: the column "bbox_dict" was not found in the tiling grid. Automatic spatial extent '
              'generation in the job_function of the JobManager will be not possible')

    # reset the index
    return result_gdf.reset_index()

def reproj_bbox_to_ll(bbox: openEO_bbox_format, buffer: bool = False, densify: bool = False,
                      return_geojson: bool = False) -> Union[Polygon, geojson.Feature]:
    """
    Transform a bounding box from its original CRS to EPSG:4326, with optional
    buffering, densification, and return format customization. This function
    utilizes the `pyproj` library to reproject bounding box coordinates and
    generate a polygon representing the bounding box in EPSG:4326.

    :param bbox: Input bounding box in `openEO_bbox_format` containing its
        coordinates and CRS. Must include 'crs', 'west', 'east', 'north',
        and 'south' fields.
    :param buffer: Boolean flag to indicate whether the polygon should
        be buffered by 0.003 degrees.
    :param densify: Boolean flag to indicate whether the polygon should
        undergo densification with a simplification of 0.05 degrees.
    :param return_geojson: Boolean flag to specify whether the output
        should be a GeoJSON Feature instead of a shapely `Polygon`.
    :return: The transformed polygon in `EPSG:4326` as a shapely `Polygon`
        or a GeoJSON Feature based on the `return_geojson` flag.
    """
    # Create the PyProj transformer objects for bbox['crs'] and EPSG 3246
    transformer_to_4326 = pyproj.Transformer.from_crs(bbox['crs'], 'EPSG:4326', always_xy=True)

    # Transform the bounding box coordinates from input CRS to  EPSG:3246
    minx, maxy = transformer_to_4326.transform(bbox['west'], bbox['north'])
    maxx, miny = transformer_to_4326.transform(bbox['east'], bbox['south'])

    # Create a polygon using the transformed coordinates in EPSG 3246
    polygon_4326 = Polygon([(minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny), (minx, maxy)])

    if buffer:
        polygon_4326 = polygon_4326.buffer(0.003)

    if densify:
        polygon_4326 = polygon_4326.simplify(0.05)

    if return_geojson:
        return geojson.Feature(geometry=polygon_4326, properties={})

    return polygon_4326

def bbox_area(bbox: openEO_bbox_format, only_number: bool = False) -> float | None:
    """
    Calculates the area of a bounding box (bbox) in square kilometers (km²). This function supports two modes of
    operation. If `only_number` is set to True, it returns the calculated area as a number. Otherwise, it prints
    the area of the bounding box to the console.

    :param bbox: Dictionary defining the bounding box with keys `'west'`, `'south'`, `'east'`, `'north'`, and `'crs'`.
        - `'west'`, `'south'`, `'east'`, `'north'`: Float values representing the geographical bounds.
        - `'crs'`: Coordinate Reference System (CRS) of the bounding box.
    :param only_number: Boolean flag to control output. When set to True, the method returns the area as a float
        number. Otherwise, the method prints the area information instead of returning it.
    :return: If `only_number` is True, returns the area of the bounding box in square kilometers. Otherwise,
        returns None.
    """
    df = gpd.GeoDataFrame({"id": 1, "geometry": [box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])]})
    df.crs = bbox['crs']

    # if EPSG not projected convert to an equal-area projection
    if df.crs == 'EPSG:4326':
        df = df.to_crs('EPSG:6933')

    if only_number:
        return df.iloc[0].geometry.area / 10 ** 6
    else:
        print(f'area of AOI in km2: {df.iloc[0].geometry.area / 10 ** 6}')

def bbox_of_PointsFeatureCollection(points_collection: geojson.FeatureCollection) -> openEO_bbox_format:
    """
    Calculate a bounding box from a GeoJSON FeatureCollection containing points.

    This function generates a bounding box (openEO spatial extent dictionary)
    from a GeoJSON FeatureCollection that contains point geometries. The resulting
    dictionary includes the minimum and maximum spatial extent of the points in
    the CRS 'EPSG:4326'.

    :param points_collection: A GeoJSON FeatureCollection containing point features.
    :return: The computed bounding box as a dictionary with keys 'east', 'south',
             'west', 'north', and 'crs'.
    """
    # create an openEO spatial_extent dict from the GeoJSON FeatureCollection bbox of all points
    coords = np.array(list(geojson.utils.coords(points_collection)))
    return {'east': coords[:,0].max(),
            'south': coords[:,1].min(),
            'west': coords[:,0].min(),
            'north': coords[:,1].max(),
            'crs': 'EPSG:4326'}

def get_point_info(longitude: float, latitude: float) -> Tuple[str, float, float, str]:
    """
    Gets metadata and identifiers for a geographical point based on its longitude and latitude.

    This function performs multiple spatial transformations to extract identifiers and
    coordinates in standardized formats. It converts the provided longitude and latitude
    to UTM format, shifts the coordinates to the center of the corresponding UTM 10x10m
    pixel, and computes other related spatial indices. The output includes the reference
    point in MGRSid10 format, its geodetic longitude and latitude, and the grid20id
    associated with the point for openEO processing.

    :param longitude: The longitude of the point in decimal degrees.
    :param latitude: The latitude of the point in decimal degrees.
    :return: A 4-tuple containing:
             - MGRSid10: The MGRSid10 index for the point.
             - center_lon: The longitude of the center of the UTM 10x10m pixel, rounded
               to 7 decimal places.
             - center_lat: The latitude of the center of the UTM 10x10m pixel, rounded
               to 7 decimal places.
             - grid20id: The grid20id corresponding to the openEO processing grid.
    """
    # get the coordinates in UTM format
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    # shift to the center of the corresponting UTM 10x10m pixel and get the LL coordinates for that
    rounded_easting = floor_to_nearest_5(easting)
    rounded_northing = floor_to_nearest_5(northing)
    center_lon, center_lat = UTM_2_LL(rounded_easting, rounded_northing, zone_number, zone_letter)

    # get the MGRSid10 index for the reference point
    MGRSid10 = UTM_2_MGRSid10(rounded_easting, rounded_northing, zone_number, zone_letter)

    # get the corresponding grid20id to identify the openEO processing grid for the reference point
    grid20id = UTM_2_grid20id(rounded_easting, rounded_northing, zone_number, zone_letter)

    return MGRSid10, round(center_lon, 7), round(center_lat, 7), grid20id

def geoJson_2_BBOX(file_path: str, delete_file: bool = False,
                   size_check: Optional[int] = None) -> Optional[openEO_bbox_format]:
    """
    Processes a GeoJSON file to extract the bounding box (BBOX) of all geometries combined
    and converts it into an openEO-compliant bounding box format. Optionally, the function
    can delete the input file after processing and enforce a bounding box size limit.

    :param file_path:
        Path to the GeoJSON file to be processed.
    :param delete_file:
        Whether to delete the file after processing. Defaults to False.
    :param size_check:
        Optional maximum allowable area for the BBOX. If the calculated BBOX
        area exceeds this value, an error is raised.
    :return:
        A dictionary containing the calculated BBOX in the openEO format
        with keys 'west', 'south', 'east', 'north', and 'crs'.
    :raises FileNotFoundError:
        If the specified file does not exist.
    :raises ValueError:
        If the GeoJSON file contains no features or if the calculated BBOX area
        exceeds the allowed size (when size_check is specified).
    :raises Exception:
        For any unexpected errors occurring during file processing.
    """
    exported_file_path = os.path.normpath(file_path)

    try:
        # Step 1: Open and parse the GeoJSON export file
        with open(exported_file_path, "r") as f:
            geojson_data = json.load(f)

        # Step 2: Combine individual bounding boxes
        all_geometries = []
        if geojson_data["features"]:
            for feature in geojson_data["features"]:
                # Use Shapely to convert feature geometry to a Polygon/LineString/Shape
                geom = shape(feature["geometry"])
                all_geometries.append(geom)

            # Step 3: Calculate the total bounding box using unary_union
            combined = unary_union(all_geometries)
            total_bbox = combined.bounds  # (minx, miny, maxx, maxy)
            openEO_bbox: openEO_bbox_format = {
                "west": total_bbox[0],  # minx
                "south": total_bbox[1],  # miny
                "east": total_bbox[2],  # maxx
                "north": total_bbox[3],  # maxy
                'crs': 'EPSG:4326'}
        else:
            raise ValueError("No features found in the GeoJSON file.")

        # Step 4: Delete the file after processing
        if delete_file:
            os.remove(exported_file_path)

        if size_check:
            if bbox_area(openEO_bbox, only_number=True) > size_check:
                raise ValueError(f"bbox area of {bbox_area(openEO_bbox, only_number=True)} is larger than allowed "
                                 f"size of {size_check}, please check your input data and try again.")

        return openEO_bbox

    except FileNotFoundError:
        print(f"File '{exported_file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def is_valid_geometry(geometry: dict) -> bool:
    """
    Checks if the given geometry is a valid GeoJSON geometry object.

    This function validates whether the input dictionary adheres to the standard
    structure of a GeoJSON geometry. It ensures the presence of required keys and
    validates the geometry type against a predefined set of valid GeoJSON geometry
    types.

    :param geometry: A dictionary representing the GeoJSON geometry object. It should
        contain at least a "type" and "coordinates" key. The "type" should be one of the
        supported GeoJSON types: "Point", "MultiPoint", "LineString", "MultiLineString",
        "Polygon", "MultiPolygon", or "GeometryCollection".
    :return: True if the input dictionary matches the structure of a valid GeoJSON
        geometry object, and the "type" is one of the valid GeoJSON geometry types.
        False otherwise.
    """
    if not isinstance(geometry, dict):
        return False
    if "type" not in geometry or "coordinates" not in geometry:
        return False
    valid_geom_types = {
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection"
    }
    return geometry["type"] in valid_geom_types


def is_geojson(data: str | dict) -> bool:
    """
    Determine if the given data represents a valid GeoJSON object.

    The function accepts input in the form of a string or a dictionary. It verifies whether the
    data conforms to GeoJSON specifications, including checking the "type" field and ensuring
    the presence of required attributes for valid GeoJSON objects such as "Feature",
    "FeatureCollection", or other geometric types.

    :param data: The input data to validate as GeoJSON. Can be a JSON string or a dictionary.
    :return: True if the input data is a valid GeoJSON object, otherwise False.
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            return False

    if not isinstance(data, dict) or "type" not in data:
        return False

    if data["type"] == "Feature":
        return (
                "geometry" in data and is_valid_geometry(data["geometry"]) and
                "properties" in data and isinstance(data["properties"], dict)
        )
    elif data["type"] == "FeatureCollection":
        return (
                "features" in data and isinstance(data["features"], list) and
                all(is_geojson(feature) for feature in data["features"])
        )
    elif data["type"] in {"Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon",
                          "GeometryCollection"}:
        return is_valid_geometry(data)

    return False
