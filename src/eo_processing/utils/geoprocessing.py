import pyproj
from shapely.geometry import Polygon
from shapely.geometry import box
import geopandas as gpd
import numpy as np
import geojson
from typing import Union, TypedDict
from eo_processing.utils.mgrs import LL_2_UTM, floor_to_nearest_5, UTM_2_LL, UTM_2_MGRSid10, UTM_2_grid20id

openEO_bbox_format = TypedDict('openEO_bbox_format', {'east': float,
                                                      'south': float,
                                                      'west': float,
                                                      'north': float,
                                                      'crs': str})

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

def bbox_area(bbox: openEO_bbox_format) -> None:
    """
    Calculate and print the area of a bounding box (AOI) in square kilometers.

    This function takes a bounding box represented in a specific format and calculates
    its area. The bounding box is first converted into a GeoDataFrame object using its
    geographical boundaries and the specified coordinate reference system (CRS). The
    function divides the calculated area by 10^6 to convert it from square meters to
    square kilometers and prints the result.

    :param bbox: A dictionary containing the bounding box details with keys `west`,
                 `south`, `east`, `north`, and `crs`. The keys `west`, `south`,
                 `east`, and `north` define the extent of the bounding box, while the
                 `crs` provides the coordinate reference system of the bounding box.
    :return: None
    """
    df = gpd.GeoDataFrame({"id": 1, "geometry": [box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])]})
    df.crs = bbox['crs']
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

def get_point_info(longitude: float, latitude: float) -> tuple[str, float, float, str]:
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
