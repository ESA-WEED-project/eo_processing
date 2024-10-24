import pyproj
from shapely.geometry import Polygon
from shapely.geometry import box
import geopandas as gpd
import numpy as np
import geojson

def laea20km_id_to_extent(laea_id: str):
    """Method to get extent in EPSG:3035
    from a 20km LAEA grid ID

    Args:
        laea_id: an id, like 'E380N278'

    Returns:

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

def reproj_bbox_to_ll(bbox: dict, buffer: bool = False, densify: bool = False, return_geojson: bool = False):
    """ convert bbox to lat lon Polygon including buffering and desifying if wished

    :param bbox: Dictionary containing the bounding box coordinates and coordinate reference system (CRS).
    :param buffer: Optional boolean indicating whether to apply a small buffer to the polygon. Defaults to False.
    :param densify: Optional boolean indicating whether to densify the polygon. Defaults to False.
    :return: A polygon transformed to the EPSG:4326 coordinate reference system.
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
        polygon_4326 = geojson.Feature(geometry=polygon_4326, properties={})

    return polygon_4326

def bbox_area(bbox):
    """ Calculate the area of the AOI to process in km2
    Note: bbox must be given in a projected coordinate system to get a correct estimate

    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    """
    df = gpd.GeoDataFrame({"id": 1, "geometry": [box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])]})
    df.crs = bbox['crs']
    print(f'area of AOI in km2: {df.iloc[0].geometry.area / 10 ** 6}')

def bbox_of_PointsFeatureCollection(points_collection):
    # create an openEO spatial_extent dict from the GeoJSON FeatureCollection bbox of all points
    coords = np.array(list(geojson.utils.coords(points_collection)))
    return {'east': coords[:,0].max(),
            'south': coords[:,1].min(),
            'west': coords[:,0].min(),
            'north': coords[:,1].max(),
            'crs': 'EPSG:4326'}
