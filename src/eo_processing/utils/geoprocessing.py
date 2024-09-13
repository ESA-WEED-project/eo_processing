import pyproj
from shapely.geometry import Polygon
from shapely.geometry import box
import geopandas as gpd

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

def reproj_bbox_to_ll(bbox: dict):
    '''convert bbox to lat lon Polygon
    param bbox: a bounding box dictionary with south,west,north,east and crs
    return polygon of bbox in latlon
    '''

    # ToDo: rewrite that with Marcel's method - add extra points along the border and then reproject in 4326 !!

    # Create the PyProj transformer objects for bbox['crs'] and EPSG 3246
    transformer_to_4326 = pyproj.Transformer.from_crs(bbox['crs'], 'EPSG:4326', always_xy=True)

    # Transform the bounding box coordinates from input CRS to  EPSG:3246
    minx, maxy = transformer_to_4326.transform(bbox['west'], bbox['north'])
    maxx, miny = transformer_to_4326.transform(bbox['east'], bbox['south'])

    # Create a polygon using the transformed coordinates in EPSG 3246
    polygon_4326 = Polygon([(minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny), (minx, maxy)])

    return polygon_4326

def bbox_area(bbox):
    """ Calculate the area of the AOI to process in km2
    Note: bbox must be given in a projected coordinate system to get a correct estimate

    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    """
    df = gpd.GeoDataFrame({"id": 1, "geometry": [box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])]})
    df.crs = bbox['crs']
    print(f'area of AOI in km2: {df.iloc[0].geometry.area / 10 ** 6}')
