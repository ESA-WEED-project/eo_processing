from extentmapping.utils import laea20km_id_to_extent
import pandas as pd
import geopandas as gdp
from shapely.geometry import box

gdf = gdp.read_file(r'C:\Users\BUCHHORM\Downloads\LAEA_20km_grid_EU_EPSG3035.gpkg')

lNeeded = gdf.name.unique().tolist()

result = gdp.GeoDataFrame(columns=['tile_id', 'geometry'], geometry='geometry', crs='EPSG:3035')


for tile_id in lNeeded:
    aoi = laea20km_id_to_extent(tile_id)

    df = gdp.GeoDataFrame({"tile_id": tile_id, "geometry": [box(aoi['west'], aoi['south'], aoi['east'], aoi['north'])]})
    df.crs = aoi['crs']

    df['geometry'] = df['geometry'].apply(lambda x: x.segmentize(250))

    result = result.append(df)


result.to_file(r'C:\Users\buchhorm\Downloads\LAEA_20km_tiling_grid_EU_high_res_EPSG3035.gpkg')

print()