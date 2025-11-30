from eo_processing.utils.geoprocessing import laea20km_id_to_extent, laea50km_id_to_extent
import pandas as pd
import geopandas as gdp
from shapely.geometry import box

gdf = gdp.read_file(r'C:\Users\BUCHHORM\Downloads\LAEA-50km_add-info.gpkg')

lNeeded = gdf.name.unique().tolist()

frames = []  # List to collect DataFrames

for tile_id in lNeeded:
    aoi = laea50km_id_to_extent(tile_id)

    df = gdp.GeoDataFrame({"tile_id": tile_id, "geometry": [box(aoi['west'], aoi['south'], aoi['east'], aoi['north'])]})
    df.crs = aoi['crs']

    df['geometry'] = df['geometry'].apply(lambda x: x.segmentize(250))

    frames.append(df)

# Concatenate all frames at once
result = pd.concat(frames, ignore_index=True)
result = gdp.GeoDataFrame(result, geometry='geometry', crs='EPSG:3035')

result.to_file(r'C:\Users\buchhorm\Downloads\LAEA_50km_tiling_grid_EU_high_res_EPSG3035.gpkg')