try:
    import importlib.resources as importlib_resources
except:
    import importlib_resources
import eo_processing.resources
from os.path import normpath
import geopandas as gpd
from eo_processing.utils.geoprocessing import laea50km_id_to_extent
import pandas as pd
from shapely.geometry import box

# we need the 20x20 km grid to know which areas need a 50x50 EU grid cell
file_path = importlib_resources.files(eo_processing.resources).joinpath('LAEA-20km_add-info.gpkg')
file_path = normpath(file_path)

gdf = gpd.read_file(file_path)

# get the 100km identifier
gdf['name100'] = gdf.name.apply(lambda x: x[:3] + x[4:7])
# convert s2_tileid_list to list
gdf['s2_tileid_list'] = gdf.apply(lambda row : row['s2_tileid_list'].split(','), axis =1)

# we get a list of unique name100
ltiles = gdf['name100'].unique().tolist()

# now we loop over all tiles and create all possible 50x50km identifier to create the 50x50km grid
l50kmtiles = []
for tile in ltiles:
    parts = tile.lstrip('E').split('N')
    # create the 4 sub-tiles
    l50kmtiles.append(f'E{parts[0]}0N{parts[1]}0')
    l50kmtiles.append(f'E{parts[0]}5N{parts[1]}0')
    l50kmtiles.append(f'E{parts[0]}0N{parts[1]}5')
    l50kmtiles.append(f'E{parts[0]}5N{parts[1]}5')

# create pandas dataframe with the 50x50km tiles
gdf_50 = pd.DataFrame(l50kmtiles, columns=['name'])

# now we create the bbox_dict column
gdf_50['bbox_dict'] = gdf_50.apply(lambda row: laea50km_id_to_extent(row['name']), axis=1)

# create the bbox coordinates from the bbox_dict column
gdf_50['geometry'] = gdf_50['bbox_dict'].apply(lambda row: box(row['west'], row['south'], row['east'], row['north']))

# convert to geopandas
gdf_50 = gpd.GeoDataFrame(gdf_50, geometry='geometry', crs='EPSG:3035')

# resample to 4326
gdf_50 = gdf_50.to_crs(epsg=4326)

### now we want to add the s2_tileid_list
# for that: 1. run the high_res_openEO_grid.py script to get a high res 50x50km grid vector file
# 2. load or create the high-res Sentinel-2 tiling grid (high_res_Sentinel2_tiling_grid.py)
# 3. run the assignment_S2_to_laea_grid.py script to now which S2 tiles intersect which each 50x50km grid cell
# 4. do some cleanup in QGIS (add the single tileid matches into the s2_tileid_list column)
# load the s2_tile info
info_path = r'C:\Users\buchhorm\Downloads\LAEA_50km_tiling_grid_S2info_new.gpkg'
gdf_info = gpd.read_file(info_path)

# add info from s2_multi_list
gdf_final = gdf_50.merge(gdf_info[['laea_tileid','s2_multi_list']], how='left', left_on='name', right_on='laea_tileid')

#rename column to s2_tileid_list
gdf_final.rename(columns={'s2_multi_list': 's2_tileid_list'}, inplace=True)
# drop
gdf_final.drop(columns=['laea_tileid'], inplace=True)

# write out
path_out = r'C:\Users\buchhorm\Downloads\LAEA-50km_add-info.gpkg'
gdf_final.to_file(path_out, driver='GPKG')

# run some final checks in QGIS and remove 50x50km tiles which are not need (which do not "contain" a 20x20km grid cell)
