"""
finalizing the LAEA grids
- the polygons itself have to be in 4326 for better intersection with geojson or geoparquet files
- name as str
- bbox_dict as str BUT the crs has to be an int
- s2_tileid_list as str seperated by comma

we first run 20k, then 50K and in the end 100k (for 100K the tile list is assembled from the 20K)

"""

try:
    import importlib.resources as importlib_resources
except:
    import importlib_resources
import eo_processing.resources
from os.path import normpath
import geopandas as gpd
from eo_processing.utils.helper import convert_to_list
from eo_processing.utils.geoprocessing import laea100km_id_to_extent

### start with 20K
path_20k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_EPSG4326.gpkg'
path_20k_meta = r'C:\Users\buchhorm\Downloads\new_grids\S2_tile_info_20K_grid.gpkg'

gdf20 = gpd.read_file(path_20k)
gdf20_meta = gpd.read_file(path_20k_meta)

out_20k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA-20km_add-info.gpkg'

gdf20 = gdf20.merge(gdf20_meta[['name', 's2_multi_list']], how='left', left_on='name', right_on='name')
gdf20.rename(columns={'s2_multi_list': 's2_tileid_list'}, inplace=True)
gdf20['bbox_dict'] = gdf20.bbox_dict.str.replace("'EPSG:3035'", "3035")

gdf20[['name', 's2_tileid_list', 'bbox_dict', 'geometry']].to_file(out_20k, driver='GPKG')


### now the 50K
path_50k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_EPSG4326.gpkg'
path_50k_meta = r'C:\Users\buchhorm\Downloads\new_grids\S2_tile_info_50K_grid.gpkg'

gdf50 = gpd.read_file(path_50k)
gdf50_meta = gpd.read_file(path_50k_meta)

out_50k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA-50km_add-info.gpkg'

gdf50 = gdf50.merge(gdf50_meta[['name', 's2_multi_list']], how='left', left_on='name', right_on='name')
gdf50.rename(columns={'s2_multi_list': 's2_tileid_list'}, inplace=True)
gdf50['bbox_dict'] = gdf50.bbox_dict.str.replace("'EPSG:3035'", "3035")

gdf50[['name', 's2_tileid_list', 'bbox_dict', 'geometry']].to_file(out_50k, driver='GPKG')

### finally the 100K
path_100k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_EPSG4326.gpkg'
out_100k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA-100km_add-info.gpkg'

#create the metadata from the 20k
gdf100 = gpd.read_file(path_100k)
gdf100_meta = gdf20.copy()

gdf100_meta['name100'] = gdf100_meta.name.apply(lambda x: x[:3] + x[4:7])
# convert s2_tileid_list to list
gdf100_meta['s2_tileid_list'] = gdf100_meta.apply(lambda row : row['s2_tileid_list'].split(','), axis =1)

# update the 's2_tileid_list' column
s2_grouped = gdf100_meta.groupby('name100')['s2_tileid_list'].apply(
        lambda x: list(set([item for sublist in x for item in sublist]))
    )
gdf100_meta['s2_tileid_list100'] = gdf100_meta['name100'].map(s2_grouped)

# now we dissolve by name100
gdf100_meta = gdf100_meta.dissolve(by='name100')

gdf100_meta.reset_index(inplace=True)

gdf100_meta.drop(columns=['name', 's2_tileid_list'], inplace=True)

gdf100_meta.rename(columns={'name100': 'name', 's2_tileid_list100': 's2_tileid_list'}, inplace=True)

#convert list to string
gdf100_meta['s2_tileid_list'] = gdf100_meta['s2_tileid_list'].apply(lambda x: str(x))
gdf100_meta['s2_tileid_list'] = gdf100_meta['s2_tileid_list'].apply(lambda x: x.replace("'", ""))
gdf100_meta['s2_tileid_list'] = gdf100_meta['s2_tileid_list'].apply(lambda x: x.replace(" ", ""))
gdf100_meta['s2_tileid_list'] = gdf100_meta['s2_tileid_list'].apply(lambda x: x.replace("[", ""))
gdf100_meta['s2_tileid_list'] = gdf100_meta['s2_tileid_list'].apply(lambda x: x.replace("]", ""))

#add this info to the 100k grid
gdf100 = gdf100.merge(gdf100_meta[['name', 's2_tileid_list']], how='left', left_on='name', right_on='name')
gdf100['bbox_dict'] = gdf100.bbox_dict.str.replace("'EPSG:3035'", "3035")

gdf100[['name', 's2_tileid_list', 'bbox_dict', 'geometry']].to_file(out_100k, driver='GPKG')
