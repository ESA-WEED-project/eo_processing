"""
This script is used to generate the base LAEA tiling grids in 20, 100 and 50km resolution covering the full panEU

1. step create tiling grid in 100km over all areas via function name-to-bbox
2. filter by buffered panEU layer
3. write out (in EPSG:3035 and EPSG:4326)
4. write out the high res layers in EPSG:3035 and EPSG:4326
5. create also the 50x50 and 20x20 km variants

"""

from eo_processing.utils.geoprocessing import laea100km_id_to_extent, laea50km_id_to_extent, laea20km_id_to_extent
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
import os
from tqdm import tqdm

def create_100k_grid():
    ### start with the 100km tiling grid for EU
    # filter N and E
    N_min = 8
    N_max = 75
    E_min = 9
    E_max = 80

    print('start creating combinations')
    # create all combinations
    entries = []
    for N in range(N_min, N_max+1):
        for E in range(E_min, E_max+1):
            name = f'E{E:02d}N{N:02d}'
            entries.append(name)
    print(f'done creating combinations. number of tiles: {len(entries)} ')

    print('start creating geodataframe')
    frames = []  # List to collect DataFrames

    for tile_id in tqdm(entries, desc='creating tiles'):
        aoi = laea100km_id_to_extent(tile_id)

        df = gpd.GeoDataFrame(
            {"name": tile_id, "bbox_dict": str(aoi), "geometry": [box(aoi['west'], aoi['south'], aoi['east'], aoi['north'])]})
        df.crs = aoi['crs']
        frames.append(df)

    # Concatenate all frames at once
    result = pd.concat(frames, ignore_index=True)
    gdf_100 = gpd.GeoDataFrame(result, geometry='geometry', crs='EPSG:3035')
    print('done creating geodataframe')

    print('filter tiles to panEU')
    #load the panEU shapefile
    gdf_panEU = gpd.read_file(r'C:\Users\buchhorm\Downloads\new_grids\EU_biogeographic_buffered_final.gpkg')
    #convert to EPSG:3035
    gdf_panEU = gdf_panEU.to_crs(epsg=3035)

    #intersect
    gdf_final = gdf_100[gdf_100.name.isin(gdf_100.clip(gdf_panEU).name.unique().tolist())]

    print('write out')
    #write out
    gdf_final.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_EPSG3035.gpkg')

    print('create variants')
    print('create high res version')
    gdf_high = gdf_final.copy()
    gdf_high['geometry'] = gdf_high['geometry'].apply(lambda x: x.segmentize(250))
    gdf_high.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_high_res_EPSG3035.gpkg')

    print('convert to 4326 versions')
    # create also the EPSG:4326 versions
    gdf_high4326 = gdf_high.to_crs(epsg=4326)
    gdf_high4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_high_res_EPSG4326.gpkg')
    gdf_final4326 = gdf_final.to_crs(epsg=4326)
    gdf_final4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_EPSG4326.gpkg')

path_100k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_100km_tiling_grid_EU_EPSG3035.gpkg'
if os.path.isfile(path_100k):
    gdf_100k = gpd.read_file(path_100k)
else:
    create_100k_grid()
    gdf_100k = gpd.read_file(path_100k)

##### now we create the 50x50km versions
def create_50k_grid(gdf100):
    # we get a list of unique name100
    ltiles = gdf100['name'].unique().tolist()
    print('create 50km tiles')
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
    gdf_50['geometry'] = gdf_50['bbox_dict'].apply(
        lambda row: box(row['west'], row['south'], row['east'], row['north']))

    # convert to geopandas
    gdf_50 = gpd.GeoDataFrame(gdf_50, geometry='geometry', crs='EPSG:3035')

    #intersetc to further filter
    print('filter tiles to panEU')
    #load the panEU shapefile
    gdf_panEU = gpd.read_file(r'C:\Users\buchhorm\Downloads\new_grids\EU_biogeographic_buffered_final.gpkg')
    #convert to EPSG:3035
    gdf_panEU = gdf_panEU.to_crs(epsg=3035)

    #intersect
    gdf_final = gdf_50[gdf_50.name.isin(gdf_50.clip(gdf_panEU).name.unique().tolist())]


    print('write out')
    #write out
    gdf_final.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_EPSG3035.gpkg')

    print('create variants')
    print('create high res version')
    gdf_high = gdf_final.copy()
    gdf_high['geometry'] = gdf_high['geometry'].apply(lambda x: x.segmentize(250))
    gdf_high.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_high_res_EPSG3035.gpkg')

    print('convert to 4326 versions')
    # create also the EPSG:4326 versions
    gdf_high4326 = gdf_high.to_crs(epsg=4326)
    gdf_high4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_high_res_EPSG4326.gpkg')
    gdf_final4326 = gdf_final.to_crs(epsg=4326)
    gdf_final4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_EPSG4326.gpkg')

path_50k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_50km_tiling_grid_EU_EPSG3035.gpkg'
if os.path.isfile(path_50k):
    gdf_50k = gpd.read_file(path_50k)
else:
    create_50k_grid(gdf_100k)
    gdf_50k = gpd.read_file(path_50k)

# create the 20x20km tiling grid
def create_20k_grid(gdf100, gdf50):
    # we get a list of unique name100
    ltiles = gdf100['name'].unique().tolist()
    print('create 20km tiles')
    # now we loop over all tiles and create all possible 50x50km identifier to create the 50x50km grid
    l20kmtiles = []
    for tile in ltiles:
        parts = tile.lstrip('E').split('N')
        # create the 25 sub-tiles
        for x in range(0, 10, 2):
            for y in range(0, 10, 2):
                l20kmtiles.append(f'E{parts[0]}{x}N{parts[1]}{y}')


    # create pandas dataframe with the 50x50km tiles
    gdf_20 = pd.DataFrame(l20kmtiles, columns=['name'])

    # now we create the bbox_dict column
    gdf_20['bbox_dict'] = gdf_20.apply(lambda row: laea20km_id_to_extent(row['name']), axis=1)

    # create the bbox coordinates from the bbox_dict column
    gdf_20['geometry'] = gdf_20['bbox_dict'].apply(
        lambda row: box(row['west'], row['south'], row['east'], row['north']))

    # convert to geopandas
    gdf_20 = gpd.GeoDataFrame(gdf_20, geometry='geometry', crs='EPSG:3035')

    #intersetc to further filter
    print('filter tiles to panEU')
    #load the panEU shapefile
    gdf_panEU = gpd.read_file(r'C:\Users\buchhorm\Downloads\new_grids\EU_biogeographic_buffered_final.gpkg')
    #convert to EPSG:3035
    gdf_panEU = gdf_panEU.to_crs(epsg=3035)

    #intersect
    gdf_final = gdf_20[gdf_20.name.isin(gdf_20.clip(gdf_panEU).name.unique().tolist())]

    # also do intersect with 50km grid since that is also optimized
    gdf_final = gdf_final[gdf_final.name.isin(gdf_final.clip(gdf_50k).name.unique().tolist())]


    print('write out')
    #write out
    gdf_final.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_EPSG3035.gpkg')

    print('create variants')
    print('create high res version')
    gdf_high = gdf_final.copy()
    gdf_high['geometry'] = gdf_high['geometry'].apply(lambda x: x.segmentize(250))
    gdf_high.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_high_res_EPSG3035.gpkg')

    print('convert to 4326 versions')
    # create also the EPSG:4326 versions
    gdf_high4326 = gdf_high.to_crs(epsg=4326)
    gdf_high4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_high_res_EPSG4326.gpkg')
    gdf_final4326 = gdf_final.to_crs(epsg=4326)
    gdf_final4326.to_file(r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_EPSG4326.gpkg')

path_20k = r'C:\Users\buchhorm\Downloads\new_grids\LAEA_20km_tiling_grid_EU_EPSG3035.gpkg'
if os.path.isfile(path_20k):
    gdf_20k = gpd.read_file(path_20k)
else:
    create_20k_grid(gdf_100k, gdf_50k)
    gdf_20k = gpd.read_file(path_20k)
