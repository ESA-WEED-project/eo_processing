"""
this script generates a global 20x20km tiling grid in UTM using the grid20id system for naming. the tiles are
optimized to the global land masses within the Sentinel-2 sensing area (including a 10km buffer).

"""


'''
STEPS
1. load the UTM zones gpkg
2. load the global land masses gpkg
3. loop over the 60 UTM zones
    a. load the full 20x20 km grid and change the epsg to the UTM zone
    b. load a copy of the UTM zone polygon and warp to correct epsg
    c. clip the grid to the UTM zone polygon
    d. load a copy of the land masses polygon and warp to correct epsg
    e. clip the grid to the land masses
    f. run over the remaining 20x20km tiles and add the grid20 name (using the centeroid of the polygon) as well as
       add the openEO bbox dict using the bounds of the polygon
    g. convert the geodataframe back to EPSG:4326 and save it to a new gpkg
4. combine all UTM zones into one gpkg and clean up the metadata
5. save the final global processing grid in the package ressources

'''

import geopandas as gpd
import pandas as pd
import math
from eo_processing.utils.mgrs import UTM_2_grid20id
import os

# standard paths
path_utm = os.path.normpath(r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\global_high-res_UTMzones.gpkg')
path_land = os.path.normpath(r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\ne_10m_land\Land_masses_10km_buffered_Clipped_S2.gpkg')
path_grid = os.path.normpath(r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\basic_20x20km_grid_no-crs.gpkg')

# load UTM zones
gdf_utm = gpd.read_file(path_utm)

# loop over all UTM zones
lZones = gdf_utm.name.unique().tolist()
lFiles = []

for UTMzone in lZones:
    print(f'processing zone: {UTMzone}')
    # get the UTM zone polygon and the EPSG
    if UTMzone[-1] == 'N':
        epsg = 32600 + int(UTMzone[:2])
    else:
        epsg = 32700 + int(UTMzone[:2])

    gdf_zone = gdf_utm[gdf_utm.name == UTMzone].copy()

    # get the land masses
    gdf_land = gpd.read_file(path_land)
    gdf_land = gdf_land.clip(gdf_zone)
    gdf_land = gdf_land.dissolve()

    # bring all to correct epsg
    gdf_zone = gdf_zone.to_crs(epsg=epsg)
    gdf_land = gdf_land.to_crs(epsg=epsg)
    
    # short cut if the gdf_land is empty
    if gdf_land.empty:
        continue

    # now we load the basic 20x20km grid and set the crs to the correct epsg
    gdf_grid = gpd.read_file(path_grid).set_crs(epsg=epsg, allow_override=True)

    # clipping
    gdf_grid = gdf_grid.clip(gdf_zone)
    
    # intersecting
    gdf_grid = gdf_grid[gdf_grid.intersects(gdf_land.union_all(method='coverage'))]

    # name the 20x20 grid cells with the grid20id
    if str(epsg)[2] == '6':
        zone_letter = 'Z'
    else:
        zone_letter = 'A'

    gdf_grid['grid20id'] = gdf_grid.apply(
        lambda row: UTM_2_grid20id(row['geometry'].centroid.x, row['geometry'].centroid.y, int(str(epsg)[-2:]), zone_letter), axis=1, result_type='expand')

    # add the bbox dict for openEO
    gdf_grid['bbox_dict'] = gdf_grid.apply(
        lambda row: {
            'west': math.floor(row.geometry.bounds[0] / 100) * 100.,
            'south': math.floor(row.geometry.bounds[1] / 100) * 100.,
            'east': math.ceil(row.geometry.bounds[2] / 100) * 100.,
            'north': math.ceil(row.geometry.bounds[3] / 100) * 100.,
            'epsg': epsg
        }, axis=1
    )

    # save to disk
    gdf_grid[['grid20id', 'bbox_dict', 'geometry']].to_file(
        os.path.normpath(
            r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\results\UTM_zone_{0}.gpkg'.format(
                epsg)))
    lFiles.append(os.path.normpath(
        r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\results\UTM_zone_{0}.gpkg'.format(epsg)))


# load all geopackages which were produced and convert to EPSG:4326 and merge them into one GeoDataFrame
result = gpd.GeoDataFrame(columns=['grid20id', 'bbox_dict', 'geometry'], geometry='geometry', crs='EPSG:4326')

for file in lFiles:
    gdf_tmp = gpd.read_file(file)
    gdf_tmp = gdf_tmp.to_crs(epsg=4326)
    result = pd.concat([result, gdf_tmp], ignore_index=True)

# save final result to disk
result.to_file(os.path.normpath(r'C:\Users\BUCHHORM\Downloads\global_20x20km_opneEO_processing_grid\final_global_grid.gpkg'))
