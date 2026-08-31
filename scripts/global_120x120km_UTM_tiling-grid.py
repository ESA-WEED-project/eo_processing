"""
this script generates a global 120x120km tiling grid in UTM.

"""


'''
STEPS
1. load the UTM zones gpkg
2. load the global land masses gpkg
3. loop over the 120 UTM zones
    a. load the full 120x120 km grid depending on the hemisphere and change the epsg to the UTM zone
    b. load a copy of the UTM zone polygon and warp to correct epsg
    c. select the grid cells intersecting the UTM zone polygon and delete non-needed
    d. load a copy of the land masses polygon and warp to correct epsg
    e. select the grid cells intersecting the land masses and dlete non-needed ones
    f. run over the remaining 120x120km tiles and add the naming as well as
       add the openEO bbox dict using the bounds of the polygon (plus add the bounds in WGS84)
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
path_grid_n = os.path.normpath(r'C:\Users\buchhorm\Downloads\120x120km_grid\basic_120x120km_grid_nothern_no-crs_v2.gpkg')
path_grid_s = os.path.normpath(r'C:\Users\buchhorm\Downloads\120x120km_grid\basic_120x120km_grid_southern_no-crs_v2.gpkg')
path_land = os.path.normpath(r'C:\Users\buchhorm\Downloads\120x120km_grid\land_sea_mask_20kmbuffered_EPSG4326_v2.gpkg')
path_utm = os.path.normpath(r'C:\Users\buchhorm\Downloads\120x120km_grid\UTM_zones_high-res_EPSG4326.gpkg')

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

    path_out = os.path.normpath(
            r'C:\Users\buchhorm\Downloads\120x120km_grid\results\UTM_zone_{0}.gpkg'.format(
                epsg))
    os.makedirs(os.path.dirname(path_out), exist_ok=True)

    if not os.path.exists(path_out):
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

        # now we load the basic 120x120km grid and set the crs to the correct epsg
        if UTMzone[-1] == 'N':
            gdf_grid = gpd.read_file(path_grid_n).set_crs(epsg=epsg, allow_override=True)
        else:
            gdf_grid = gpd.read_file(path_grid_s).set_crs(epsg=epsg, allow_override=True)

        # select grid cells which are intersecting with utm zone
        gdf_grid = gdf_grid[gdf_grid.intersects(gdf_zone.union_all(method='coverage'))]

        # intersecting
        gdf_grid = gdf_grid[gdf_grid.intersects(gdf_land.union_all(method='coverage'))]

        # add the bbox dict for openEO
        gdf_grid['bbox_dict'] = gdf_grid.apply(
            lambda row: {
                'west': math.floor(row.geometry.bounds[0] / 100) * 100.,
                'south': math.floor(row.geometry.bounds[1] / 100) * 100.,
                'east': math.ceil(row.geometry.bounds[2] / 100) * 100.,
                'north': math.ceil(row.geometry.bounds[3] / 100) * 100.,
                'crs': epsg
            }, axis=1
        )

        # save to disk


        gdf_grid[['left', 'top', 'right', 'bottom', 'row_index','col_index',  'bbox_dict', 'geometry']].to_file(
            path_out)
    else:
        # load
        gdf_grid = gpd.read_file(path_out)
        gdf_zone = gdf_zone.to_crs(epsg=epsg)
        gdf_grid = gdf_grid.clip(gdf_zone)
        gdf_grid.to_file(path_out)

    lFiles.append(path_out)


# load all geopackages which were produced and convert to EPSG:4326 and merge them into one GeoDataFrame
result = gpd.GeoDataFrame(columns=['left', 'top', 'right', 'bottom', 'row_index','col_index', 'bbox_dict', 'geometry'], geometry='geometry', crs='EPSG:4326')

for file in lFiles:
    gdf_tmp = gpd.read_file(file)
    gdf_tmp = gdf_tmp.to_crs(epsg=4326)
    result = pd.concat([result, gdf_tmp], ignore_index=True)

# save final result to disk
result.to_file(os.path.normpath(r'C:\Users\buchhorm\Downloads\120x120km_grid\ECDC_global_120x120km_grid.gpkg'))
