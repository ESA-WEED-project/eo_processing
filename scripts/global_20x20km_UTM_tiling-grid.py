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

