""" extract from the official Sentinel2 tiling grid KML file the UTM bounding box"""

import bs4 as bs
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon
import shapely.wkt

def get_epsg(tile_id):
    """ get epsg code from the Sentinel-2 tile name"""
    # calculate the UTM zone_number and zone_letter from lon, lat
    zone_number = int(tile_id[:2])
    zone_letter = tile_id[2]

    # figure out from zone_letter if N or S
    northern = (zone_letter >= 'N')

    # get EPSG number from zone number and northern
    if northern:
        target_EPSG = 32600 + zone_number
    else:
        target_EPSG = 32700 + zone_number

    return target_EPSG


# filter to tiles in UTM zones needed for 20x20km grid for EU
df2 = pd.read_csv(r'C:\Users\buchhorm\Downloads\UTMzones.csv')
df2['utm'] = df2['ZONE'].astype(str) + df2['ROW_']
lNeeded = df2['utm'].tolist()


## Start read out everything we need with beautiful soup from Sentinel-2 KML
xml_file = r'C:\Users\buchhorm\Downloads\Sentinel2_tiles.kml'
soup = bs.BeautifulSoup(open(xml_file), 'xml')

#find all VRTRasterBands in the soup
tiles = soup.findAll('description')

data = []

for element in tiles:
    text = bs.BeautifulSoup(element.text, 'html.parser')
    try:
        rows = text.table.find_all('tr')
    except:
        continue
    for row in rows:
        try:
            cols = row.find_all('td')
        except:
            continue
        cols = [ele.text.strip() for ele in cols]
        if cols[0] == 'TILE_ID': data1 = cols[1]
        if cols[0] == 'UTM_WKT': data2 = cols[1]
    if data1[:3] in lNeeded:
        data.append((data1, data2, get_epsg(data1)))



df = pd.DataFrame(data, columns=['tile_id', 'geometry', 'epsg'])
data = None
soup = None
tiles = None

# convert the geometry string into a shapely geometry
df['geometry'] = df['geometry'].apply(lambda x: shapely.wkt.loads(x))


# now we convert the UTM into EPSG:3035 and combine all


result = gpd.GeoDataFrame(columns=['tile_id', 'epsg', 'geometry'], geometry='geometry', crs='EPSG:3035')

lEPSG = df.epsg.unique().tolist()

for tile in lEPSG:
    df3 = df[df.epsg == tile].copy()
    gdf = gpd.GeoDataFrame(df3, geometry=df3.geometry, crs=f'EPSG:{tile}')

    # add extra points in 250m intervall
    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.segmentize(250))




    gdf2 = gdf.to_crs(epsg=3035)

    result = result.append(gdf2, ignore_index=True)

# write out
result.to_file(r'C:\Users\buchhorm\Downloads\Sentinel2_tiling_grid_EU_high_res_EPSG3035.gpkg')
