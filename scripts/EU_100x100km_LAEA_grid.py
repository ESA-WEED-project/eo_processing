try:
    import importlib.resources as importlib_resources
except:
    import importlib_resources
import eo_processing.resources
from os.path import normpath
import geopandas as gpd
from eo_processing.utils.helper import convert_to_list
from eo_processing.utils.geoprocessing import laea100km_id_to_extent

file_path = importlib_resources.files(eo_processing.resources).joinpath('LAEA-20km_add-info.gpkg')
file_path = normpath(file_path)

gdf = gpd.read_file(file_path)

# get the 100km identifier
gdf['name100'] = gdf.name.apply(lambda x: x[:3] + x[4:7])

# convert s2_tileid_list to list
gdf['s2_tileid_list'] = gdf.apply(lambda row : row['s2_tileid_list'].split(','), axis =1)

# update the 's2_tileid_list' column
s2_grouped = gdf.groupby('name100')['s2_tileid_list'].apply(
        lambda x: list(set([item for sublist in x for item in sublist]))
    )
gdf['s2_tileid_list100'] = gdf['name100'].map(s2_grouped)

# now we dissolve by name100
gdf_dissolved = gdf.dissolve(by='name100')

gdf_dissolved.reset_index(inplace=True)

gdf_dissolved.drop(columns=['name', 'match', 's2_tileid_list'], inplace=True)

gdf_dissolved.rename(columns={'name100': 'name', 's2_tileid_list100': 's2_tileid_list'}, inplace=True)

#updating the bbox_dict column
gdf_dissolved['bbox_dict'] = gdf_dissolved.apply(lambda row: laea100km_id_to_extent(row['name']), axis=1)

#convert list to string
gdf_dissolved['s2_tileid_list'] = gdf_dissolved['s2_tileid_list'].apply(lambda x: str(x))
gdf_dissolved['s2_tileid_list'] = gdf_dissolved['s2_tileid_list'].apply(lambda x: x.replace("'", ""))
gdf_dissolved['s2_tileid_list'] = gdf_dissolved['s2_tileid_list'].apply(lambda x: x.replace(" ", ""))
gdf_dissolved['s2_tileid_list'] = gdf_dissolved['s2_tileid_list'].apply(lambda x: x.replace("[", ""))
gdf_dissolved['s2_tileid_list'] = gdf_dissolved['s2_tileid_list'].apply(lambda x: x.replace("]", ""))

#write out
path_out = r'C:\Users\buchhorm\Downloads\LAEA-100km_add-info.gpkg'
gdf_dissolved.to_file(path_out, driver='GPKG')
