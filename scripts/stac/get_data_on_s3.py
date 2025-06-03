'''
It is required to use the jsons from Globes, in this case the data will be automatically/correctly
added to the correct metadata field.
In a first step we need to get the data on S3 as a COG format in order to get this we will first check if the files
have a valid cog format if not we will convert the files to valid COG formats and upload it to S3.
This will also create a correct json file that needs to be used in the next step.'''

import glob
import os
import json
from collections import defaultdict
from eo_processing.utils.stac import (
    get_datafrom_toml
)
from eo_processing.utils.storage import storage
import logging

logger = logging.getLogger(__name__)

# get configuration details from config.toml
filename = "config_1.toml"
config = get_datafrom_toml(filename)

# Set environment variables for S3 bucket access
s3_config = config["s3bucket"]
#create storage object for easy handling
store = storage({
                "s3_access_key": s3_config["AWS_ACCESS_KEY_ID"],
                "s3_secret_key": s3_config["AWS_SECRET_ACCESS_KEY"],
                "s3_endpoint": s3_config["AWS_ENDPOINT_URL_S3"],
                "bucket_name": "ecdc-waw4-1-ekqouvq3otv8hmw0njzuvo0g4dy0ys8r985n7dggjis3erkpn5o",
                "export_workspace": ""
            })

#get input data dir
inputdir = config["inputdata"]["INPUT_DATADIR"]

#get collection info
collection = config["stacbuild"]["COLLECTION"]
#all info should be according the as is metadata
bands_json = glob.glob(os.path.join(inputdir,"*.json"))
#bands_json.sort()

#now we just take the first json to extract needed data to upload to s3. (just the SpatialScope at the moment)
with open(bands_json[0]) as f:
    json_data = json.load(f)

general_info = json_data["GeneralInformation"]


prefix = f"preprocessed/{general_info['SpatialScope']}/{collection.replace('-','/')}/"


#the all_assets dict is used to store all the information needed for the assets
all_assets = {}
providers = []
#Get all data and move to S3
for product in bands_json:
    with open(product) as f:
        json_data = json.load(f)
    files_id = json_data.get("OrganizationalAspects").get("FileInformation")[0].get("Filename")

    name = json_data.get("GeneralInformation").get("AbbreviationOfTheDataset").replace('-','')
    #name = f"{collection}-{name}"

    #add metadata/band/asset info to the list of  assets
    #add categorical info
    names =json_data.get("DataCoverageAndContents").get("SpecificationsOfVariables").get("ClassifiedDataSpecifications").get("NamesOfClasses")
    codes =json_data.get("DataCoverageAndContents").get("SpecificationsOfVariables").get("ClassifiedDataSpecifications").get("CodesOfClasses")
    descripts = json_data.get("DataCoverageAndContents").get("SpecificationsOfVariables").get("ClassifiedDataSpecifications").get("DescriptionsOfClasses")

    if not descripts:
        descripts = names
    if len(descripts) != len(names):
        logger.error("For some data classes it seems that the description is missing")

    # to be removed when metadata is ok
    names = [name.replace(' ', '') for name in names]

    all_assets[name] = {"title": json_data.get("GeneralInformation").get("TitleOfTheDataSource"),
                        "description": json_data.get("GeneralInformation").get("DescriptionOfTheDataSource"),
                        "eo_bands": [
                            {
                                "name": name,
                                "description": json_data.get("GeneralInformation").get("DescriptionOfTheDataSource")
                            }]
                        }


    if len(names) != 0:
        all_assets[name]["classification:classes"] = [
               {"value":int(code), "name":names[idx], "description":descripts[idx]} for idx,code in enumerate(codes)]



    #This check is done on globes since sometimes the Role is not filled which is a mandatory field.
    for provider in json_data.get("OrganizationalAspects").get("DatasetProvider"):
        if provider["Role"] == '':
            logger.error(f"It seems that there is a role missing for dataset providers for metadata {os.path.basename(product)}")
        providers.append(provider)

    #now we move the data to the correct S3 location.
    data2move = glob.glob(os.path.join(inputdir,"**",f"{files_id}*.tif"),recursive=True)
    for tif_name in data2move:
        continue
        #warnings, errors, details = validate(tif_name)
        with rasterio.open(tif_name) as src:
            driver = src.profile['driver']
            profile = src.profile

            #checks on file
            if src.profile['count'] != 1:
                logger.error(f"It seems that {tif_name} has multiple bands, only single band files are supported")

        validator = rio_cogeo.cog_validate(tif_name,strict=True)
        if validator[0] != True:
            logger.warning(f"It seems that {tif_name} is not a cog. We will convert it to COG for you.")
            #now we need a different filename
            cog_name = f"{os.path.splitext(tif_name)[0]}_COG.tif"
            rio_cogeo.cog_translate(tif_name,cog_name, rio_cogeo.cog_profiles.get('lzw'))
            tifname = cog_name


        #this should always follow the same structure
        info_list = os.path.relpath(tif_name,inputdir).split('/')
        year = info_list[1]
        area = info_list[0]

        #hardcoded subversion 00
        s3key = os.path.join(prefix, f"{collection.replace('-','_')}{'00'}_{year}_{area}_{name}.tiff")
        #slightly extend functionality on storage object to be able to handle fixed output names
        store.upload_file_to_s3key(tif_name, s3key, exist_check=True)

    #I know this is double but for now (first do without) should be removed in the end

    for area in  ['ZAF','CZE','NOR','VNM','GRC','COL']:
        for year in ['2018','2021','2024']:
            continue
            #check which ones are missing
            s3key = os.path.join(prefix, f"{collection.replace('-', '_')}{'00'}_{year}_{area}_{name}.tiff")
            check = store.s3_object_exists(s3key)
            if not check:
                basic_s3key = os.path.join(prefix, f"{collection.replace('-', '_')}{'00'}_2018_{area}_{name}.tiff")
                local_file_path = store.download_file_key(basic_s3key,os.path.join(inputdir,'TEMP'),exist_check=True)
                tifname = local_file_path.replace('.tif','_nodata.tif')
                with rasterio.open(local_file_path,'r') as src:
                    profile = src.profile
                with rasterio.open(tifname, 'w', **profile) as src:
                    pass
                    
                validator = rio_cogeo.cog_validate(tifname, strict=True)
                if validator[0] != True:
                    cog_name = f"{os.path.splitext(tifname)[0]}_COG.tif"
                    rio_cogeo.cog_translate(tifname, cog_name, rio_cogeo.cog_profiles.get('lzw'))
                    tifname = cog_name
                store.upload_file_to_s3key(tifname, s3key, exist_check=True)




#create correct collection config
#create basic config
#first clean-up providers
providers_clean = []
for idx, prov_dict in enumerate(providers):
    prov_dict.pop("Surname")
    prov_dict.pop("Name")
    if prov_dict not in providers_clean:
        providers_clean.append(prov_dict)

grouped = defaultdict(list)
for d in providers_clean:
    key = (d["Organisation"],d["Link"])
    grouped[key].append(d["Role"])

providers_clean_agg =[]
for (Organisation,Link) , Roles in grouped.items():
    providers_clean_agg.append({"name":Organisation, "roles":Roles,"url":Link})

#2nd create config

basic_config = {
    "collection_id": collection,
    "title": "Globes",
    "description": "Globes collection third version",
    "instruments": [],
    "providers": providers_clean_agg ,
    "layout_strategy_item_template": "${collection}",
    "input_path_parser": {
        "classname": "DefaultInputPathParser",
        "parameters": {
            "regex_pattern":  ".*/(?P<collection_id>.*)_(?P<item_id>(.*)_(?P<year>\\d{4})_(.*))_(?P<asset_type>.*).tiff$",
            "period": "yearly",
            "fixed_values": {}
        }},
    "item_assets" : all_assets

}

#now we write out the basic_config
with open(f"{collection}-config-collection.json","w") as f:
    f.write(json.dumps(basic_config, indent=2))

