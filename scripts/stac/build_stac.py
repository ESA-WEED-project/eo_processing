"""
Build stac:
1. Load config file from toml
2. Set environment variables to read s3 bucket
3. Build stac
4. Check if the collection is built or exists. This function also defined some parameters.
5. Validate data using pystac validate_all functionality. And also check the input number of assets from stac builder and validate_all.
"""

from upath import UPath  # for s3 storage
from pystac import Collection
from eo_processing.utils.stac import (
    get_datafrom_toml,
    buildcollection_locally,
    set_s3bucket_env,
    check_collection,
)

# get configuration details from config.toml
filename = "config.toml"
config = get_datafrom_toml(filename)

# Set environment variables for S3 bucket access
set_s3bucket_env(config)

# build stac
coll_inputs = config["stacbuild"]
inputdir = UPath(coll_inputs["INPUT_DATADIR"])
configfile = coll_inputs["INPUT_CONFIG_JSON"]
filepattern = coll_inputs["FILEPATTERN"]
overwrite = config["stacbuild"]["OVERWRITECOLLECTION"]
no_input_assets = buildcollection_locally(inputdir, configfile, filepattern, overwrite)

# CHECK IF COLLECTION WAS CREATED AND FILES DO EXIST
config = check_collection(config)

# VALIDATE COLLECTION
# stac validation
print("VALIDATION:")
# assume the collection consists of linked items
collection = Collection.from_file(config["weedstac"]["data"]["COLLECTION_JSON"])
noitems = collection.validate_all()
print(f"  Number of items created in collection is {noitems}")
# if the number of assets are proper
noassets = 0
for _i in collection.get_items():
    noassets += len(_i.assets)
print(f"  Number of assets in collection is {noassets} and input is {no_input_assets}")
