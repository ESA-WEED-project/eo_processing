# Create collections for ECDC.
- TODO Creating COG files
- TODO Validating COG files
- Building a stac-collections locally from s3 storage data [build_stac.py]
- Validating the data  [build_stac.py]
- Uploading the data to weed stac server  [ecdccollection.py]
- Deleting the collection in weed stac server  [ecdccollection.py]

## Structure 
1. config.toml has the settings for
    - Authentication
        - To upload to s3 buckets [key: s3bucket]
        - To upload to weed stac [key: weedstac.auth]
    - Weed stac api CATALOGUE_URL [key: weedstac.data]
    - Stacbuilder settings [key: stacbuild]
        - Key INPUT_CONFIG_JSON refers to the json file used by stacbuilder to identify assets and has information on unique items and collection. It is important to create different json files for different collections. Also bands are defined in this file.
        - Key FILEPATTERN refers to patttern of the assets to glob
        - Key INPUT_DATADIR refers to the location of the data/assets (for eg: COG files) to read
2. functions.py
    - Contains all the functions needed to generate stac, validate, upload collection to weed stac api and delete a collection in weed stac api
3. build_stac.py 
   - Builds data from s3 storage and creates stac collection locally.
   - Validates the data.
4. upload_delete_collection.py
   - Uploads the data to weed stac server.
   - Deletes the collection in weed stac server.
