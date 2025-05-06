"""
Upload the collection to the STAC server and delete it. It is important to
follow first three steps before either creating or deleting a collection from the STAC server.

1. Load config file from toml
2. Check if the collection exists also define the few parameters
3. Authenticate to the STAC server
4. Upload the collection to the STAC server
    - Create a collection or gets its url from stac server
    - Upload all items to the STAC server
5. Delete the collection from the STAC server
"""

from eo_processing.utils.ecdc_stac import (
    get_datafrom_toml,
    check_collection,
    get_bearer_auth,
    create_collection_url,
    ingest_all_items,
    delete_collection,
)

# read input toml file
data = get_datafrom_toml("config.toml")
# Check if collection exists locally
data = check_collection(data)

# UPLOADING and DELETING
# authentication
auth = get_bearer_auth(data["weedstac"]["auth"])
print(f"Bearer token: {auth.token}")

# UPLOADING COLLECTION
stacdata = data["weedstac"]["data"]
coll_id = create_collection_url(auth, stacdata)
# upload data
ingest_all_items(
    auth,
    stacdata["CATALOGUE_URL"],
    stacdata["COLLECTIONNAME"],
    stacdata["COLLECTIONPATH"],
)

# DELETE
stacdata = data["weedstac"]["data"]
url = stacdata["CATALOGUE_URL"] / "collections" / stacdata["collection"]
delete_collection(auth, url)
