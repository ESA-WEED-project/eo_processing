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

from eo_processing.utils.stac import (
    get_datafrom_toml,
    delete_collection,
)

# read input toml file
config = get_datafrom_toml("config_delete.toml")
# Check if collection exists locally and some information to config dict.

# DELETING
# authentication
auth = get_bearer_auth(config["weedstac"]["auth"])
print(f"Bearer token: {auth.token}")

# DELETE
stacdata = config["weedstac"]["data"]
url = stacdata["CATALOGUE_URL"] + "/collections/" + stacdata["COLLECTION"]
delete_collection(auth, url)
