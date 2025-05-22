"""
delete a collection on the STAC server.

1. Load config file from toml
2. Authenticate to the STAC server
3. Delete the collection from the STAC server
"""

from eo_processing.utils.stac import (
    get_datafrom_toml,
    delete_collection,
    get_bearer_auth
)

# read input toml file
config = get_datafrom_toml("config_delete.toml")
# Check if collection exists locally and some information to config dict.

# authentication
auth = get_bearer_auth(config["weedstac"]["auth"])
print(f"Bearer token: {auth.token}")

# DELETE
stacdata = config["weedstac"]["data"]
url = stacdata["CATALOGUE_URL"] + "/collections/" + stacdata["COLLECTION"]
delete_collection(auth, url)
