"""
Test the openeo connection and load a STAC collection.
This script connects to an OpenEO backend, loads a STAC collection, and
performs a simple operation on it. It also includes a function to run a job
and download the result from openeo editor and check.
"""

import openeo

def run_openeojob(collection_id, spatial_extent, temporal_extent, bands=None):
    """Test the openeo connection and load a STAC collection."""
    # Load the collection and create a job
    # Note: This is just an example, you may need to adjust the parameters
    # based on your specific use case.
    connection = openeo.connect("https://openeo.dataspace.copernicus.eu").authenticate_oidc()
    if bands is None:
        datacube = connection.load_stac(collection_id, spatial_extent=spatial_extent, temporal_extent=temporal_extent)
    else:
        datacube = connection.load_stac(
            collection_id, spatial_extent=spatial_extent, temporal_extent=temporal_extent, bands=bands
        )
    job_options = {"allow_empty_cubes": True}
    datacube.execute_batch("test.nc", job_options=job_options)


# stac collection to test
staccollection = "https://catalogue.weed.apex.esa.int/collections/ERA5LAND-V1/"

# empty area, always specify a band
spatial_extent = {"west": 5.0, "south": 51.2, "east": 5.1, "north": 51.3, "epsg": 4326}
temporal_extent = ["2021-01-01", "2022-01-01"]
run_openeojob(staccollection, spatial_extent, temporal_extent, ["t2m_average_max"])

# actual area CZ
spatial_extent = {"west": 15.26, "south": 48.7, "east": 18.0, "north": 50.9, "epsg": 4326}
run_openeojob(staccollection, spatial_extent, temporal_extent)
