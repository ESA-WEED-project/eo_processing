### This is at the moment  a non working prototype

import json
from openeo.rest import OpenEoApiError
from pathlib import Path
import logging
import fire
import collections
from time import sleep
import geopandas as gpd
import os


from eo_processing.openeo.processing import croptype_map
from eo_processing.utils import laea20km_id_to_extent, UTM20km_id_to_extent
from eo_processing.utils.jobmanager import WeedJobManager
from eo_processing._version import  __version__
from eo_processing.config import get_job_options, get_processing_options, get_collection_options
import openeo
from openeo.util import rfc3339

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("openeo_classification.cropmap")



def produce_on_creodias():
    produce_map(parallel_jobs=1, input_file="https://artifactory.vgt.vito.be:443/artifactory/auxdata-public/grids/LAEA-20km.gpkg",
                              status_file="/vitodata/EEA_HRL_VLCC/data/production/croptype_production_2022.csv",
                              output_dir="/vitodata/EEA_HRL_VLCC/data/production", backends=["creodias"])







def produce_map(parallel_jobs=20, input_file="terrascope_test.geojson", status_file="eu27_2021_terrascope_klein.csv", output_dir=".", backends=["terrascope"]):
    """
    Script to start and monitor jobs for the EU27 croptype map project in openEO platform CCN.
    The script can use multiple backends, to maximize throughput. Jobs are tracked in a CSV file, upon failure, the script can resume
    processing by pointing to the same csv file. Delete that file to start processing from scratch.

    @param provider: The data provider: terrascope - sentinelhub - creodias
    @param parallel_jobs:
    @param status_file: The local file where status should be tracked.
    @return:
    """

    #with Path(input_file).open('r') as f:
    #    tiles_to_produce = gpd.GeoDataFrame.from_features(json.load(f))
    tiles_to_produce = gpd.read_file(input_file)

    logger.info(
        f"Found {len(tiles_to_produce)} tiles to process.")


    def run(row, connection_provider, connection, provider):
        year = int(row['year'])
        name = row['name']
        version= row['version']


        #allow using multiple 'creodias' endpoints
        if "creodias" in provider:
            provider = "creodias"

        job_options = get_job_options(provider)
        processing_options = get_processing_options(provider)
        processing_options.update({'year':int(year)})
        if  isinstance(row ['Tile'],str):
            tiles = row['Tile']
            processing_options.update({'tiles': tiles})

        #adding check to make sure that model-tag and software version where not yet set.
        if row.description.replace('MODEL_TAG',processing_options["modeltag"]) \
                .replace('SOFTWARE_VERSION',__version__) == row.description:
            logger.warning("It seems this tile already has a software version/model_tag. \n" + \
                           "This should not be possible please check input csv.")

        if 'DOM' in input_file:
            EXTENT_20KM = UTM20km_id_to_extent(name)
            processing_options.update({'target_crs':int(name[:5])})
        else:     EXTENT_20KM = laea20km_id_to_extent(name)

        print(f"submitting job to {provider}")


        clf_results = croptype_map(EXTENT_20KM,
                                   connection,provider, processing_options)
        try:
            job = clf_results.create_job(
                title=row.title,
                description=(row.description).replace('MODEL_TAG',processing_options["modeltag"])\
                                            .replace('SOFTWARE_VERSION',__version__),
                out_format="GTiff",
                job_options=job_options, overview_method="mode", filename_prefix=f"CROP_{year}_{name}-03035-010m_{version}")#colormap=cropclass.openeo.croptype_colors())
        except:
            sleep(60)
            job = clf_results.create_job(
                title=row.title,
                description=(row.description).replace('MODEL_TAG',processing_options["modeltag"]) \
                    .replace('SOFTWARE_VERSION',__version__),
                out_format="GTiff",
                job_options=job_options, overview_method="mode", filename_prefix=f"CROP_{year}_{name}-03035-010m_{version}")#colormap=cropclass.openeo.croptype_colors())

        return job

    creo = openeo.connect("openeo.creo.vito.be", default_timeout=60).authenticate_oidc()

    manager = WeedJobManager()
    if "terrascope" in backends:
        manager.add_backend(
            "terrascope", connection=terrascope, parallel_jobs=parallel_jobs)

    if "sentinelhub" in backends:
        manager.add_backend(
            "sentinelhub", connection=terrascope, parallel_jobs=parallel_jobs)
    if "creodias" in backends:
        manager.add_backend("creodias", connection=creo, parallel_jobs=1)

    manager.run_jobs(
        df=tiles_to_produce,
        start_job=run,
        output_file=Path(status_file)
    )


if __name__ == '__main__':
  fire.Fire(produce_on_creodias())