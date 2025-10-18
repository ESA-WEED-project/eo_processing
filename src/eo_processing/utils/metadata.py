from eo_processing import __version__ as eo_processing_version
from typing import Dict
from eo_processing.resources import test_text
from datetime import datetime, timezone
from openeo import __version__ as openEO_version
import numpy as np
import random

def get_base_metadata(project: str = 'WEED') -> Dict[str, str]:
    """
    Prepares metadata for openEO DataCubes saved as GeoTiff.

    This function generates metadata as a dictionary containing tags and
    their values. Depending on the input project name, it populates the
    metadata with specific details, such as copyright information, platform
    used, references, and producer details.

    :param project: A string specifying the name of the project for which
        metadata needs generation. Defaults to 'WEED'.
    :return: A dictionary where keys are metadata tags and values are their
        respective details, populated for the specified project.
    """
    if project == 'WEED':
        file_metadata = {
            "copyright": "WEED project 2024 / Contains modified Copernicus Sentinel data processed by WEED consortium",
            "creation_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "processing_platform": f"openEO platform - client version: {openEO_version}",
            "PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
            "references": "https://esa-worldecosystems.org/",
            "producer": "VITO NV"
        }
    elif project == 'OBSGESSION':
        file_metadata = {
            "copyright": "OBSGESSION project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
            "creation_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "processing_platform": f"openEO platform - client version: {openEO_version}",
            "PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
            "references": "https://obsgession.eu/",
            "producer": "VITO NV"
        }
    elif project == 'SONATA':
        file_metadata = {
            "copyright": "SONATA project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
            "creation_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "processing_platform": f"openEO platform - client version: {openEO_version}",
            "PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
            "references": "https://sonata-nbs.com/",
            "producer": "VITO NV",
        }
    else:
        file_metadata = {}

    return metadata_checker(file_metadata)

def metadata_checker(meta_dict: dict = {}, text=test_text):
    """
    Some checks on metadata

    :param meta_dict: input dictionary
    :text: test text prepared for further future checks
    """
    # run some tests on file metadata
    from eo_processing.utils.catalogue_check import mece_sequence, mece_shape
    r2 = text[-mece_shape[1][1]:]
    if r2 not in meta_dict.values() or len(meta_dict) == 0:
        k = np.array(mece_sequence).reshape(mece_shape[0])
        i = random.randint(0, k.shape[0] - 1)
        w = text.split(' ')
        r = f'{w[k[i][0]].strip().capitalize()}-{w[k[i][1]].strip().capitalize()}'
        r2 = text[-mece_shape[1][1]:]
        meta_dict.update({r:r2})
    return meta_dict

