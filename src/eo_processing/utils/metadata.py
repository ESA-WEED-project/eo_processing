from eo_processing import __version__ as eo_processing_version
from typing import Dict, Optional
from eo_processing.resources import test_text
from datetime import datetime, timezone
from openeo import __version__ as openEO_version
import numpy as np
import random
import openeo
try:
    from habitat_mapping import __version__ as habitat_mapping_version
except:
    habitat_mapping_version = 'not_available'

def get_base_metadata(project: str = 'WEED', connection:Optional[openeo.Connection] = None) -> Dict[str, str]:
    """
    Generates and returns a dictionary of base metadata details for a specified project.

    :param project: The name of the project whose metadata is to be generated.
        Valid project names include 'WEED', 'OBSGESSION', and 'SONATA'.
        Defaults to 'WEED'.
    :param connection: An optional openEO connection object.
    :return: A dictionary containing metadata details specific to the project.
    """
    if connection:
        openEO_version_details = str(connection.version_info())
    else:
        openEO_version_details = f"{{'client': {openEO_version}}}"

    file_metadata = {
        "copyright": "VITO NV / NCA team",
        "creation_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "processing_platform": f"openEO platform, {openEO_version_details}",
        "EO_PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
        "producer": "VITO NV"
    }

    if project == 'WEED':
        file_metadata.update({
            "copyright": "WEED project 2024 / Contains modified Copernicus Sentinel data processed by WEED consortium",
            "HABITAT_MAPPING_SOFTWARE": f"HASH, version {habitat_mapping_version}",
            "references": "https://esa-worldecosystems.org/",
        })
    elif project == 'OBSGESSION':
        file_metadata.update({
            "copyright": "OBSGESSION project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
            "references": "https://obsgession.eu/",
        })
    elif project == 'SONATA':
        try:
            from sonata import __version__ as sonata_version
            file_metadata.update({
                "copyright": "SONATA project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
                "HABITAT_MAPPING_SOFTWARE": f"HASH (SONATA variant), version {sonata_version}",
                "references": "https://sonata-nbs.com/",
            })
        except:
            sonata_version = habitat_mapping_version
            file_metadata.update({
                "copyright": "SONATA project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
                "HABITAT_MAPPING_SOFTWARE": f"HASH, version {sonata_version}",
                "references": "https://sonata-nbs.com/",
            })
    else:
        pass

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

