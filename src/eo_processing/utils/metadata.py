from eo_processing import __version__ as eo_processing_version
from typing import Dict
from datetime import datetime
from openeo import __version__ as openEO_version

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
            "creation_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "processing_platform": f"openEO platform - client version: {openEO_version}",
            "PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
            "references": "https://esa-worldecosystems.org/",
            "producer": "VITO NV"
        }
    elif project == 'OBSGESSION':
        file_metadata = {
            "copyright": "OBSGESSION project 2025 / Contains modified Copernicus Sentinel data processed by VITO",
            "creation_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "processing_platform": f"openEO platform - client version: {openEO_version}",
            "PROCESSING_SOFTWARE": f"eo_processing, version {eo_processing_version}",
            "references": "https://obsgession.eu/",
            "producer": "VITO NV"
        }
    else:
        file_metadata = {}

    return file_metadata
