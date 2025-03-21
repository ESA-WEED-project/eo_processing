from __future__ import annotations
from typing import TypedDict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from eo_processing.utils.storage import WEED_storage

openEO_bbox_format = TypedDict('openEO_bbox_format', {'east': float,
                                                      'south': float,
                                                      'west': float,
                                                      'north': float,
                                                      'crs': str})

storage_option_format = TypedDict('storage_option_format', {'workspace_export': bool,
                                                            'S3_prefix': Optional[str],
                                                            'local_S3_needed': bool,
                                                            'WEED_storage': Optional['WEED_storage']})

s3_credentials_format = TypedDict('s3_credentials_format', {'s3_access_key': str,
                                                            's3_secret_key': str,
                                                            's3_endpoint': str,
                                                            'bucket_name': str})
