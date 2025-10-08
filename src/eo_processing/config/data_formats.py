from __future__ import annotations
from typing import TypedDict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from eo_processing.utils.storage import WEED_S3_storage

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
                                                            'bucket_name': str,
                                                            'export_workspace': str})

sql_credentials_format = TypedDict('sql_credentials_format', {'dbname': str,
                                                            'schema': str,
                                                            'password': str,
                                                            'host': str,
                                                            'port': str})

gdrive_credentials_format = TypedDict('gdrive_credentials_format', {'type': str,
                                                    'project_id': str,
                                                    'private_key_id': str,
                                                    'private_key': str,
                                                    'client_email': str,
                                                    'client_id': str,
                                                    'auth_uri': str,
                                                    'token_uri': str,
                                                    'auth_provider_x509_cert_url': str,
                                                    'universe_domain': str
                                                })
stac_credentials_format = TypedDict('stac_credentials_format',  {"CLIENT_ID": str,
                                                    "CLIENT_SECRET": str,
                                                    "TOKEN_URL": str,
                                                    "catalog_url": str
                                                })

                                   