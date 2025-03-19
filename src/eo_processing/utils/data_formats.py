from typing import TypedDict

openEO_bbox_format = TypedDict('openEO_bbox_format', {'east': float,
                                                      'south': float,
                                                      'west': float,
                                                      'north': float,
                                                      'crs': str})