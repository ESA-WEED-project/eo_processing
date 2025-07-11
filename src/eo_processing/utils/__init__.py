__all__ = ['laea20km_id_to_extent', 'reproj_bbox_to_ll', 'bbox_area']

# Lazy loading extents
def __getattr__(name):
    if name in __all__:
        from .geoprocessing import laea20km_id_to_extent, reproj_bbox_to_ll, bbox_area
        return locals()[name]
    raise AttributeError(f"module {__name__} has no attribute {name}")