DEFAULT_DEM_METADATA = {
    "id": "COPERNICUS_30",
    "cube:dimensions":{
        "x": {"type": "spatial"}, 
        "y": {"type": "spatial"}, 
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["DEM"]}
        },
    "summaries": {
        "eo:bands": [
        {"name": "DEM", "common_name": "DEM"}]
        }
}

DEFAULT_S1_METADATA = {
    "id": "SENTINEL1_GRD",
    "cube:dimensions":{
        "x": {"type": "spatial"}, 
        "y": {"type": "spatial"}, 
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["VV", "VH"]}
        },
    "summaries": {
        "eo:bands": [
        {"name": "VV", "common_name": "VV", "center_wavelength": 0.055},
        {"name": "VH", "common_name": "VH", "center_wavelength": 0.06}]
        }
}

DEFAULT_S2_METADATA = {
    "id": "SENTINEL2_L2A",
    "cube:dimensions": {
        "x": {"type": "spatial"},
        "y": {"type": "spatial"},
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12", "SCL"]}
        },
    "summaries": {
        "platform": ["sentinel-2a", "sentinel-2b"],
        "eo:bands": [
        {"name": "B01", "common_name": None, "center_wavelength": 0.443},
        {"name": "B02", "common_name": "blue", "center_wavelength": 0.4966},
        {"name": "B03", "common_name": "green", "center_wavelength": 0.560},
        {"name": "B04", "common_name": "red", "center_wavelength": 0.6645},
        {"name": "B05", "common_name": None, "center_wavelength": 0.7039},
        {"name": "B06", "common_name": None, "center_wavelength": 0.7402},
        {"name": "B07", "common_name": None, "center_wavelength": 0.7825},
        {"name": "B08", "common_name": "nir", "center_wavelength": 0.8351},
        {"name": "B8A", "common_name": None, "center_wavelength": 0.8648},
        {"name": "B09", "common_name": None, "center_wavelength": 0.945},
        {"name": "B11", "common_name": "swir16", "center_wavelength": 1.610},
        {"name": "B12", "common_name": "swir22", "center_wavelength": 2.190},
        {"name": "SCL", "common_name": "SCL", "center_wavelength": None}]
    }
}


DEFAULT_WERN_METADATA = {
    "id": "wern_features",
    "bands":['gdd5', 'cfvo', 'gsp', 'soc', 'pop', 'occur', 'clay', 'bdod', 'gst', 'bio12', 'cec', 'dist', 'sand', 'phh2o', 'scd'],
    "item_assets": {},
    "summaries": {},
    "assets": {}
}