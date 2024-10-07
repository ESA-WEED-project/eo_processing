from eo_processing.openeo.processing import VI_LIST, RADAR_LIST, S2_SCALING
from eo_processing.openeo.preprocessing import S2_BANDS

# ---------------------------------------------------
# standard processing options
TARGET_CRS = 3035                   # can be all known EPSG codes
S1_ORBITDIRECTION = 'DESCENDING'    #
TARGET_RESOLUTION = 10.
TIME_INTERPOLATION = False
TS_INTERVAL = 'dekad'
MASKING_ALGO = 'mask_scl_dilation'

# ---------------------------------------------------
# Job options for OpenEO

OPENEO_EXTRACT_JOB_OPTIONS = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "8G",
    "driver-cores": "2",
    "executor-memory": "3G",
    "executor-memoryOverhead": "2G",
    "executor-cores": "2",
    "max-executors": "50",
    "soft-errors": "true"
}

OPENEO_EXTRACT_CREO_JOB_OPTIONS = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "2G",
    "driver-cores": "1",
    "executor-memory": "2000m",
    "executor-memoryOverhead": "3500m",
    "executor-cores": "4",
    "executor-request-cores": "400m",
    "max-executors": "200"
}

OPENEO_EXTRACT_CDSE_JOB_OPTIONS = {
    "driver-memory": "8G",
    "driver-memoryOverhead": "5G",
    "driver-cores": "1",
    "executor-cores": "1",
    "executor-request-cores": "800m",
    "executor-memory": "1500m",
    "executor-memoryOverhead": "2500m",
    "max-executors": "25",
    "executor-threads-jvm": "7",
    "logging-threshold": "info"
}

# ---------------------------------------------------
# COLLECTION options

# Collection definitions on Terrascope
_TERRASCOPE_COLLECTIONS = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CREO
_CREO_COLLECTIONS = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on Sentinelhub
_SENTINELHUB_COLLECTIONS = {
    'S2_collection': "SENTINEL2_L2A_SENTINELHUB",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CDSE
_CDSE_COLLECTIONS = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

def _get_default_job_options():
    return OPENEO_EXTRACT_JOB_OPTIONS

def get_job_options(provider: str = None):

    job_options = _get_default_job_options()

    if 'creo' in provider.lower():
        job_options.update(OPENEO_EXTRACT_CREO_JOB_OPTIONS)
    if provider.lower() == 'cdse' or provider.lower() == 'cdse-staging':
        job_options.update(OPENEO_EXTRACT_CDSE_JOB_OPTIONS)

    return job_options

def get_collection_options(provider: str):

    if provider.lower() == 'terrascope' or provider.lower() == 'development':
        return _TERRASCOPE_COLLECTIONS
    elif provider.lower() == 'sentinelhub' or provider.lower() == 'shub':
        return _SENTINELHUB_COLLECTIONS
    elif 'creo' in provider.lower():
        return _CREO_COLLECTIONS
    elif provider.lower() == 'cdse' or provider.lower() == 'cdse-staging':
        return _CDSE_COLLECTIONS
    else:
        raise ValueError(f'Provider `{provider}` not known.')

def get_standard_processing_options(provider: str, task: str = 'raw_extraction'):

    if task == 'raw_extraction':
        proc_opt = {
            "provider": provider,
            "s1_orbitdirection": S1_ORBITDIRECTION,
            "target_crs": TARGET_CRS,
            "resolution": TARGET_RESOLUTION,
            "time_interpolation": TIME_INTERPOLATION,
            "ts_interval": TS_INTERVAL,
            "SLC_masking_algo": MASKING_ALGO,
            "S2_bands": S2_BANDS   # we have to create a copy of the constant list
        }
    elif (task == 'feature_generation') or (task == 'vi_generation'):
        proc_opt = {
            "provider": provider,
            "s1_orbitdirection": S1_ORBITDIRECTION,
            "target_crs": TARGET_CRS,
            "resolution": TARGET_RESOLUTION,
            "time_interpolation": TIME_INTERPOLATION,
            "ts_interval": TS_INTERVAL,
            "SLC_masking_algo": MASKING_ALGO,
            "S2_bands": S2_BANDS,
            "optical_vi_list" : VI_LIST,
            "radar_vi_list" : RADAR_LIST,
            "S2_scaling" : S2_SCALING,
            "S1_db_rescale" : True,
            "append" : True
        }
    else:
        raise ValueError(f'Task `{task}` not known.')
    return proc_opt

def get_advanced_options(provider: str, s1_orbitdirection=S1_ORBITDIRECTION, target_crs=TARGET_CRS,
                         resolution=TARGET_RESOLUTION, ts_interpolation=TIME_INTERPOLATION,
                         ts_interval=TS_INTERVAL, slc_masking=MASKING_ALGO, S2_bands = S2_BANDS,
                         optical_vi_list=VI_LIST, radar_vi_list=RADAR_LIST,
                         S2_scaling=S2_SCALING, S1_db_rescale=True, append=True):
    """ validates also the given options"""

    if s1_orbitdirection not in ['ASCENDING', 'DESCENDING', None]:
        raise ValueError(f'parameter for s1_orbitdirection: {s1_orbitdirection} is not valid.')

    if type(target_crs) != int:
        raise ValueError(f'parameter for target_crs must be an integer value.')

    if type(resolution) != int:
        if type(resolution) != float:
            raise ValueError(f'parameter for resolution must be an integer value.')

    if ts_interval not in ['day', 'week', 'dekad', 'month', 'season', 'year', None]:
        raise ValueError(f'parameter for ts_interpolation: {ts_interval} is not valid.')

    if type(ts_interpolation) != bool:
        raise ValueError(f'parameter for ts_interpolation must be an boolean.')

    if slc_masking not in ['mask_scl_dilation', 'satio', None]:
        raise ValueError(f'parameter for slc_masking: {slc_masking} is not valid.')

    if type(S2_bands) != list:
        raise ValueError(f'parameter for S2 reflectance bands must be an list.')

    if type(optical_vi_list) != list:
        raise ValueError(f'parameter for optical_vi_list must be an list.')

    if type(radar_vi_list) != list:
        raise ValueError(f'parameter for radar_vi_list must be an list.')

    if type(S2_scaling) != list:
        raise ValueError(f'parameter for S2_scaling must be an list.')

    if type(S1_db_rescale) != bool:
        raise ValueError(f'parameter for S1_db_rescale must be an boolean.')

    if type(append) != bool:
        raise ValueError(f'parameter for append must be an boolean.')

    proc_opt = {
        "provider": provider,
        "s1_orbitdirection": s1_orbitdirection,
        "target_crs": target_crs,
        "resolution": resolution,
        "time_interpolation": ts_interpolation,
        "ts_interval": ts_interval,
        "SLC_masking_algo": slc_masking,
        "S2_bands": S2_bands,
        "optical_vi_list": optical_vi_list,
        "radar_vi_list": radar_vi_list,
        "S2_scaling": S2_scaling,
        "S1_db_rescale": S1_db_rescale,
        "append": append
    }

    return proc_opt
