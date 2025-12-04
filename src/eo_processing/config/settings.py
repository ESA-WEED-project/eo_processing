from __future__ import annotations
from typing import List, Optional, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from eo_processing.config.data_formats import storage_option_format
    from eo_processing.utils.storage import WEED_storage

# ---------------------------------------------------
# standard processing options
TARGET_CRS: int = 3035                   # can be all known EPSG codes
S1_ORBITDIRECTION: str = 'DESCENDING'    #
TARGET_RESOLUTION: float = 10.
TIME_INTERPOLATION: bool = False
TS_INTERVAL: str = 'dekad'
S2_TEMPORAL_REDUCER: str = 'median'
S1_TEMPORAL_REDUCER: str = 'mean'
MASKING_ALGO: str = 'mask_scl_dilation'
APPLY_CLOUD_MASK: bool = True
S2_MAX_CLOUD_COVER = 95
S2_BANDS = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]

VI_LIST = ['NDVI',
           'AVI',
           'CIRE',
           'NIRv',
           'NDMI',
           'NDWI',
           'BLFEI',
           'MNDWI',
           'NDVIMNDWI',
           'S2WI',
           'S2REP',
           'IRECI']

RADAR_LIST = ['VHVVD',
              'VHVVR',
              'RVI']

S2_SCALING = [0, 10000, 0, 1.0]
S2_TILEID_LIST = None
SKIP_CHECK_S1: bool = False
SKIP_CHECK_S2: bool = False

# ---------------------------------------------------
# Planet Processing options
PLANET_MASKING_ALGO = str = 'mask_udm_dilation'
PLANET_BANDS = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08"] # Same for UDM2 and Spectral bands
PLANET_RESOLUTION = 3.0
PLANET_VI_LIST = ['NDVI',
           'AVI',
           'CIRE',
           'NIRv',
           'NDWI'
           ]
PLANET_SCALING = [0, 10000, 0, 1.0]

# ---------------------------------------------------
# Job options for OpenEO

OPENEO_EXTRACT_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "8G",
    "driver-cores": 2,
    "executor-memory": "3G",
    "executor-memoryOverhead": "2G",
    "executor-cores": 2,
    "max-executors": 50,
    "soft-errors": "true"
}

OPENEO_EXTRACT_CREO_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "2G",
    "driver-cores": 1,
    "executor-memory": "2000m",
    "executor-memoryOverhead": "3500m",
    "executor-cores": 4,
    "executor-request-cores": "400m",
    "max-executors": 200
}

OPENEO_EXTRACT_CDSE_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "8G",
    "driver-memoryOverhead": "5G",
    "driver-cores": 1,
    "executor-memory": "2000m",
    "executor-memoryOverhead": "256m",
    "python-memory": "2500m",
    "executor-cores": 1,
    "max-executors": 25,
    "logging-threshold": "info"
}

OPENEO_INFERENCE_CDSE_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "1000m",
    "driver-memoryOverhead": "1000m",
    "driver-cores": 1,
    "executor-memory": "1500m",
    "executor-memoryOverhead": "256m",
    "executor-cores": 1,
    "max-executors": 20,
    "python-memory": "4000m",
    "logging-threshold": "info",
    "udf-dependency-archives": [
        "https://s3.waw3-1.cloudferro.com/swift/v1/project_dependencies/onnx_deps_python311.zip#onnx_deps"
    ]
}

OPENEO_POINTEXTRACTION_CDSE_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "2G",
    "driver-memoryOverhead": "1G",
    "driver-cores": 1,
    "executor-memory": "2000m",
    "executor-memoryOverhead": "256m",
    "python-memory": "2500m",
    "executor-cores": 1,
    "max-executors": 25,
    "logging-threshold": "info"
}

OPENEO_CUBEEXTRACTION_CDSE_JOB_OPTIONS: Dict[str, str] = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "4G",
    "driver-cores": 1,
    "executor-memory": "2000m",
    "executor-memoryOverhead": "256m",
    "python-memory": "2500m",
    "executor-cores": 1,
    "max-executors": 20,
    "logging-threshold": "info"
}
# ---------------------------------------------------
# COLLECTION options

# Collection definitions on Terrascope
_TERRASCOPE_COLLECTIONS: Dict[str, str] = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CREO
_CREO_COLLECTIONS: Dict[str, str] = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on Sentinelhub
_SENTINELHUB_COLLECTIONS: Dict[str, str] = {
    'S2_collection': "SENTINEL2_L2A_SENTINELHUB",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CDSE
_CDSE_COLLECTIONS: Dict[str, str] = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

def _get_default_job_options() -> Dict[str, str]:
    """
    Retrieves the default job options for OpenEO extract operations.

    This function returns a predefined dictionary containing the default job
    options configured for use with OpenEO extract. These options are setup
    to maintain consistency and ensure reliability when performing extract
    operations.

    :return: A dictionary representing the default job options for OpenEO extract.
    """
    return OPENEO_EXTRACT_JOB_OPTIONS.copy()

def get_job_options(provider: str = None, task: str = 'raw_extraction') -> Dict[str, str]:
    """
    Retrieve job options based on the specified provider and task.

    This function returns a dictionary of job options that are determined by the
    given provider and task. If the provider specified belongs to certain defined
    categories, additional job options specific to that provider and possibly the
    task are merged into the default job options.

    :param provider:
        The name of the provider, which is used to determine specific job options.
        Custom job options are applied if the provider matches certain criteria.
    :param task:
        The type of task for which the job options are being retrieved. Default
        is 'raw_extraction'. This parameter can affect the selected job options
        for certain providers. Currently, the following tasks are supported in CDSE:
        ínference, point_extraction.

    :return:
        A dictionary containing the merged set of job options for the given
        provider and task. This includes a combination of the default options
        and any applicable overrides for the provider and task.
    """
    job_options = _get_default_job_options()

    if 'creo' in provider.lower():
        job_options.update(OPENEO_EXTRACT_CREO_JOB_OPTIONS)
    if provider.lower() == 'cdse' or provider.lower() == 'cdse-staging':
        if task in ['inference']:
            job_options.update(OPENEO_INFERENCE_CDSE_JOB_OPTIONS)
        elif task in ['point_extraction']:
            job_options.update(OPENEO_POINTEXTRACTION_CDSE_JOB_OPTIONS)
        elif task in ['feature_generation']:
            job_options.update(OPENEO_CUBEEXTRACTION_CDSE_JOB_OPTIONS)
        else:
            job_options.update(OPENEO_EXTRACT_CDSE_JOB_OPTIONS)

    return job_options

def get_collection_options(provider: str) -> Dict[str, str]:
    """
    Retrieve available collection options for a given data provider.

    This function takes the name of a data provider as input and returns a
    dictionary of corresponding collection options. It maintains internal
    mappings for different supported providers and checks the input string
    to determine which provider's collections to return. If the provider name
    does not match any known provider, a `ValueError` is raised.

    :param provider: The name of the data provider. Supported values (case
        insensitive) include 'terrascope', 'development', 'sentinelhub', 'shub',
        'creo' (or any string containing 'creo'), 'cdse', and 'cdse-staging'.
    :return: A dictionary containing collection options for the specified
        provider.

    :raises ValueError: If the given provider is not recognized.
    """
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

def get_standard_processing_options(provider: str, task: str = 'raw_extraction') -> dict:
    """
    Generate standard processing options based on the provider and specified task.

    This function creates and returns a dictionary containing processing options
    required to perform standard operations depending on the type of task. It can
    handle tasks such as `raw_extraction`, `feature_generation`, and `vi_generation`.
    The returned dictionary includes parameters such as provider, resolution,
    coordinate reference system, time interpolation, spectral bands, and other
    task-specific configurations. An unknown or unsupported task value will raise
    a ValueError.

    :param provider: The name of the data provider, which determines specific
        configurations for the generated processing options.
    :param task: The type of task for which the processing options are needed.
        Supported tasks are 'raw_extraction', 'feature_generation', and
        'vi_generation'. Defaults to 'raw_extraction'.

    :return: A dictionary containing processing parameters tailored to the
        specified task, including key configurations for spectral, spatial,
        and temporal processing.
    """
    if task == 'raw_extraction':
        proc_opt = {
            "provider": provider,
            "s1_orbitdirection": S1_ORBITDIRECTION,
            "target_crs": TARGET_CRS,
            "resolution": TARGET_RESOLUTION,
            "time_interpolation": TIME_INTERPOLATION,
            "ts_interval": TS_INTERVAL,
            "S2_temporal_reducer": S2_TEMPORAL_REDUCER,
            "S1_temporal_reducer": S1_TEMPORAL_REDUCER,
            "SLC_masking_algo": MASKING_ALGO,
            "S2_bands": S2_BANDS,   # we have to create a copy of the constant list
            "s2_tileid_list": S2_TILEID_LIST,
            "skip_check_S1": SKIP_CHECK_S1,
            "skip_check_S2": SKIP_CHECK_S2,
            "apply_cloud_mask": APPLY_CLOUD_MASK,
        }
    elif (task == 'feature_generation') or (task == 'vi_generation'):
        proc_opt = {
            "provider": provider,
            "s1_orbitdirection": S1_ORBITDIRECTION,
            "target_crs": TARGET_CRS,
            "resolution": TARGET_RESOLUTION,
            "time_interpolation": TIME_INTERPOLATION,
            "ts_interval": TS_INTERVAL,
            "S2_temporal_reducer": S2_TEMPORAL_REDUCER,
            "S1_temporal_reducer": S1_TEMPORAL_REDUCER,
            "SLC_masking_algo": MASKING_ALGO,
            "S2_bands": S2_BANDS,
            "s2_tileid_list": S2_TILEID_LIST,
            "skip_check_S1": SKIP_CHECK_S1,
            "skip_check_S2": SKIP_CHECK_S2,
            "apply_cloud_mask": APPLY_CLOUD_MASK,
            "optical_vi_list" : VI_LIST,
            "radar_vi_list" : RADAR_LIST,
            "S2_scaling" : S2_SCALING,
            "S1_db_rescale" : True,
            "append" : True
        }
    else:
        raise ValueError(f'Task `{task}` not known.')
    return proc_opt

def get_advanced_options(provider: str, s1_orbitdirection: str = S1_ORBITDIRECTION, target_crs: int = TARGET_CRS,
                         resolution: int | float = TARGET_RESOLUTION, ts_interpolation: bool = TIME_INTERPOLATION,
                         ts_interval: str = TS_INTERVAL, S2_temporal_reducer: str = S2_TEMPORAL_REDUCER,
                         S1_temporal_reducer: str = S1_TEMPORAL_REDUCER, slc_masking: str = MASKING_ALGO,
                         S2_bands: List[str] = S2_BANDS, s2_tileid_list: Optional[List[str]] = S2_TILEID_LIST,
                         skip_check_S1: bool = SKIP_CHECK_S1, skip_check_S2: bool = SKIP_CHECK_S2,
                         apply_cloud_mask: bool = APPLY_CLOUD_MASK,
                         optical_vi_list: List[str] = VI_LIST, radar_vi_list: List[str] = RADAR_LIST,
                         S2_scaling: List[int | float] = S2_SCALING, S1_db_rescale: bool = True,
                         append: bool = True) -> Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]:
    """
    Generates advanced processing options for use in remote sensing data analysis.

    This function validates the provided parameters for correctness and returns a
    dictionary containing processing options for both optical and radar remote sensing
    data. The function ensures detailed configuration of processing parameters, such
    as temporal reducers, masking algorithms, scaling, interpolation, and cloud masking.
    All parameters are validated for their appropriate types and acceptable values.

    :param provider: The data provider (e.g., 'Sentinel', 'Landsat').
    :param s1_orbitdirection: The orbit direction for Sentinel-1 data, must be 'ASCENDING',
                              'DESCENDING', or None.
    :param target_crs: The target Coordinate Reference System (CRS) as an integer value.
    :param resolution: The spatial resolution for processing, can be an integer or float.
    :param ts_interpolation: Whether to enable time series interpolation, must be a boolean.
    :param ts_interval: The time series interval, must be one of 'day', 'week', 'dekad',
                        'month', 'season', 'year', or None.
    :param S2_temporal_reducer: The temporal reducer for Sentinel-2 data, must be one
                                of 'median', 'mean', 'max', 'min', 'first', 'last',
                                'product', 'sd', 'sum', or 'variance'.
    :param S1_temporal_reducer: The temporal reducer for Sentinel-1 data, must be one
                                of 'median', 'mean', 'max', 'min', 'first', 'last',
                                'product', 'sd', 'sum', or 'variance'.
    :param slc_masking: The algorithm for SLC (Scan Line Corrector) masking, must be
                        'mask_scl_dilation', 'satio', or None.
    :param S2_bands: A list of Sentinel-2 reflectance bands to be included in the
                     processing.
    :param s2_tileid_list: An optional list of Sentinel-2 tile IDs to restrict the
                           processing, or None.
    :param skip_check_S1: Whether to skip validation checks for Sentinel-1 data,
                          must be a boolean.
    :param skip_check_S2: Whether to skip validation checks for Sentinel-2 data,
                          must be a boolean.
    :param apply_cloud_mask: Whether to apply cloud masking or to add as own band, must be a boolean. Not used if
                              slc_masking is set to None.
    :param optical_vi_list: A list of optical vegetation indices to include in the
                            processing.
    :param radar_vi_list: A list of radar vegetation indices to include in the
                          processing.
    :param S2_scaling: A list of scaling factors for Sentinel-2 bands, can contain
                       integers or floats. Will be used for VI generation only.
    :param S1_db_rescale: Whether to rescale Sentinel-1 data to decibel (DB) scale before VI generation,
                          must be a boolean.
    :param append: Whether to append the S1/S2 VI's to the reference bands or to replace them, must be a boolean.

    :raises ValueError: If any parameter is of invalid type or contains invalid value.

    :return: A dictionary containing the advanced processing options where
             keys are parameter names and values are the provided or default values.
    """

    if s1_orbitdirection not in ['ASCENDING', 'DESCENDING', None]:
        raise ValueError(f'parameter for s1_orbitdirection: {s1_orbitdirection} is not valid.')

    if type(target_crs) != int:
        raise ValueError(f'parameter for target_crs must be an integer value.')

    if type(resolution) != int:
        if type(resolution) != float:
            raise ValueError(f'parameter for resolution must be an integer value.')

    if ts_interval not in ['day', 'week', 'dekad', 'month', 'season', 'year', None]:
        raise ValueError(f'parameter for ts_interpolation: {ts_interval} is not valid.')

    if S2_temporal_reducer not in ['median', 'mean', 'max', 'min', 'first', 'last', 'product', 'sd', 'sum', 'variance']:
        raise ValueError(f'parameter for S2_temporal_reducer: {S2_temporal_reducer} is not valid.')

    if S1_temporal_reducer not in ['median', 'mean', 'max', 'min', 'first', 'last', 'product', 'sd', 'sum', 'variance']:
        raise ValueError(f'parameter for S2_temporal_reducer: {S1_temporal_reducer} is not valid.')

    if type(ts_interpolation) != bool:
        raise ValueError(f'parameter for ts_interpolation must be an boolean.')

    if slc_masking not in ['mask_scl_dilation', 'satio', None]:
        raise ValueError(f'parameter for slc_masking: {slc_masking} is not valid.')

    if type(S2_bands) != list:
        raise ValueError(f'parameter for S2 reflectance bands must be an list.')

    if s2_tileid_list and type(s2_tileid_list) != list:
        raise ValueError(f'parameter for s2_tileid_list must be an list or None.')

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

    if type(skip_check_S1) != bool:
        raise ValueError(f'parameter for skip_check_S1 must be an boolean.')

    if type(skip_check_S2) != bool:
        raise ValueError(f'parameter for skip_check_S2 must be an boolean.')

    if type(apply_cloud_mask) != bool:
        raise ValueError(f'parameter for apply_cloud_mask must be an boolean.')

    proc_opt = {
        "provider": provider,
        "s1_orbitdirection": s1_orbitdirection,
        "target_crs": target_crs,
        "resolution": resolution,
        "time_interpolation": ts_interpolation,
        "ts_interval": ts_interval,
        "S2_temporal_reducer": S2_temporal_reducer,
        "S1_temporal_reducer": S1_temporal_reducer,
        "SLC_masking_algo": slc_masking,
        "S2_bands": S2_bands,
        "s2_tileid_list": s2_tileid_list,
        "skip_check_S1": skip_check_S1,
        "skip_check_S2": skip_check_S2,
        "apply_cloud_mask": apply_cloud_mask,
        "optical_vi_list": optical_vi_list,
        "radar_vi_list": radar_vi_list,
        "S2_scaling": S2_scaling,
        "S1_db_rescale": S1_db_rescale,
        "append": append
    }
    return proc_opt

def generate_storage_options(workspace_export: bool = False,
                             S3_prefix: Optional[str] = None,
                             local_S3_needed: bool = False,
                             storage: Optional[WEED_storage] = None) -> storage_option_format:
    """
    Generates a dictionary of storage options based on provided parameters. This function is used to configure
    the storage settings for exporting results and handling S3 storage interactions.

    :param workspace_export: A boolean indicating whether the results should be exported to the workspace (S3).
    :param S3_prefix: A string representing the S3 bucket prefix to use for exporting. Mandatory if workspace
                      export to S3 is enabled.
    :param local_S3_needed: A boolean specifying whether a local copy of the S3 data is required.
    :param storage: A WEED_storage object representing the storage configuration when a local copy of S3 data
                    is needed.

    :return: A dictionary containing the configured storage options, including the workspace export flag,
             S3 prefix, local S3 requirements, and storage details.

    :raises ValueError: If `workspace_export` is True but no `S3_prefix` value is provided.
    :raises ValueError: If both `local_S3_needed` and `workspace_export` are True but no storage object is defined.
    """
    if not workspace_export:
       return {'workspace_export': workspace_export, 'S3_prefix': None, 'local_S3_needed': False, 'WEED_storage': None}
    else:
        storage_options =  {'workspace_export':workspace_export,
                            'S3_prefix': S3_prefix,
                            'local_S3_needed':local_S3_needed,
                            'WEED_storage': storage}

    #just some checks to avoid missing parameters
    if workspace_export and not S3_prefix:
        raise print("You want to export the openEO results to S3, please specify the S3_prefix parameter.")

    if workspace_export and not storage:
        raise print("A storage object has to be defined to specify the export_workspace name "
                    "and/or allow local_file_copy.")
    return storage_options
