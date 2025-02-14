from eo_processing.openeo.processing import VI_LIST, RADAR_LIST, S2_SCALING
from eo_processing.openeo.preprocessing import S2_BANDS
from eo_processing.utils.storage import WEED_storage
from typing import List, TypedDict, Optional

# ---------------------------------------------------
# standard processing options
TARGET_CRS: int = 3035                   # can be all known EPSG codes
S1_ORBITDIRECTION: str = 'DESCENDING'    #
TARGET_RESOLUTION: float = 10.
TIME_INTERPOLATION: bool = False
TS_INTERVAL: str = 'dekad'
MASKING_ALGO: str = 'mask_scl_dilation'

# ---------------------------------------------------
# Job options for OpenEO

OPENEO_EXTRACT_JOB_OPTIONS: dict = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "8G",
    "driver-cores": "2",
    "executor-memory": "3G",
    "executor-memoryOverhead": "2G",
    "executor-cores": "2",
    "max-executors": "50",
    "soft-errors": "true"
}

OPENEO_EXTRACT_CREO_JOB_OPTIONS: dict = {
    "driver-memory": "4G",
    "driver-memoryOverhead": "2G",
    "driver-cores": "1",
    "executor-memory": "2000m",
    "executor-memoryOverhead": "3500m",
    "executor-cores": "4",
    "executor-request-cores": "400m",
    "max-executors": "200"
}

OPENEO_EXTRACT_CDSE_JOB_OPTIONS: dict = {
    "driver-memory": "8G",
    "driver-memoryOverhead": "5G",
    "driver-cores": "1",
    "executor-cores": "1",
    "executor-request-cores": "800m",
    "executor-memory": "1500m",
    "executor-memoryOverhead": "2500m",
    "max-executors": "15",
    "executor-threads-jvm": "7",
    "logging-threshold": "info"
}

OPENEO_INFERENCE_CDSE_JOB_OPTIONS: dict = {
    "driver-memory": "500m",
    "driver-memoryOverhead": "2000m",
    "driver-cores": "1",
    "executor-cores": "1",
    "executor-request-cores": "800m",
    "executor-memory": "2000m",
    "executor-memoryOverhead": "500m",
    "max-executors": "15",
    "executor-threads-jvm": "7",
    "logging-threshold": "info",
    "python-memory": "4000m"
}

OPENEO_POINTEXTRACTION_CDSE_JOB_OPTIONS: dict = {
    "driver-memory": "2G",
    "driver-memoryOverhead": "1G",
    "driver-cores": "1",
    "executor-cores": "1",
    "executor-request-cores": "800m",
    "executor-memory": "1500m",
    "executor-memoryOverhead": "2500m",
    "max-executors": "15",
    "executor-threads-jvm": "7",
    "logging-threshold": "info"
}

# ---------------------------------------------------
# COLLECTION options

# Collection definitions on Terrascope
_TERRASCOPE_COLLECTIONS: dict = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CREO
_CREO_COLLECTIONS: dict = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on Sentinelhub
_SENTINELHUB_COLLECTIONS: dict = {
    'S2_collection': "SENTINEL2_L2A_SENTINELHUB",
    'S1_collection': "SENTINEL1_GRD"
}

# Collection definitions on CDSE
_CDSE_COLLECTIONS: dict = {
    'S2_collection': "SENTINEL2_L2A",
    'S1_collection': "SENTINEL1_GRD"
}

storage_option_format = TypedDict('storage_option_format', {'workspace_export': bool,
                                                            'S3_prefix': Optional[str],
                                                            'local_S3_needed': bool,
                                                            'WEED_storage': Optional[WEED_storage]})


def _get_default_job_options() -> dict:
    """
    Retrieves the default job options for OpenEO extract operations.

    This function returns a predefined dictionary containing the default job
    options configured for use with OpenEO extract. These options are setup
    to maintain consistency and ensure reliability when performing extract
    operations.

    :return: A dictionary representing the default job options for OpenEO extract.
    """
    return OPENEO_EXTRACT_JOB_OPTIONS

def get_job_options(provider: str = None, task: str = 'raw_extraction') -> dict:
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
        if task in ['point_extraction']:
            job_options.update(OPENEO_POINTEXTRACTION_CDSE_JOB_OPTIONS)
        else:
            job_options.update(OPENEO_EXTRACT_CDSE_JOB_OPTIONS)

    return job_options

def get_collection_options(provider: str) -> dict:
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


def get_advanced_options(provider: str, s1_orbitdirection: str = S1_ORBITDIRECTION, target_crs: int = TARGET_CRS,
                         resolution: int | float = TARGET_RESOLUTION, ts_interpolation: bool = TIME_INTERPOLATION,
                         ts_interval: str = TS_INTERVAL, slc_masking: str = MASKING_ALGO, S2_bands: List[str] = S2_BANDS,
                         optical_vi_list: List[str] = VI_LIST, radar_vi_list: List[str] = RADAR_LIST,
                         S2_scaling: List[int | float] = S2_SCALING, S1_db_rescale: bool = True,
                         append: bool = True) -> dict:
    """
    Generates and validates advanced processing options for geospatial data, encapsulating
    parameters such as data provider, spatial resolution, temporal interpolation, and band scaling.
    The options include settings for Sentinel-1 and Sentinel-2 data processing, enabling users
    to specify orbit direction, coordinate reference system, spectral and radar indices, and additional
    customizations. Validates all input arguments to ensure conformity with expected formats and values.

    :param provider: Specifies the data provider (e.g., 'ESA', 'NASA').
    :param s1_orbitdirection: Indicates the orbit direction for Sentinel-1 data, accepting 'ASCENDING',
        'DESCENDING', or None.
    :param target_crs: Target coordinate reference system as an integer EPSG code (e.g., 4326 for WGS84).
    :param resolution: Desired spatial resolution, accepting integer or float values.
    :param ts_interpolation: Determines whether temporal interpolation is applied, as a boolean flag.
    :param ts_interval: Temporal interval for interpolation, with valid values including 'day', 'week',
        'dekad', 'month', 'season', 'year', or None.
    :param slc_masking: Specifies the Sentinel-2 masking algorithm, with valid options such as
        'mask_scl_dilation', 'satio', or None.
    :param S2_bands: List of Sentinel-2 spectral bands to include in processing.
    :param optical_vi_list: List of optical vegetation indices for retrieval.
    :param radar_vi_list: List of radar vegetation indices for retrieval.
    :param S2_scaling: List of scaling factors for Sentinel-2 bands, containing integer or float values.
    :param S1_db_rescale: Boolean flag indicating if Sentinel-1 data should be scaled to decibels.
    :param append: Boolean flag to determine whether to append processed data to an existing dataset.

    :return: A dictionary containing validated and structured advanced processing options.
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

def generate_storage_options(workspace_export: bool = False, S3_prefix: Optional[str] = None,
                             local_S3_needed: bool = False, storage: Optional[WEED_storage] = None):
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
       return {'workspace_export': workspace_export, 'S3_prefix': None, 'local_S3_needed': False, 'storage': None}
    else:
        storage_options =  {'workspace_export':workspace_export,
                            'S3_prefix': S3_prefix,
                            'local_S3_needed':local_S3_needed}

    #just some checks to avoid missing parameters
    if workspace_export and not S3_prefix  :
        raise print("You want to export the openEO results to S3, please specify the S3_prefix parameter.")

    if local_S3_needed and workspace_export:
        if not storage:
            raise print("A local copy S3 data is wanted but no storage object is defined")
        storage_options.update({'WEED_storage': storage})
    else:
        storage_options.update({'WEED_storage': None})

    return storage_options
