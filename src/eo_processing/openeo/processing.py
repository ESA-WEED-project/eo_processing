from __future__ import annotations
import itertools
import openeo
from openeo.rest.datacube import DataCube
from openeo.extra.spectral_indices import append_indices, compute_indices
from openeo.processes import array_create, ProcessBuilder, array_concat, subtract
from openeo.metadata import metadata_from_stac
from eo_processing.openeo.preprocessing import (extract_S2_datacube, extract_S1_datacube,
                                                extract_planet_datacube)
from eo_processing.utils.stac_helper import get_stac_collection_url
from eo_processing.config.settings import VI_LIST, RADAR_LIST, S2_SCALING, \
    PLANET_VI_LIST, PLANET_SCALING, CHUNK_SIZE

from typing import Optional, Dict, Union, List, TYPE_CHECKING
if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def optical_indices(
        input_cube: DataCube,
        collection: str ='SENTINEL2_L2A',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """creates vegetation indices times series cube from given datacube of optical EO data.
       Currently Sentinel-2 and PlanetScope are supported

    :param input_cube: openEO DataCube
    :param collection: name of the collection used for the input data
    :param processing_options: parameters for the processing of the datacube (optical_vi_list, S2_scaling, append)
    :return: VI datacube merged of input_cube and vi results
    """
    # evaluate additional processing_options
    if collection == 'SENTINEL2_L2A':
        vi_list: List = processing_options.get("optical_vi_list", VI_LIST)
        input_scaling = processing_options.get("S2_scaling", S2_SCALING)
        append = processing_options.get("append", True)
        platform = 'Sentinel-2A'
    elif collection == 'PlanetScope':
        vi_list: List = processing_options.get("optical_vi_list", PLANET_VI_LIST)
        input_scaling = processing_options.get("planet_scaling", PLANET_SCALING)
        append = processing_options.get("append", True)
        platform = 'PlanetScope'
    else:
        raise ValueError ('No Valid collection given')

    # convert input DataCube into float
    input_cube = input_cube.linear_scale_range(*input_scaling)

    # calculate VI's
    if append:
        vi_cube = append_indices(datacube=input_cube, indices=vi_list, platform=platform)
    else:
        vi_cube = compute_indices(datacube=input_cube, indices=vi_list, append=False, platform=platform)

    # TODO: convert the datacube back to int16 - using the output scaling functionality tested in one
    #  of the example notebooks
    return vi_cube

def radar_indices(
        input_cube: DataCube,
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """creates radar indices times series cube from given datacube of radar EO data

    :param input_cube: openEO DataCube
    :param processing_options: parameters for the processing of the datacube (radar_vi_list, db_rescaling, append)
    :return: VI datacube merged of input_cube and vi results
    """
    # evaluate additional processing_options
    vi_list: List = processing_options.get("radar_vi_list", RADAR_LIST)
    db_rescaling = processing_options.get("S1_db_rescale", True)
    append = processing_options.get("append", True)
    chunk_size = processing_options.get("openeo_chunk_size", CHUNK_SIZE)
    platform = "sentinel1"

    # convert input DataCube into float (db)
    if db_rescaling:
        input_cube = input_cube.apply_dimension(
            dimension="bands",
            process=lambda x: array_create(
                [(20. * x[0].log(base=10)) - 83.,
                 (20. * x[1].log(base=10)) - 83.])
        )

    # calculate VI's
    if append:
        vi_cube = append_indices(datacube=input_cube, indices=vi_list, platform=platform)
    else:
        vi_cube = compute_indices(datacube=input_cube, indices=vi_list, platform=platform, append=False)
    return vi_cube

def generate_S1_indices(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, s1_orbitdirection, radar_vi_list, S1_db_rescale, append)
    :return: DataCube
    """
    # get the S1 input data pre-processed
    input_cube = extract_S1_datacube(connection, bbox, start, end, S1_collection=S1_collection, **processing_options)

    # call the VI generator
    result_cube = radar_indices(input_cube, **processing_options)

    return result_cube

def generate_S2_indices(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, optical_vi_list, S2_scaling, append, S2_bands)
    :return: DataCube
    """
    # Note: for cubes with a time dimension, no s2-nvbt band can be attached
    get_NVBT: bool = processing_options.get("get_NVBT", False)
    if get_NVBT:
        processing_options["get_NVBT"] = False

    # get the Sentinel-2 datacube as starting point
    input_cube = extract_S2_datacube(connection, bbox, start, end, S2_collection=S2_collection, **processing_options)
    # call the VI generator
    result_cube = optical_indices(input_cube, collection=S2_collection, **processing_options)

    return result_cube

def generate_indices_master_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A', S1_collection: Optional[str] ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, s1_orbitdirection, optical_vi_list,
            S2_scaling, append, S2_bands, radar_vi_list, S1_db_rescale)
    :return: DataCube
    """
    # get S2 indices cube - no nobs_perc band needed
    indices_cube = generate_S2_indices(connection, bbox, start, end, S2_collection=S2_collection,
                                       **processing_options)

    # merge the S1 indices
    if S1_collection is not None:
        indices_cube = indices_cube.merge_cubes(generate_S1_indices(connection, bbox, start, end,
                                                                    S1_collection=S1_collection,
                                                                    **processing_options))
    return indices_cube

def generate_indices_planet_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        planet_collection: str = 'PlanetScope',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed PlanetScope data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param planet_collection: (str): Collection name for Planet data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, UDM_masking_algo, optical_vi_list, planet_scaling, append, Palent_bands)
    :return: DataCube
    """
    # get the Planet input data pre-processed
    input_cube = extract_planet_datacube(connection, bbox, start, end, **processing_options)
    # call the VI generator
    result_cube = optical_indices(input_cube, collection=planet_collection, **processing_options)

    return result_cube

def _compute_features(input_timeseries: DataCube) -> ProcessBuilder:
    """
    Computes a set of statistical features from the given input time series. The computed
    features include quantiles at specified probabilities, the mean, the standard deviation,
    the sum, and the interquartile range (IQR) derived as the difference between the 75th
    and 25th percentiles.

    :param input_timeseries: An object representing the input time series with methods to
        compute quantiles, mean, standard deviation, and sum of its data.
    :return: Concatenated array of computed statistical features, including quantiles,
        mean, standard deviation, sum, and the interquartile range (IQR).
    """
    return array_concat(
        input_timeseries.quantiles(probabilities=[0.02, 0.05, 0.25, 0.5, 0.75, 0.95, 0.98]),
        [input_timeseries.mean(), input_timeseries.sd(), input_timeseries.sum(),
         subtract(x=input_timeseries.quantiles(probabilities=[0.75]),
                  y=input_timeseries.quantiles(probabilities=[0.25])),
         subtract(x=input_timeseries.quantiles(probabilities=[0.95]),
                  y=input_timeseries.quantiles(probabilities=[0.05]))
         ])

def calculate_features_cube(input_data: DataCube, chunk_size: int = CHUNK_SIZE) -> DataCube:
    """
    Calculates feature statistics for each time series within the input data cube. This function applies
    statistical summaries to the bands of the input `DataCube` across the time dimension (`t`). It then
    modifies the band names to reflect the applied statistical operations and further filters out bands
    with summaries that are not meaningful for the analysis. The processed output is returned as a new
    `DataCube`.

    :param input_data: The input `DataCube` object, which contains multi-dimensional spatio-temporal data
                       to process. Bands represent different variables or channels, and the time series
                       are evaluated along the `t` dimension.
    :return: Returns a `DataCube` object containing the processed features. The output includes bands
             with applied statistical summaries (e.g., mean, median, percentiles) and excludes irrelevant
             bands based on practical considerations.
    """
    # calculate the features
    features_cube = input_data.apply_dimension(dimension='t',
                                               process=_compute_features,
                                               target_dimension='bands',
                                               context={"parallel": True,
                                                        "TileSize": chunk_size})
    # adapt the band names
    new_band_names = [
        band + "_" + stat
        for band in input_data.metadata.band_names
        for stat in ["p2", "p5", "p25", "median", "p75", "p95", "p98", "mean", "sd", "sum", "iqr", "iqr0595"]
    ]

    features_cube = features_cube.rename_labels('bands', new_band_names)

    # remove some bands which make no sense :)
    # mainly from S2REP --> sd, sum, iqr
    bands_keep = [band for band in features_cube.metadata.band_names if
                  band not in ['S2REP_sd', 'S2REP_sum', 'S2REP_iqr', 'S2REP_iqr0595' , 'VV_sum', 'VH_sum', 'VHVVD_sum',
                               'S2-CLOUD-MASK_p2', 'S2-CLOUD-MASK_p5', 'S2-CLOUD-MASK_p25', 'S2-CLOUD-MASK_median',
                               'S2-CLOUD-MASK_p75','S2-CLOUD-MASK_p95', 'S2-CLOUD-MASK_p98', 'S2-CLOUD-MASK_mean',
                               'S2-CLOUD-MASK_sd','S2-CLOUD-MASK_iqr','S2-CLOUD-MASK_iqr0595', 'S2-CLOUD-MASK_sum']]

    features_cube = features_cube.filter_bands(bands=bands_keep)

    return features_cube

def generate_S1_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, s1_orbitdirection, radar_vi_list, S1_db_rescale, append)
    :return: DataCube with only features
    """
    chunk_size: int = processing_options.get("openeo_chunk_size", CHUNK_SIZE)

    # get the natural values and VI time series cube
    input_data = generate_S1_indices(connection, bbox, start, end, S1_collection=S1_collection, **processing_options)
    # get features
    features_cube = calculate_features_cube(input_data, chunk_size=chunk_size)

    return features_cube

def generate_S2_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, optical_vi_list, S2_scaling, append, S2_bands)
    :return: DataCube with only features
    """
    chunk_size: int = processing_options.get("openeo_chunk_size", CHUNK_SIZE)

    get_NVBT: bool = processing_options.get("get_NVBT", False)
    # feature cubes have no time dimension, so we can add the s2-nvbt band to the cube
    if get_NVBT:
        # get the reflectance and VI time series cube PLUS NOBS_perc
        input_cube, nvbt_band = extract_S2_datacube(connection, bbox, start, end, S2_collection=S2_collection,
                                                    **processing_options)
        # call the VI generator
        indices_cube = optical_indices(input_cube, collection=S2_collection, **processing_options)
    else:
        # get the reflectance and VI time series cube
        indices_cube = generate_S2_indices(connection, bbox, start, end, S2_collection=S2_collection,
                                           **processing_options)

    # get features
    features_cube = calculate_features_cube(indices_cube, chunk_size=chunk_size)

    # add the nobs_perc_band to the cube if needed
    if get_NVBT:
        features_cube = features_cube.merge_cubes(nvbt_band)

    return features_cube

def generate_planet_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        planet_collection: str ='PlanetScope',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed Planet data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param planet_collection: (str, optional): Collection name for Planet data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, UDM_masking_algo, optical_vi_list,
            planet_scaling, append, planet_bands)
    :return: DataCube
    """
    chunk_size: int = processing_options.get("openeo_chunk_size", CHUNK_SIZE)
    # get the Planet indices
    # get the reflectance and VI time series cube
    input_data = generate_indices_planet_cube(connection, bbox, start, end, planet_collection=planet_collection,
                                             **processing_options)
    # get features
    features_cube = calculate_features_cube(input_data, chunk_size=chunk_size)

    return features_cube

def generate_master_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A', S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, int, float, bool, List[str], List[Union[int, float]], None]])\
        -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, s1_orbitdirection, optical_vi_list,
            S2_scaling, append, S2_bands, radar_vi_list, S1_db_rescale)
    :return: DataCube with only features
    """
    chunk_size: int = processing_options.get("openeo_chunk_size", CHUNK_SIZE)

    ### create the full master indices cube
    get_NVBT: bool = processing_options.get("get_NVBT", False)
    # feature cubes have no time dimension, so we can add the nobs_perc band to the cube
    if get_NVBT:
        # get the reflectance and VI time series cube PLUS NOBS_perc
        input_cube, nvbt_band = extract_S2_datacube(connection, bbox, start, end, S2_collection=S2_collection,
                                                    **processing_options)
        # call the VI generator
        indices_cube = optical_indices(input_cube, collection=S2_collection, **processing_options)
    else:
        # get the reflectance and VI time series cube
        indices_cube = generate_S2_indices(connection, bbox, start, end, S2_collection=S2_collection,
                                           **processing_options)

    # merge the S1 indices
    if S1_collection is not None:
        indices_cube = indices_cube.merge_cubes(generate_S1_indices(connection, bbox, start, end,
                                                                    S1_collection=S1_collection,
                                                                    **processing_options))
    # get features
    features_cube = calculate_features_cube(indices_cube, chunk_size=chunk_size)

    # add the nobs_perc_band to the cube if needed
    if get_NVBT:
        features_cube = features_cube.merge_cubes(nvbt_band)

    return features_cube

def create_collections_list_from_bands(input_bands : List[str]):
    """
    Creates a list of (collections, band)  extracted from the provided bands.

    The function processes a list of input band strings to determine whether
    each band corresponds to a STAC collection. It verifies availability of the
    collection in the STAC catalog, using a specific STAC API for validation.
    If no corresponding collection is found, a message is printed.

    Parameters:
    input_bands: List[str]
        A list of strings representing the input bands to process. Each string
        typically contains information necessary to deduce the relevant STAC
        collection.

    Raises:
    TypeError
        If input_bands is not a list, or if any element in the list is not a string.
    """

    bands_list = []
    for band in input_bands:
        #first check if the band is a STAC catalogue (at the moment ony our own STAC api is checked.

        band_breakdown = band.split('-',2)

        if len(band_breakdown) == 2:
            print(f"No specific band name provided for {band}. Assuming all bands in collection.")
            bands_list.append((f"{band_breakdown[0]}-{band_breakdown[1]}", None))
        elif len(band_breakdown) == 3:
            bands_list.append((f"{band_breakdown[0]}-{band_breakdown[1]}", band_breakdown[2]))
        else:
            raise (f"It seems that this bandname is malformed. Please check the band name {band}.")


    #now we need to group this based on the collections
    temp = itertools.groupby(bands_list, key=lambda x: x[0])
    collections_list = [(key, [x[1] for x in group]) for key, group in temp]

    return collections_list

def generate_nonEO_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        collections_list: List[str],
        base_cube : DataCube,
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:

    """ Warper to generate the data cube of all nonEO data based on a collections  list of the form [(collection, [band1, band2, ...])]"""
    temporal_extent = [start, end]
    temporal_extent = None

    chunk_size: int = processing_options.get("openeo_chunk_size", CHUNK_SIZE)

    for collection, bands, reproj, year in collections_list:
        #first need to distinguish between STAC and collection
        #we assume that they will allways be an url type of link in contrary with a collections which should just be a name
        if temporal_extent:
            temporal_extent = [f"{year}-01-01T00:00:00Z", f"{year}-12-31T23:59:59Z"]
        STAC_url = get_stac_collection_url(collection)
        isSTAC = STAC_url is not None

        #secondly we know there are some specific case of reprojection EG DEM should be bilinear iso near
        isDEM = "DEM" in bands
        if reproj:
            reprojection_method = reproj
        else:
            reprojection_method = "near"

        # load the features from public STAC

        if isSTAC:
            if bands == []:
                bands = metadata_from_stac(STAC_url).band_names
            #to be checked does the temporal filtering work on eg WERN
            nonEO_feature_cube = connection.load_stac(STAC_url,
                                                      bands=bands,
                                                      temporal_extent=temporal_extent
                                                      )
            nonEO_feature_cube.result_node().update_arguments(featureflags={'tilesize': chunk_size})

        else:
            #if openeo the -v1 should be split off of the collection
            if bands == []:
                nonEO_feature_cube = connection.load_collection(collection.split('-')[0],
                                                                temporal_extent=temporal_extent)
                nonEO_feature_cube.result_node().update_arguments(featureflags={'tilesize': chunk_size})
                bands = nonEO_feature_cube.dimension_labels('bands')
            else:
                nonEO_feature_cube = connection.load_collection(collection.split('-')[0],
                                                                bands = bands,
                                                                temporal_extent = temporal_extent)
                nonEO_feature_cube.result_node().update_arguments(featureflags={'tilesize': chunk_size})
            if isDEM:
                # reduce the temporal domain since copernicus_30 collection is "special" and feature only are one time stamp
                nonEO_feature_cube = nonEO_feature_cube.reduce_dimension(dimension='t', reducer=lambda x: x.last(ignore_nodata=True))

        new_bands = [f"{collection}-{band}" for band in bands]


        # resample the cube to 10m and EPSG of corresponding 20x20km grid tile
        nonEO_feature_cube = nonEO_feature_cube.resample_spatial(projection=processing_options['target_crs'],
                                     resolution=processing_options['resolution'],
                                     method=reprojection_method)
        # drop the time dimension this only needs to be done for STAC
        if isSTAC:
            try:
                nonEO_feature_cube = nonEO_feature_cube.drop_dimension('t')
            except:
                # workaround if we still have the client issues with the time dimensions for STAC dataset with only one time stamp
                nonEO_feature_cube.metadata = nonEO_feature_cube.metadata.add_dimension("t", label=None, type="temporal")
                nonEO_feature_cube = nonEO_feature_cube.drop_dimension('t')


        nonEO_feature_cube = nonEO_feature_cube.rename_labels('bands', target = new_bands ,source = bands)

        # merge into the base data cube
        base_cube = base_cube.merge_cubes(nonEO_feature_cube)

    return base_cube
