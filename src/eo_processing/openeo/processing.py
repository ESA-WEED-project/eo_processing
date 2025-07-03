from __future__ import annotations
from openeo.rest.datacube import DataCube
from openeo.extra.spectral_indices import append_indices, compute_indices
from openeo.processes import array_create, ProcessBuilder, array_concat, subtract

from eo_processing.openeo.preprocessing import (extract_S2_datacube, extract_S1_datacube)
import openeo

from typing import Optional, Dict, Union, List, TYPE_CHECKING
from eo_processing.config.settings import VI_LIST, RADAR_LIST, S2_SCALING

if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def optical_indices(
        input_cube: DataCube,
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """creates vegetation indices times series cube from given datacube of optical EO data

    :param input_cube: openEO DataCube
    :param processing_options: parameters for the processing of the datacube (optical_vi_list, S2_scaling, append)
    :return: VI datacube merged of input_cube and vi results
    """
    # evaluate additional processing_options
    vi_list = processing_options.get("optical_vi_list", VI_LIST)
    input_scaling = processing_options.get("S2_scaling", S2_SCALING)
    append = processing_options.get("append", True)

    # convert input DataCube into float
    input_cube = input_cube.linear_scale_range(*input_scaling)

    # calculate VI's
    if append:
        vi_cube = append_indices(datacube=input_cube, indices=vi_list)
    else:
        vi_cube = compute_indices(datacube=input_cube, indices=vi_list, append=False)

    # TODO: convert the datacube back to int16 - using the output scaling functionality tested in one
    #  of the example notebooks

    return vi_cube

def radar_indices(
        input_cube: DataCube,
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """creates radar indices times series cube from given datacube of radar EO data

    :param input_cube: openEO DataCube
    :param processing_options: parameters for the processing of the datacube (radar_vi_list, db_rescaling, append)
    :return: VI datacube merged of input_cube and vi results
    """
    # evaluate additional processing_options
    vi_list = processing_options.get("radar_vi_list", RADAR_LIST)
    db_rescaling = processing_options.get("S1_db_rescale", True)
    append = processing_options.get("append", True)

    # convert input DataCube into float (db)
    if db_rescaling:
        input_cube = input_cube.apply_dimension(
            dimension="bands",
            process=lambda x: array_create(
                [(20. * x[0].log(base=10)) - 83.,
                 (20. * x[1].log(base=10)) - 83.])
        )

    # calculate all VI's manual since Spectral_Indices doesnt know the SENTINEL_1_GRD catalogID
    # TODO: activate the automatic calculation as below commented area
    if append:
        def compute_indices1(bands):
            VV = bands["VV"]
            VH = bands["VH"]
            RVI = (4 * VH) / (VV + VH)
            VHVVD = VH - VV
            VHVVR = VH / VV
            return array_create([VV, VH, RVI, VHVVD, VHVVR])

        vi_cube = input_cube.apply_dimension(
            dimension="bands",
            process=compute_indices1,
            context={"parallel": True,
                     "TileSize": 128}
        ).rename_labels("bands", ["VV", "VH", "RVI", "VHVVD", "VHVVR"])
    else:
        def compute_indices2(bands):
            VV = bands["VV"]
            VH = bands["VH"]
            RVI = (4 * VH) / (VV + VH)
            VHVVD = VH - VV
            VHVVR = VH / VV
            return array_create([RVI, VHVVD, VHVVR])

        vi_cube = input_cube.apply_dimension(
            dimension="bands",
            process=compute_indices2,
            context={"parallel": True,
                     "TileSize": 128}
        ).rename_labels("bands", ["RVI", "VHVVD", "VHVVR"])

    """
    # check the list if RVI is in which is not in Awesome Package
    RVI_flag = False
    if 'RVI' in vi_list:
        RVI_flag = True
        vi_list.remove('RVI')

        def compute_indices(bands):
            VV = bands["VV"]
            VH = bands["VH"]
            RVI = (4 * VH) / (VV + VH)
            return array_create([RVI])

        cube_RVI = input_cube.apply_dimension(
            dimension="bands",
            process=compute_indices,
            context={"parallel": True,
                     "TileSize": 128}
        ).rename_labels("bands", ["RVI"])

    # calculate VI's
    if append:
        vi_cube = append_indices(datacube=input_cube, indices=vi_list)
    else:
        vi_cube = compute_indices(datacube=input_cube, indices=vi_list, append=False)

    # add RVI if needed
    if RVI_flag:
        vi_cube = vi_cube.merge_cubes(cube_RVI)
    """

    return vi_cube

def generate_S1_indices(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
    # get the S2 input data pre-processed
    input_cube = extract_S2_datacube(connection, bbox, start, end, S2_collection=S2_collection,
                                     **processing_options)
    # call the VI generator
    result_cube = optical_indices(input_cube, **processing_options)

    return result_cube

def generate_indices_master_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A', S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
    # get the S2 indices
    indices_cube = generate_S2_indices(connection, bbox, start, end, S2_collection=S2_collection,
                                       **processing_options)
    # merge the S1 indices
    if S1_collection is not None:
        indices_cube = indices_cube.merge_cubes(generate_S1_indices(connection, bbox, start, end,
                                                                    S1_collection=S1_collection,
                                                                    **processing_options))
    return indices_cube

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
        input_timeseries.quantiles(probabilities=[0.02, 0.25, 0.5, 0.75, 0.98]),
        [input_timeseries.mean(), input_timeseries.sd(), input_timeseries.sum(),
         subtract(x=input_timeseries.quantiles(probabilities=[0.75]),
                  y=input_timeseries.quantiles(probabilities=[0.25]))])

def calculate_features_cube(input_data: DataCube) -> DataCube:
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
                                                        "TileSize": 128})
    # adapt the band names
    new_band_names = [
        band + "_" + stat
        for band in input_data.metadata.band_names
        for stat in ["p2", "p25", "median", "p75", "p98", "mean", "sd", "sum", "iqr"]
    ]
    features_cube = features_cube.rename_labels('bands', new_band_names)

    # remove some bands which make no sense :)
    # mainly from S2REP --> sd, sum, iqr
    bands_keep = [band for band in features_cube.metadata.band_names if
                  band not in ['S2REP_sd', 'S2REP_sum', 'S2REP_iqr', 'VV_sum', 'VH_sum', 'VHVVD_sum']]

    features_cube = features_cube.filter_bands(bands=bands_keep)

    return features_cube

def generate_S1_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
    # get the reflectance and VI time series cube
    input_data = generate_S1_indices(connection, bbox, start, end, S1_collection=S1_collection,
                                     **processing_options)
    # get features
    features_cube = calculate_features_cube(input_data)

    return features_cube

def generate_S2_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
    # get the reflectance and VI time series cube
    input_data = generate_S2_indices(connection, bbox, start, end, S2_collection=S2_collection,
                                     **processing_options)
    # get features
    features_cube = calculate_features_cube(input_data)

    return features_cube

def generate_master_feature_cube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A', S1_collection: str ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
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
    # get the reflectance and VI time series cube
    input_data = generate_indices_master_cube(connection, bbox, start, end, S2_collection=S2_collection,
                                              S1_collection=S1_collection, **processing_options)
    # get features
    features_cube = calculate_features_cube(input_data)

    return features_cube
