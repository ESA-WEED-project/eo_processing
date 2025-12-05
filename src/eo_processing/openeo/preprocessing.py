from __future__ import annotations
import openeo
from openeo.processes import array_create, if_, is_nodata, power, array_contains
from openeo.rest.datacube import DataCube

from eo_processing.openeo.masking import scl_mask_erode_dilate, classify_udm2, udm2_mask_erode_dilate
from eo_processing.utils.catalogue_check import (catalogue_check_S1, catalogue_check_S2,
                                                 catalogue_check_CDSE_S1, catalogue_check_CDSE_S2)
from eo_processing.config.settings import S2_BANDS, PLANET_BANDS

from typing import Optional, Dict, Union, List, TYPE_CHECKING
if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def ts_datacube_extraction(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str ='SENTINEL2_L2A',
        S1_collection: Optional[str] ='SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, s1_orbitdirection, S2_bands)
    :return: DataCube
    """
    # get the Sentinel-2 datacube as starting point
    bands = extract_S2_datacube(connection, bbox, start, end,
                                S2_collection=S2_collection,
                                **processing_options)

    # add the Sentinel-1 data
    if S1_collection is not None:
        bands = bands.merge_cubes(extract_S1_datacube(connection, bbox, start, end,
                                                      S1_collection=S1_collection, **processing_options))
        # run an extra temporal filter.... not clear why up to now
        bands = bands.filter_temporal(start, end)

    # final forcing 16bit - UInt16 again - maybe not needed anymore
    bands = bands.linear_scale_range(0, 65534, 0, 65534)

    return bands

def extract_S1_datacube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S1_collection: str = 'SENTINEL1_GRD',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """ extract the Sentinel-1 data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S1_collection: (str): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, s1_orbitdirection)
    :return: DataCube
    """
    # evaluate additional processing options
    if ("creo" in processing_options.get("provider", "").lower()) or \
            (processing_options.get("provider", "").lower() == "cdse" and bbox is not None):
        catalogue_check = True
    else:
        catalogue_check = False

    if processing_options.get('skip_check_S1', False):
        catalogue_check = False

    isCreo = "creo" in processing_options.get("provider", "").lower()
    orbit_direction: Optional[str] = processing_options.get('s1_orbitdirection', None)
    target_crs: Optional[str] = processing_options.get("target_crs", None)
    target_res: float = processing_options.get("resolution", 10.)
    ts_interval: Optional[str] = processing_options.get("ts_interval", None)
    ts_reducer: str = processing_options.get("S1_temporal_reducer", "mean")
    ts_interpolation: bool = processing_options.get("time_interpolation", False)
    if ("creo" in processing_options.get("provider", "").lower()) or \
            (processing_options.get("provider", "").lower() == "terrascope") or \
            (processing_options.get("provider", "").lower() == "development") or \
            (processing_options.get("provider", "").lower() == "cdse"):
        flag_DEM = True
    else:
        flag_DEM = False

    # we have to check if enough data is available on creo platform
    if catalogue_check:
        if processing_options.get("provider", "").lower() == "cdse":
            orbit_direction = catalogue_check_CDSE_S1(orbit_direction, start, end, bbox)
        else:
            orbit_direction = catalogue_check_S1(orbit_direction, start, end, bbox)

    # convert the orbit direction parameter into openEO property
    if orbit_direction is not None:
        properties = {"sat:orbit_state": lambda orbdir: orbdir == orbit_direction}  # NOQA
    else:
        properties = {}

    # fix no-VH-data issue
    properties.update({"polarisation": lambda pol: pol == "VV&VH"})

    # Load collection
    bands = connection.load_collection(S1_collection,
                                       bands=['VH', 'VV'],
                                       spatial_extent=bbox,
                                       temporal_extent=[start, end],
                                       properties=properties)

    # compute backscatter if starting from raw GRD, otherwise assume preprocessed backscatter
    check_flag = False
    if S1_collection == "SENTINEL1_GRD":
        bands = bands.sar_backscatter(
            coefficient='sigma0-ellipsoid',
            local_incidence_angle=False,
            # DO NOT USE MAPZEN
            elevation_model='COPERNICUS_30' if flag_DEM else None,
            options={"implementation_version": "2",
                     "tile_size": 256, "otb_memory": 1024, "debug": False,
                     "elev_geoid": "/opt/openeo-vito-aux-data/egm96.tif"}
        )
        check_flag = True
    else:
        pass

    # warp and/or resample if needed
    if target_crs is not None:
        bands = bands.resample_spatial(projection=target_crs, resolution=target_res)
    else:
        bands = bands.resample_spatial(resolution=target_res)

    # time aggregation if wished
    if ts_interval is not None:
        bands = bands.aggregate_temporal_period(period=ts_interval, reducer=ts_reducer)

    # Linearly interpolate missing values if wished
    if ts_interpolation:
        bands = bands.apply_dimension(dimension="t", process="array_interpolate_linear")

    # Scale to Uint16 range
    if check_flag:
        # for CREO, rescaling also replaces nodata introduced by orfeo
        # with a low value
        # https://github.com/Open-EO/openeo-geopyspark-driver/issues/293
        # TODO: check if nodata is correctly handled in Orfeo
        bands = bands.apply_dimension(
            dimension="bands",
            process=lambda x: array_create(
                [if_(is_nodata(x[0]), 1, power(
                    base=10, p=(10.0 * x[0].log(base=10) + 83.) / 20.)),
                    if_(is_nodata(x[1]), 1, power(
                        base=10, p=(10.0 * x[1].log(base=10) + 83.) / 20.))]))
    else:
        bands = bands.apply_dimension(
            dimension="bands",
            process=lambda x: array_create(
                [power(base=10, p=(10.0 * x[0].log(base=10) + 83.) / 20.),
                 power(base=10, p=(10.0 * x[1].log(base=10) + 83.) / 20.)]))

    # Force a linear scale removing values not expected
    bands = bands.linear_scale_range(1, 65534, 1, 65534)

    return bands

def extract_S2_datacube(
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        S2_collection: str='SENTINEL2_L2A',
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """ extract the Sentinel-2 data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, S2_bands)
    :return: DataCube
    """
    # evaluate additional processing_options
    if ("creo" in processing_options.get("provider", "").lower()) or \
            (processing_options.get("provider", "").lower() == "cdse" and bbox is not None):
        catalogue_check = True
    else:
        catalogue_check = False

    if processing_options.get('skip_check_S2', False):
        catalogue_check = False

    target_crs: Optional[str] = processing_options.get("target_crs", None)
    target_res: float = processing_options.get("resolution", 10.)
    S2_bands: List[str] = processing_options.get("S2_bands", S2_BANDS)
    ts_interval: Optional[str] = processing_options.get("ts_interval", None)
    ts_reducer: str = processing_options.get("S2_temporal_reducer", "median")
    ts_interpolation: bool = processing_options.get("time_interpolation", False)
    masking: Optional[str] = processing_options.get("SLC_masking_algo", None)
    apply_mask: bool = processing_options.get("apply_cloud_mask", True)
    max_cloud_max: int = processing_options.get("S2_max_cloud_cover", 95.)
    s2_tileid_list: Optional[List[str]] = processing_options.get("s2_tileid_list", None)

    # check if the masking parameter is valid
    if masking not in ['satio', 'mask_scl_dilation', None]:
        raise ValueError(f'Unknown masking option `{masking}`')

    # we have to check if enough data is available on creo platform
    if catalogue_check:
        if processing_options.get("provider", "").lower() == "cdse":
            catalogue_check_CDSE_S2(start, end, bbox)
        else:
            catalogue_check_S2(start, end, bbox)

    #create filter for S2 tiles to limit amount of overlapping S2 input data
    properties = None
    if s2_tileid_list:
        if len(s2_tileid_list) == 1:
            properties= {"tileId": lambda tile_id: tile_id==s2_tileid_list[0]}
        else:
            properties = {"tileId": lambda tile_id: array_contains(s2_tileid_list, tile_id)}

    # request the needed datacube
    bands = connection.load_collection(
        S2_collection,
        bands=S2_bands,
        spatial_extent=bbox,
        temporal_extent=[start, end],
        max_cloud_cover=max_cloud_max,
        properties=properties
    )

    # warp and/or resample if needed
    if target_crs is not None:
        bands = bands.resample_spatial(projection=target_crs, resolution=target_res)
    else:
        bands = bands.resample_spatial(resolution=target_res)

    # apply cloud masking
    if masking == 'mask_scl_dilation':
        # we have to load the SCL mask as an extra cube to get it correctly working
        sub_collection = connection.load_collection(
            S2_collection,
            bands=["SCL"],
            spatial_extent=bbox,
            temporal_extent=[start, end],
            max_cloud_cover=max_cloud_max,
            properties=properties
        )
        # resample to 10m (needed for the correct kernels)
        if target_crs is not None:
            sub_collection = sub_collection.resample_spatial(resolution=10., projection=target_crs)
        else:
            sub_collection = sub_collection.resample_spatial(resolution=10.)

        scl_dilated_mask = sub_collection.process(
            "to_scl_dilation_mask",
            data=sub_collection,
            scl_band_name="SCL",
            kernel1_size=17,  # 17px dilation on a 10m layer
            kernel2_size=77,  # 77px dilation on a 10m layer
            mask1_values=[2, 4, 5, 6, 7],
            mask2_values=[3, 8, 9, 10, 11],
            erosion_kernel_size=3
        ).rename_labels("bands", ["S2-CLOUD-MASK"])

        if apply_mask:
            bands = bands.mask(scl_dilated_mask) # here I do trust the automatic resampling of the mask
        else:
            bands = bands.merge_cubes(scl_dilated_mask)
    elif masking == 'satio':
        # Apply satio-based mask
        mask = scl_mask_erode_dilate(
            connection,
            bbox,
            scl_layer_band=S2_collection + ':SCL',
            target_crs=target_crs,
            max_cloud_max=max_cloud_max)

        if apply_mask:
            bands = bands.mask(mask) # masks are automatically resampled/warped
        else:
            bands = bands.merge_cubes(mask)

    # time aggregation if wished
    if ts_interval is not None:
        bands = bands.aggregate_temporal_period(period=ts_interval,  reducer=ts_reducer)

    # Linearly interpolate missing values if wished
    if ts_interpolation:
        bands = bands.apply_dimension(dimension="t",
                                      process="array_interpolate_linear")
    # forcing 16bit --> UInt16
    bands = bands.linear_scale_range(0, 65534, 0, 65534)

    return bands

def extract_planet_datacube( 
        connection: openeo.Connection, bbox: Optional[openEO_bbox_format], start: str, end: str,
        **processing_options: Dict[str, Union[str, bool, int | float, List[str], List[int | float]]]) -> DataCube:
    """ Extract the Planet data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param Planet_collection: (str, optional): Collection name for Planet data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, SLC_masking_algo, planet_bands)
    :return: DataCube
    """
    # evaluate additional processing_options
    target_crs: Optional[str] = processing_options.get("target_crs", None)
    target_res: float = processing_options.get("resolution", 3.)
    p_bands: List[str] = processing_options.get("planet_bands", PLANET_BANDS)
    ts_interval: Optional[str] = processing_options.get("ts_interval", None)
    ts_interpolation: bool = processing_options.get("time_interpolation", False)
    masking: Optional[str] = processing_options.get("UDM_masking_algo", None)

    # check if the masking parameter is valid
    if masking not in ['satio','mask_udm_dilation', None]:
        raise ValueError(f'Unknown masking option `{masking}`')
    
    # we have to check if enough data is available on storage platform
    #TODO current catalogue check used PLANET API, make pystac check
    """
    if catalogue_check:
        catalogue_check_Planet(start, end, bbox) 
    """

    # request the needed datacube
    planetscope_url: Optional[str] = processing_options.get("planet_stac_url", None)
    if not planetscope_url:
        raise ValueError ('No known stac url given for PlanetScope data')

    bands = connection.load_stac(
        url=planetscope_url,
        bands=p_bands,
        spatial_extent=bbox,
        temporal_extent=[start, end],
        properties=None
    )

    # warp and/or resample if needed
    if target_crs is not None:
        bands = bands.resample_spatial(projection=target_crs, resolution=target_res)
    else:
        bands = bands.resample_spatial(resolution=target_res)

    # apply cloud masking
    if masking:
        udm2_url: Optional[str] = processing_options.get("udm_stac_url", None)
        if not udm2_url:
            raise ValueError ('No known stac url given for UDM 2')

    if masking == 'mask_udm_dilation':
        # we have to load the SCL mask as an extra cube to get it correctly working
        sub_collection = connection.load_stac(
            url=udm2_url,
            bands=p_bands,
            spatial_extent=bbox,
            temporal_extent=[start, end],
            properties=None
        )
        # resample to 3m (needed for the correct kernels)
        if target_crs is not None:
            sub_collection = sub_collection.resample_spatial(resolution=3., projection=target_crs)
        else:
            sub_collection = sub_collection.resample_spatial(resolution=3.)

        # reclassify UDM binary bands to classes
        udm_classes=sub_collection.apply_dimension(
            process=classify_udm2,
            dimension="bands"
        ).rename_labels(
            target=["UDM2"],
            dimension="bands"
        )

        # Perform dilation
        udm_dilated_mask = sub_collection.process(
            "to_scl_dilation_mask",
            data=udm_classes,
            scl_band_name="UDM2",
            kernel1_size=17,
            kernel2_size=201,
            mask1_values=[1],
            mask2_values=[2,3,4,5,6,7],
            erosion_kernel_size=3
        ).rename_labels("bands", ["Planet-UDM2_DILATED_MASK"])
        bands = bands.mask(udm_dilated_mask)  # masks are automatically resampled/warped

    elif masking == 'satio':
        # Apply satio-based mask
        mask = udm2_mask_erode_dilate(
            udm2_url,
            connection,
            bbox,
            temporal_extent=[start, end], 
            target_crs=target_crs)
        bands = bands.mask(mask) # masks are automatically resampled/warped

    # time aggregation if wished
    if ts_interval is not None:
        bands = bands.aggregate_temporal_period(period=ts_interval, reducer="median")

    # Linearly interpolate missing values if wished
    if ts_interpolation:
        bands = bands.apply_dimension(dimension="t",
                                      process="array_interpolate_linear")
    # forcing 16bit --> UInt16
    bands = bands.linear_scale_range(0, 65534, 0, 65534)

    return bands
