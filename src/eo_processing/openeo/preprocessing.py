from openeo.processes import array_create, if_, is_nodata, power
from openeo.rest.datacube import DataCube

from eo_processing.openeo.masking import scl_mask_erode_dilate
from eo_processing.utils.catalogue_check import (catalogue_check_S1, catalogue_check_S2)


S2_BANDS = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]

def ts_datacube_extraction(
        connection, bbox, start: str, end: str,
        S2_collection='SENTINEL2_L2A',
        S1_collection='SENTINEL1_GRD',
        DEM_collection='COPERNICUS_30',
        METEO_collection='AGERA5',
        **processing_options) -> DataCube:
    """ Warper to extract a full data cube of preprocessed data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S2_collection: (str, optional): Collection name for S2 data
    :param S1_collection: (str, optional): Collection name for S1 data
    :param DEM_collection: (str, optional): Collection name for DEM data
    :param METEO_collection: (str, optional): Collection name for metrological data
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

    # add AGERA5 Meteo data
    if METEO_collection is not None:
        bands = bands.merge_cubes(extract_METEO_datacube(connection, bbox, start, end,
                                                         METEO_collection=METEO_collection, **processing_options))

    # add DEM data
    if DEM_collection is not None:
        bands = bands.merge_cubes(extract_DEM_data(connection, bbox,
                                                   DEM_collection=DEM_collection, **processing_options))

    # final forcing 16bit - UInt16 again - maybe not needed anymore
    bands = bands.linear_scale_range(0, 65534, 0, 65534)

    return bands


def extract_DEM_data(connection, bbox, DEM_collection='COPERNICUS_30', rescale=True, **processing_options) -> DataCube:
    """ extract the DEM data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param DEM_collection: (str, optional): Collection name for DEM data
    :param rescale: (default=True), if the DEM is converted from float to UInt16 and the negative values are clamped
    :param processing_options: (dict, optional), processing options for preprocessing routine (target_crs,
            resolution)
    :return: DataCube
    """
    # evaluate additional processing options
    target_crs = processing_options.get("target_crs", None)
    target_res = processing_options.get("resolution", 10.)

    #get data
    dem = connection.load_collection(
        DEM_collection,
        spatial_extent=bbox,
    )

    # Resample to the S2 spatial resolution
    # TODO: check interpolation method
    # TODO: check no-data near edges of cube
    if target_crs is not None:
        dem = dem.resample_spatial(projection=target_crs, resolution=target_res,
                                   method='cubic')

    # collection has timestamps which we need to get rid of
    dem = dem.max_time()

    # forcing 16bit - UInt16 again --> note that this scaling is removing the negative heigths values and set to 0
    if rescale:
        dem = dem.linear_scale_range(0, 65534, 0, 65534)

    return dem


def extract_METEO_datacube(connection, bbox, start: str, end: str,
                           METEO_collection='AGERA5', rescale=True, **processing_options) -> DataCube:
    """ extract the METEO data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param METEO_collection: (str, optional): Collection name for metrological data
    :param rescale: (default=True), if rescale to UInt16 should be done
    :param processing_options: (dict, optional), processing options for preprocessing routine (target_crs,
            resolution, ts_interval, time_interpolation)
    :return: DataCube
    """
    # evaluate additional processing options
    target_crs = processing_options.get("target_crs", None)
    target_res = processing_options.get("resolution", 10.)
    ts_interval = processing_options.get("ts_interval", None)
    ts_interpolation = processing_options.get("time_interpolation", False)

    # get data
    # TODO: add the precipitation (precipitation-flux band) --> need extra handling since that is in mm/day/pixel
    meteo = connection.load_collection(
        METEO_collection,
        spatial_extent=bbox,
        bands=['temperature-mean'],
        temporal_extent=[start, end]
    )

    # warp and/or resample if needed
    if target_crs is not None:
        meteo = meteo.resample_spatial(projection=target_crs, resolution=target_res)

    # Composite if wished
    if ts_interval is not None:
        meteo = meteo.aggregate_temporal_period(period=ts_interval, reducer="mean")

    # Linearly interpolate missing values.
    # Shouldn't exist in this dataset but is good practice to do so
    if ts_interpolation:
        meteo = meteo.apply_dimension(dimension="t", process="array_interpolate_linear")

    # Rename band to match Radix model requirements
    meteo = meteo.rename_labels('bands', ['temperature_mean'])

    # forcing 16bit - UInt16 again
    if rescale:
        meteo = meteo.linear_scale_range(0, 65534, 0, 65534)

    return meteo


def extract_S1_datacube(connection, bbox, start: str, end: str,
                        S1_collection='SENTINEL1_GRD', **processing_options) -> DataCube:
    """ extract the Sentinel-1 data for requested time period and preprocess the data

    :param connection: active openEO connection object
    :param bbox: dict, bounding box of format {'east': x, 'south': x, 'west': x, 'north': x, 'crs': x}
    :param start: str, Start date for requested input data (yyyy-mm-dd)
    :param end: str, End date for requested input data (yyyy-mm-dd)
    :param S1_collection: (str, optional): Collection name for S1 data
    :param processing_options: (dict, optional), processing options for preprocessing routine (provider, target_crs,
            resolution, ts_interval, time_interpolation, s1_orbitdirection)
    :return: DataCube
    """
    # evaluate additional processing options
    isCreo = "creo" in processing_options.get("provider", "").lower()
    orbit_direction = processing_options.get('s1_orbitdirection', None)
    target_crs = processing_options.get("target_crs", None)
    target_res = processing_options.get("resolution", 10.)
    ts_interval = processing_options.get("ts_interval", None)
    ts_interpolation = processing_options.get("time_interpolation", False)
    if ("creo" in processing_options.get("provider", "").lower()) or \
            (processing_options.get("provider", "").lower() == "terrascope") or \
            (processing_options.get("provider", "").lower() == "development") or \
            (processing_options.get("provider", "").lower() == "cdse"):
        flag_DEM = True
    else:
        flag_DEM = False

    # we have to check if enough data is available on creo platform
    if isCreo:
        orbit_direction = catalogue_check_S1(orbit_direction, start, end, bbox)

    # convert the orbit direction parameter into openEO property
    if orbit_direction is not None:
        properties = {"sat:orbit_state": lambda orbdir: orbdir == orbit_direction}  # NOQA
    else:
        properties = {}

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

    # Composite if wished
    if ts_interval is not None:
        bands = bands.aggregate_temporal_period(period=ts_interval, reducer="mean")

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


def extract_S2_datacube(connection, bbox, start: str, end: str,
                        S2_collection='SENTINEL2_L2A', **processing_options) -> DataCube:
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
    isCreo = "creo" in processing_options.get("provider", "").lower()
    target_crs = processing_options.get("target_crs", None)
    target_res = processing_options.get("resolution", 10.)
    S2_bands = processing_options.get("S2_bands", S2_BANDS)
    ts_interval = processing_options.get("ts_interval", None)
    ts_interpolation = processing_options.get("time_interpolation", False)
    masking = processing_options.get("SLC_masking_algo", None)

    # check if the masking parameter is valid
    if masking not in ['satio', 'mask_scl_dilation', None]:
        raise ValueError(f'Unknown masking option `{masking}`')

    # we have to check if enough data is available on creo platform
    if isCreo:
        # S2URL creo only accepts request in EPSG:4326
        catalogue_check_S2(start, end, bbox)

    # add the SCL band if needed
    if masking in ['mask_scl_dilation']:
        # Need SCL band to mask
        S2_bands.append("SCL")

    # request the needed datacube
    bands = connection.load_collection(
        S2_collection,
        bands=S2_bands,
        spatial_extent=bbox,
        temporal_extent=[start, end],
        max_cloud_cover=95
    )

    # warp and/or resample if needed
    if target_crs is not None:
        bands = bands.resample_spatial(projection=target_crs, resolution=target_res)

    # NOTE: For now we mask again snow/ice because clouds
    # are sometimes marked as SCL value 11!
    if masking == 'mask_scl_dilation':
        # TODO: double check cloud masking parameters
        # https://github.com/Open-EO/openeo-geotrellis-extensions/blob/develop/geotrellis-common/src/main/scala/org/openeo/geotrelliscommon/CloudFilterStrategy.scala#L54  # NOQA
        bands = bands.process(
            "mask_scl_dilation",
            data=bands,
            scl_band_name="SCL",
            kernel1_size=17, kernel2_size=77,
            mask1_values=[2, 4, 5, 6, 7],
            mask2_values=[3, 8, 9, 10, 11],
            erosion_kernel_size=3).filter_bands(
            bands.metadata.band_names[:-1])
    elif masking == 'satio':
        # Apply satio-based mask
        mask = scl_mask_erode_dilate(
            connection,
            bbox,
            scl_layer_band=S2_collection + ':SCL',
            target_crs=target_crs).resample_cube_spatial(bands)
        bands = bands.mask(mask)

    # Composite if wished
    if ts_interval is not None:
        bands = bands.aggregate_temporal_period(period=ts_interval,  reducer="median")

    # Linearly interpolate missing values if wished
    if ts_interpolation:
        bands = bands.apply_dimension(dimension="t",
                                      process="array_interpolate_linear")
    # forcing 16bit --> UInt16
    bands = bands.linear_scale_range(0, 65534, 0, 65534)

    return bands