import os, sys
import pandas as pd
import numpy as np
import xarray as xr
import re
from typing import Dict, List, Tuple, Union
from openeo.udf import inspect
from datetime import datetime
from openeo.metadata import CubeMetadata

def apply_metadata(metadata: CubeMetadata, context:Dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    return metadata.rename_labels(dimension="bands", target=['EUNIS habitat level3'])

def _select_highest_prob_class(cube: xr.DataArray, raster_codes) -> xr.DataArray:
    """ Select per model the highest probability of occurrence class
    :param cube: data cube with probabilities for all classes of three levels
    :param raster_codes: dataframe with raster code values
    :return: data cube with highest probably of occurrence class per model (level)
    """
    # Create nodata mask before filling with 0
    nodata_mask = cube.isnull().all(dim="bands")

    # Identify the band with the highest probability for each pixel
    cube= cube.fillna(0)  #make sure argmax is not returning all slice N/A
    try:
        max_band = cube.dropna(dim="bands", how='all').argmax(axis=0)  # Index of max value, OpenEO need bands ?
    except Exception as e:
        inspect(message=f"EXCEPTION {e} in argmax for {raster_codes}")

    # Map max_band indices to corresponding raster codes
    selected_raster_code = np.choose(max_band, raster_codes)

    # Apply nodata mask to the result using the original nodata value
    selected_raster_code = xr.where(nodata_mask, np.nan, selected_raster_code)

    # Return selected highest eunis habitat (raster value) for given level
    return selected_raster_code

def _merge_hierarchical(cube: xr.DataArray, df_high_prob) -> xr.DataArray:
    """
    Merge hierarchical data levels into a base data layer by imprinting Level 2 and Level 3 data layers sequentially.

    The function processes a hierarchical data structure represented by a cube and a DataFrame.
    It first handles Level 2 data, imprinting it into the base layer (Level 1), and then processes Level
    3 data, imprinting it into its respective Level 2 classes. This allows higher-level classifications
    to influence lower levels in a structured manner.

    :param cube: Input data cube as an xarray.DataArray. Contains multiple bands representing hierarchical levels.
    :param df_high_prob: DataFrame containing metadata for Level 2 and Level 3 data layers to be processed.
    :return: The modified base layer (Level 1), represented as an xarray.DataArray, after hierarchical imprinting.
    """
    inspect(message=f"+ merge hierarchical Level 1")
    #select the first band from the cube, this is level-1
    aData = cube.isel(bands=[0])

    inspect(message=f"+ merge hierarchical Level 2")
    # get list of Level2 files for this tile which can be imprinted
    df_l2 = df_high_prob[(df_high_prob.level == '2')]

    if not df_l2.empty:
        # now we run over each of this Level 2 raster to imprint into Level1
        for row in df_l2.itertuples():
            aImprint = cube.isel(bands=[row.Index])
            nodata = [0 , -1]

            # get the Level 1 habitat code from the level 2 data
            lsub = np.unique(aImprint).tolist()
            lsub = [x for x in lsub if x not in nodata]
            if nodata in lsub: lsub.remove(nodata)
            if np.nan in lsub: lsub.remove(np.nan)
            lsub = [*set([int(np.floor(x / 10000) * 10000) for x in lsub])]

            if len(lsub) != 1:
                raise RuntimeError(
                    f'level2 sub-class results should only belong to ONE level 1 class. level 2 results of file {row.path} belong to {len(lsub)} level 1 classes ({lsub}).')
            # if we have this error then check if the classified tile is containing data, probably there is 'nodata' involved in the issue.

            # now imprint the data into level 1
            aData = xr.where(aData == lsub[0], aImprint, aData)
            # free
            aImprint = None

    inspect(message=f"+ merge hierarchical Level 3")
    # get list of Level3 files for this tile which can be imprinted
    df_l3 = df_high_prob[(df_high_prob.level == '3')]

    if not df_l3.empty:
        # now we run over each of this Level 3 raster to imprint into Level2
        for row in df_l3.itertuples():
            aImprint = cube.isel(bands=[row.Index])
            nodata = [0 , -1]

            # get the Level 2 habitat code from the level 3 data
            lsub = np.unique(aImprint).tolist()
            lsub = [x for x in lsub if x not in nodata]
            if nodata in lsub: lsub.remove(nodata)
            if np.nan in lsub: lsub.remove(np.nan)
            lsub = [*set([int(np.floor(x / 100) * 100) for x in lsub])]

            if len(lsub) != 1:
                raise RuntimeError(
                    f'level3 sub-class results should only belong to ONE level 2 class. level 3 results of file {row.model} belong to {len(lsub)} level 2 classes ({lsub}).')
            # if we have this error then check if the classified tile is containing data, probably there is 'nodata' involved in the issue.

            # now imprint the data into level 2
            aData = xr.where(aData == lsub[0], aImprint, aData)
            # free
            aImprint = None

    return aData

def parse_prob_classes_fromStac(band_names: List[str]) -> pd.DataFrame:
    """
    Parses probability class data from STAC band names and returns it as a pandas DataFrame.

    This function processes a list of band names, extracting information encoded in the band names
    in a specific format. The encoded information includes attributes like the level, class name, habitat,
    and raster code. If a band name does not match the expected pattern, it is skipped. The extracted
    information is returned as a pandas DataFrame with specific columns representing each attribute.

    :param band_names: List of band names to parse. Each name should follow a specific pattern.
    :return: A pandas DataFrame containing the parsed band information with the following columns:
        - band_nr: Band number in the list (1-indexed).
        - level: Level information extracted from the band name.
        - model: Class name or model extracted from the band name.
        - habitat: Habitat information extracted from the band name.
        - raster_code: Raster code extracted from the band name as an integer.
    """
    band_info = []
    pattern = re.compile(r"Level([\w\d]+)_class-([\w\d]+)_habitat-([\w\D\d]+)-(\d+)")
    for band_nr, band_name in enumerate(band_names, start=1):
        match = pattern.search(band_name.replace(" ", ""))  # make sure no white spaces pending
        if match:
            level, class_name, habitat, raster_code = match.groups()
            band_info.append((band_nr, level, class_name, habitat, int(raster_code)))
        else:
            print('skipping {}'.format(band_name))
    # Create DataFrame
    df = pd.DataFrame(band_info, columns=["band_nr", "level", "model", "habitat", "raster_code"])

    return df

def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    inspect(message=f"xarray dims {cube.dims}")
    # important for filling by level
    max_cube_initialized = False

    ### get the list of classes as output from inference run
    # use returned metadata to build up the class dictionary
    inspect(message=cube.indexes["bands"].values)
    df = parse_prob_classes_fromStac(cube.indexes["bands"].values)

    inspect(message=f"## context parameters")
    inspect(message=f"{df}")

    ### Determine first the highest probability per model (leveled)
    inspect(message=f"## determine highest probability per model/level")

    # read in the selected band names from the raster stack (per level and class)
    for (level, class_name), group in df.groupby(["level", "model"]):

        band_indices = group["band_nr"].values - 1  # Convert to 0-based index
        raster_codes = group["raster_code"].values
        # select the bands for this class
        subset_cube = cube.isel(bands=list(band_indices))
        # get a 2D array with the winning prob of the habitat classes
        max_probability = _select_highest_prob_class(subset_cube, raster_codes)

        if not max_cube_initialized:
            # Iniitialize the output cube only on the first iteration
            max_cube = max_probability
            max_cube_initialized = True
        else:
            # Append the result in the output cube
            max_cube = xr.concat([max_cube, max_probability], dim="bands")

    # check if xaaray has band dimension - if only level1 is processed that can happen
    if not "bands" in max_cube.dims:
        max_cube = max_cube.expand_dims(dim={"bands": [0]})

    # create new dataframe with bands from highest_prob as LUT
    df_high_prob = pd.DataFrame({'count':df.groupby(["level","model"]).size()}).reset_index(level=["model","level"])

    ### Merge highest probability classes in hierarchical way
    inspect(message=f"## merge highest probabilities")
    max_cube = _merge_hierarchical(max_cube, df_high_prob)
    # make sure the output Xarray has set correct dtype (not float64)
    max_cube = max_cube.astype("uint32")

    return max_cube