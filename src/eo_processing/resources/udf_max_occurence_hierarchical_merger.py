import pandas as pd
import numpy as np
import xarray as xr
import re
from typing import Dict, List
from openeo.udf import inspect
from openeo.metadata import CubeMetadata

def apply_metadata(metadata: CubeMetadata, context:Dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    band_name = context.get('typology', 'EUNIS')
    final_band_name = f"{band_name} habitat level3"

    return metadata.rename_labels(dimension="bands", target=[final_band_name])

def _select_highest_prob_class(cube: xr.DataArray, raster_codes) -> xr.DataArray:
    """ Select per model the highest probability of occurrence class
    :param cube: data cube with probabilities for all classes per model (level)
    :param raster_codes: dataframe with raster code values
    :return: data cube with raster value of highest occurrence probabilities per pixel for each model
    """
    # Create nodata mask before filling with 0
    nodata_mask = cube.isnull().all(dim="bands")

    # check
    if cube.sizes["bands"] != len(raster_codes):
        raise ValueError(
            f"Expected one raster code per band, got "
            f"{cube.sizes['bands']} bands and {len(raster_codes)} codes."
        )

    # we make sure we have no value 255 since that is officially the nodata value of PROBA results
    # so set 255 to nan
    cube = cube.where(cube != 255, np.nan)

    # make sure argmax is not returning all slice N/A
    cube= cube.fillna(0)

    # All-zero pixels have no valid class; argmax would incorrectly select band 0.
    nodata_mask = nodata_mask | (cube == 0).all(dim="bands")

    try:
        # Identify the band with the highest probability for each pixel
        max_band = cube.argmax(axis=cube.get_axis_num("bands"))  # Index of max value, OpenEO need bands ?
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
        # now we run over each Level 2 model cube (index in dataframe) to imprint into Level1
        for row in df_l2.itertuples():
            # since the Index of the dataframe represents the band number in the cube
            aImprint = cube.isel(bands=[row.Index])
            nodata = [0 , -1, 255]

            # get the Level 1 habitat code from the level 2 data
            lsub = [x for x in np.unique(aImprint).tolist() if x not in nodata and not np.isnan(x)]
            if len(lsub) == 0:
                inspect(message=f"no data in level 2 for model {row.model}")
                continue
            # for valid members we can generate the level 1 class by flooring the level 2 class to the nearest 10000
            lsub = [*set([int(np.floor(x / 10000) * 10000) for x in lsub])]

            if len(lsub) != 1:
                raise RuntimeError(
                    f'level2 sub-class results should only belong to ONE level 1 class. '
                    f'level 2 results of model {row.model} belong to {len(lsub)} level 1 classes ({lsub}).')
            # if we have this error then check if the classified tile is containing data, probably there is 'nodata' involved in the issue.

            # now imprint the data into level 1
            aData = xr.where(aData == lsub[0], aImprint, aData)
            # free
            aImprint = None

    inspect(message=f"+ merge hierarchical Level 3")
    # get list of Level3 files for this tile which can be imprinted
    df_l3 = df_high_prob[(df_high_prob.level == '3')]

    if not df_l3.empty:
        # now we run over each Level 3 model cube (index in dataframe) to imprint into Level2
        for row in df_l3.itertuples():
            aImprint = cube.isel(bands=[row.Index])
            nodata = [0 , -1, 255]

            # get the Level 2 habitat code from the level 3 data
            lsub = [x for x in np.unique(aImprint).tolist() if x not in nodata and not np.isnan(x)]
            if len(lsub) == 0:
                inspect(message=f"no data in level 3 for model {row.model}")
                continue
            lsub = [*set([int(np.floor(x / 100) * 100) for x in lsub])]

            if len(lsub) != 1:
                raise RuntimeError(
                    f'level3 sub-class results should only belong to ONE level 2 class. '
                    f'level 3 results of model {row.model} belong to {len(lsub)} level 2 classes ({lsub}).')
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
            inspect(message='skipping parsing of band names for band {}. Band name has wrong format.'.format(band_name))
    # Create DataFrame
    df = pd.DataFrame(band_info, columns=["band_nr", "level", "model", "habitat", "raster_code"])

    return df

def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    inspect(message=f"xarray dims {cube.dims}")
    ### get the list of classes as output from inference run
    # use returned metadata to build up the class dictionary
    input_band_names = cube.indexes["bands"].values
    inspect(message=f"input cube band names ({len(input_band_names)}): {input_band_names}")
    df = parse_prob_classes_fromStac(input_band_names)

    inspect(message=f"## parsed band names into dataframe")
    inspect(message=f"{df}")

    ### Determine first the highest probability per model (leveled)
    inspect(message=f"## determine highest probability per model/level")

    # read in the selected band names from the raster stack (per level and class)
    max_cube_initialized = False
    high_prob_records = []

    for (level, class_name), group in df.groupby(["level", "model"]):
        # get band_indices and raster_codes for this model
        band_indices = group["band_nr"].values - 1  # Convert to 0-based index
        raster_codes = group["raster_code"].values

        # select the bands for this model (its classes)
        subset_cube = cube.isel(bands=list(band_indices))
        # get a 2D array with the winning prob of the habitat classes
        max_probability = _select_highest_prob_class(subset_cube, raster_codes)

        # Do not add groups that have no valid result anywhere.
        if bool(max_probability.isnull().all().item()):
            inspect(
                message=(
                    f"Skipping level {level}, model {class_name}: "
                    "result contains only nodata."
                )
            )
            continue

        if not max_cube_initialized:
            # Iniitialize the output cube only on the first iteration which is mainly typology level 1
            max_cube = max_probability
            max_cube_initialized = True
        else:
            # Append the result in the output cube
            max_cube = xr.concat([max_cube, max_probability], dim="bands")

        # Append metadata only when the corresponding cube was appended.
        high_prob_records.append(
            {"level": level, "model": class_name, "count": len(group), }
        )

    if not max_cube_initialized:
        raise RuntimeError(
            "No valid model results found: all groups contain only nodata."
        )

    # check if xaaray has band dimension - if only level1 is processed that can happen
    if "bands" not in max_cube.dims:
        max_cube = max_cube.expand_dims(dim={"bands": [0]})

    # create new dataframe with bands from highest_prob as LUT
    df_high_prob = pd.DataFrame(high_prob_records, columns=["level", "model", "count"])
    inspect(message=f"## dataframe with highest probabilities")
    inspect(message=f"{df_high_prob}")

    if df_high_prob.iloc[0]["level"] != "1":
        raise RuntimeError(
            "Cannot merge hierarchy because no valid Level 1 result is available."
        )

    ### Merge highest probability classes in hierarchical way
    inspect(message=f"## merge highest probabilities")
    max_cube = _merge_hierarchical(max_cube, df_high_prob)

    return max_cube