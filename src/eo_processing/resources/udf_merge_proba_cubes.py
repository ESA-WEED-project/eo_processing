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
    band_names = context.get('band_names')
    return metadata.rename_labels(dimension="bands", target=band_names)

def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    """ merge proba cubes

    :param cube: data cube
    :param context: dictionary to provide external data - key class_mapping is used to provide external re-mapping dict
    :return: unique proba data cube
    """
    sorted_band_names = context.get('band_names')
    number_added_cubes = context.get('number_add_cubes')

    if not sorted_band_names:
        raise ValueError("sorted_band_names must not be empty")

    inspect(message=f"merge proba cubes. add {number_added_cubes} cubes to base cube.")
    inspect(message=f"output_band_names ({len(sorted_band_names)}): {sorted_band_names}")

    # first we create a new cube with the correct band names and all filled zeros
    proba_cube = (
        xr.full_like(
            cube.isel(bands=0, drop=True),
            fill_value=np.nan,
            dtype=np.float32,
        )
        .expand_dims(bands=sorted_band_names)
        .transpose(*cube.dims)
        .copy()
    )

    # creating the list of band names of the cubes we need to split
    input_band_names = cube.indexes["bands"].values
    inspect(message=f"input cube band names ({len(input_band_names)}): {input_band_names}")

    # loop over the input bands.... inspect to which corresponding band it belongs and imprint in proba_cube
    suffix_pattern = re.compile(r"_cube\d+$")
    sorted_band_names_set = set(sorted_band_names)

    for cube_band_name in input_band_names:
        # strip trailing "_cube{n}" suffix if present, then match exactly
        match_band_name = suffix_pattern.sub("", cube_band_name)

        if match_band_name in sorted_band_names_set:
            cube_values = cube.loc[dict(bands=cube_band_name)]
            mask = np.logical_and(cube_values != 255, ~np.isnan(cube_values))
            proba_cube.loc[dict(bands=match_band_name)] = xr.where(
                mask, cube_values, proba_cube.loc[dict(bands=match_band_name)]
            )
        else:
            inspect(message=f"no corresponding output band name found for input band name {cube_band_name}")

    # check
    unfilled_mask = proba_cube.isnull().all(dim=[d for d in proba_cube.dims if d != "bands"])
    unfilled_bands = proba_cube["bands"].where(unfilled_mask, drop=True).values.tolist()
    if unfilled_bands:
        inspect(message=f"WARNING: {len(unfilled_bands)} output band(s) never filled: {unfilled_bands}")

    # transfer back to Byte data type
    #proba_cube = proba_cube.fillna(255) # the linear scale after the UDF brings 255 back to openEO nan
    #proba_cube = proba_cube.astype("uint8")

    return proba_cube