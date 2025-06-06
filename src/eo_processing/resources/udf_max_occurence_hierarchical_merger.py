import os, sys
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union
from openeo.udf import inspect

#ToDo integrate functions from notebook
def select_highest_prob_class(cube: xr.DataArray, raster_codes) -> xr.DataArray:
    # Identify the band with the highest probability for each pixel
    cube= cube.fillna(0)  #make sure argmax is not returning all slice N/A
    try:
        max_band = cube.dropna(dim="bands", how='all').argmax(axis=0)  # Index of max value
    except Exception as e:
        inspect(message=f"EXCEPTION {e} in argmax for {raster_codes}")

    # Map max_band indices to corresponding raster codes
    selected_raster_code = np.choose(max_band, raster_codes)

    # Return selected highest eunis habitat (raster value) for given level
    return selected_raster_code

def merge_hierarchical(cube: xr.DataArray, bands) -> xr.DataArray:
    return cube

def create_output_xarray(highest_probabilities: np.ndarray, input_xr: xr.DataArray) -> xr.DataArray:
    return xr.dataArray(
        highest_probabilities,
        dims=["bands","y","x"],
        coords={"y":input_xr.coords["y"], "x":input_xr.coords["x"]},
    )

def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    inspect(message=f"xarray dims {cube.dims}")
    # fill nan in cube and make sure the cube is in the right dtype
    max_cube_initialized = False

    ### get the list of classes as output from inference run
    inspect(message=f"## context parameters")
    tileID = context.get("tile")
    inspect(message=f"tile ID: {tileID}")
    df = pd.DataFrame.from_dict(context.get("level_info"))
    inspect(message=f"{df}")

    ### Determine first the highest probability per model (leveled)
    inspect(message=f"## determine highest probability per model/level")

    # read in the selected band names from the raster stack (per level and class)
    for (level, class_name), group in df.groupby(["level", "model"]):

        band_indices = group["band_nr"].values - 1  # Convert to 0-based index
        raster_codes = group["raster_code"].values

        inspect(message=f"processing level:group {level}:{class_name} with bands: {band_indices}")
        subset_cube = cube.isel(bands=list(band_indices))
        max_probability = select_highest_prob_class(subset_cube, raster_codes)
        #max_level_cube = create_output_xarray(max_probability, subset_cube)

        if not max_cube_initialized:
            # Iniitialize the output cube only on the first iteration
            max_cube = max_probability
            band_names = [class_name]
            max_cube_initialized = True
        else:
            # Append the result in the output cube
            max_cube = xr.concat([max_cube, max_probability], dim="bands")
            band_names.append(class_name)

    #max_cube = max_cube.assign_attrs(bands=",".join(str(x) for x in band_names))

    ### Merge highest probability classes in hierarchical way
    inspect(message=f"## merge highest probabilities")
    max_cube = merge_hierarchical(max_cube, df)

    return max_cube







