import os, sys
import pandas as pd
import geopandas as gpd
import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union
from openeo.udf import inspect

#ToDo integrate functions from notebook
def select_highest_prob_class(cube: xr.DataArray, raster_codes) -> np.ndarray:
    # Identify the band with the highest probability for each pixel
    max_band = np.argmax(cube, axis=0)  # Index of max value

    # Map max_band indices to corresponding raster codes
    selected_raster_code = np.choose(max_band, raster_codes)

    # Return selected highest eunis habitat (raster value) for given level
    return selected_raster_code

def merge_hierarchical(df, df_files, pTile, map_year=2024, lut_year=2021) -> np.ndarray:
    return

def create_output_xarray(highest_probabilities: np.ndarray, input_xr: xr.DataArray) -> xr.dataArray:
    return xr.dataArray(
        highest_probabilities,
        dims=["bands","y","x"],
        coords={"y":input_xr.coords["y"], "x":input_xr.coords["x"]},
    )

def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    # fill nan in cube and make sure the cube is in the right dtype
    cube = cube.fillna(0)
    cube = cube.astye("uint16")

    # get the list of classes as output from inference run
    df = context.get("df")

    # Determine first the highest probability per model (leveled)
    inspect(message=f"determine highest probability per model/level")

    # read in the selected band names from the raster stack (per level and class)
    for (level, class_name), group in df.groupby(["level", "model"]):
        inspect(message=f"processing group {class_name}")
        band_indices = group["band_nr"].values - 1  # Convert to 0-based index
        raster_codes = group["raster_code"].values

        subset_cube = cube.sel(bands=list(band_indices + 1))
        max_cube = select_highest_prob_class(subset_cube, raster_codes)  #Todo append bands

        break

    inspect(message=f"highest probabilities identified, converting to xarray ...")
    max_output_cube = create_output_xarray(max_cube)

    pass







