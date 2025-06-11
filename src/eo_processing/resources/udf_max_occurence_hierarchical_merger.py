import os, sys
import pandas as pd
import numpy as np
import xarray as xr
import re
from typing import Dict, List, Tuple, Union
from openeo.udf import inspect
from datetime import datetime
from openeo.metadata import CubeMetadata

#ToDo Clean and add more comments

def apply_metadata(metadata: CubeMetadata, context:dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    return metadata.rename_labels(dimension="bands", target=['EUNIS habitat level3'])

def _get_metadata(l1_classes, l1_values, l2_classes, l2_values, l3_classes, l3_values) -> Dict:
    attrs = {}
    attrs['eunis habitats L1'] = ""
    for idx, l1 in enumerate(l1_classes):
        attrs['eunis habitats L1'] += l1+':'+str(l1_values[idx])+', '
    attrs['eunis habitats L2'] = ""
    for idx, l2 in enumerate(l2_classes):
        attrs['eunis habitats L2'] += l2+':'+str(l2_values[idx])+','
    attrs['eunis habitats L3'] = ""
    for idx, l3 in enumerate(l3_classes):
        attrs['eunis habitats L3'] += l3+':'+str(l3_values[idx])+','

    return attrs

def _create_output_xarray(output_ar: np.ndarray, input_xr: xr.DataArray) -> xr.DataArray:
    return xr.DataArray(
        output_ar,
        dims=["bands","y","x"],
        coords={"bands":range(1),"y":input_xr.coords["y"], "x":input_xr.coords["x"]},
    )

def _select_highest_prob_class(cube: xr.DataArray, raster_codes) -> xr.DataArray:
    """ Select per model the highest probability of occurrence class
    :param cube: data cube with probabilities for all classes of three levels
    :param raster_codes: dataframe with raster code values
    :return: data cube with highest probably of occurrence class per model (level)
    """
    # Identify the band with the highest probability for each pixel
    cube= cube.fillna(0)  #make sure argmax is not returning all slice N/A
    try:
        max_band = cube.dropna(dim="bands", how='all').argmax(axis=0)  # Index of max value, OpenEO need bands ?
    except Exception as e:
        inspect(message=f"EXCEPTION {e} in argmax for {raster_codes}")

    # Map max_band indices to corresponding raster codes
    selected_raster_code = np.choose(max_band, raster_codes)

    # Return selected highest eunis habitat (raster value) for given level
    return selected_raster_code

def _merge_hierarchical(cube: xr.DataArray, df, df_high_prob) -> xr.DataArray:
    # currently band names are pushed in attributes.
    #if len(df_high_prob) != len(cube.attrs["bands"].split(',')):
    #    inspect(message=f"EXCEPTION mismatch in highest probability cube per group")

    #print('+ get level1 to start with')
    inspect(message=f"+ merge hierarchical Level 1")
    #select the first band from the cube, this is level-1
    aData = cube.isel(bands=[0])  #.to_numpy()[0] not supported in OpenEO

    #print('+ imprint Level2 data into Level1 classes')
    inspect(message=f"+ merge hierarchical Level 2")
    l1_classes = df[df.level == '1']['habitat'].unique().tolist()
    l1_values = df[df.level == '1']['raster_code'].unique().tolist()
    l2_classes = df[df.level == '2']['habitat'].unique().tolist()
    l2_values = df[df.level == '2']['raster_code'].unique().tolist()

    # get list of Level2 files for this tile which can be imprinted
    # use of 'df' --> so based on for which level 2 classes a model was built!
    df_l2 = df_high_prob[(df_high_prob.level == '2')]
    #l2_class_unique = df_l2['sub_class'].unique()
    l2_class_unique = [l2_class for l2_class in l2_classes if l2_class[0] in [l1_class[0] for l1_class in l1_classes]]

    if not df_l2.empty:
        # now we run over each of this Level 2 raster to imprint into Level1
        for row in df_l2.itertuples():
            #print(f'++ retrieve & imprint data for Level 1 {row.model}')
            aImprint = cube.isel(bands=[row.Index])  #.to_numpy()[0] not supported in OpenEO
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
            #aData[aData == lsub[0]] = aImprint[aData == lsub[0]]  #multi-boolean indexing not supported in xarray
            aData = xr.where(aData == lsub[0], aImprint, aData)
            # free
            aImprint = None

    #print('+ imprint Level3 data into Level2 classes')
    inspect(message=f"+ merge hierarchical Level 3")
    l3_classes = df[df.level == '3']['habitat'].unique().tolist()
    l3_values = df[df.level == '3']['raster_code'].unique().tolist()

    # get list of Level3 files for this tile which can be imprinted
    # use of 'df' --> so based on for which level 2 classes a model was built!
    df_l3 = df_high_prob[(df_high_prob.level == '3')]
    #l2_class_unique = df_l2['sub_class'].unique()
    l3_class_unique = [l3_class for l3_class in l3_classes if l3_class[0] in [l2_class[0] for l2_class in l2_classes]]

    if not df_l3.empty:
        # now we run over each of this Level 3 raster to imprint into Level1
        for row in df_l3.itertuples():
            #print(f'++ retrieve & imprint data for Level 3 {row.model}')
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
            #aData[aData == lsub[0]] = aImprint[aData == lsub[0]]
            aData = xr.where(aData == lsub[0], aImprint, aData)
            # free
            aImprint = None

    #to keep the spatial dimensions, we take a copy of cube (first band) and imprint the results
    #eunis_cube = cube.isel(bands=[0])
    #eunis_cube[0] = aData
    eunis_cube = aData

    eunis_cube.attrs = _get_metadata(l1_classes, l1_values, l2_classes, l2_values, l3_classes, l3_values)
    #eunis_cube = xr.set_options() set_nodata(eunis_cube, 0)

    return eunis_cube

def parse_prob_classes_fromStac(band_names):

    band_info = []
    pattern = re.compile(r"Level([\w\d]+)_class-([\w\d]+)_habitat-([\w\d]+)-(\d+)")
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
    # fill nan in cube and make sure the cube is in the right dtype
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

        #inspect(message=f"processing level:group {level}:{class_name} with bands: {band_indices}")
        subset_cube = cube.isel(bands=list(band_indices))
        max_probability = _select_highest_prob_class(subset_cube, raster_codes)
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

    #TODO assign names to bands via data variables iso attributes
    #max_cube = max_cube.assign_attrs(bands=",".join(str(x) for x in band_names))
    # create new dataframe with bands from highest_prob
    df_high_prob = pd.DataFrame({'count':df.groupby(["level","model"]).size()}).reset_index(level=["model","level"])

    ### Merge highest probability classes in hierarchical way
    inspect(message=f"## merge highest probabilities")
    max_cube = _merge_hierarchical(max_cube, df, df_high_prob)
    # make sure the output Xarray has set correct dtype (not float64)
    max_cube = max_cube.astype("uint32")

    return max_cube