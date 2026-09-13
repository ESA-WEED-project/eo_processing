import xarray as xr
from typing import Dict
from openeo.udf import inspect
from openeo.metadata import CubeMetadata

def apply_metadata(metadata: CubeMetadata, context:Dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    band_names = context.get("band_names")
    typology_schema = context.get("typology_schema")
    force_ocean = context.get("force_ocean")
    force_snow = context.get("force_snow")
    mask_non_mangrove = context.get("mask_non_mangrove")

    if force_ocean:
        if typology_schema == "EUNIS2021plus":
            needed = "Level1_class-0_habitat-MH-20000"
        elif typology_schema == "IUCNGET":
            needed = "Level1_class-0_habitat-M-80000"
        else:
            raise Exception("unknown typology schema - please adjust the code base of UDF")
        if needed not in band_names:
            band_names.append(needed)

    if force_snow:
        if typology_schema == "EUNIS2021plus":
            needed = "Level1_class-0_habitat-U-90000"
        elif typology_schema == "IUCNGET":
            needed = "Level1_class-0_habitat-T-10000"
        else:
            raise Exception("unknown typology schema - please adjust the code base of UDF")
        if needed not in band_names:
            band_names.append(needed)

    if mask_non_mangrove:
        if typology_schema == "IUCNGET":
            needed = "Level1_class-0_habitat-MFT-100000"
        else:
            raise Exception("currently mangrove masking is only possible in IUCNGET. set parameter to False for "
                            "EUNIS or adjust the code base of UDF for new typology.")
        if needed not in band_names:
            band_names.append(needed)

    return metadata.rename_labels(dimension="bands", target=band_names)

def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """ imprint or mask external data into the probability cube

    :param cube: data cube
    :param context: dictionary to provide external data - key class_mapping is used to provide external re-mapping dict
    :return: data cube with remapped values (new cube)
    """

    # get all needed data together
    output_band_names = context.get("band_names")
    typology_schema = context.get("typology_schema")
    force_ocean = context.get("force_ocean")
    force_snow = context.get("force_snow")
    mask_non_mangrove = context.get("mask_non_mangrove")
    mask_non_ocean = context.get("mask_non_ocean")
    inspect(message=f"settings: typology_schema: {typology_schema}, force_ocean: {force_ocean}, force_snow: "
                    f"{force_snow}, mask_non_mangrove: {mask_non_mangrove}, mask_non_ocean: {mask_non_ocean},"
                    f"output_band_names ({len(output_band_names)}): {output_band_names}")

    # get the band names of cube handed over to UDF
    input_band_names = cube.indexes["bands"].values
    inspect(message=f"input cube band names ({len(input_band_names)}): {input_band_names}")

    # check that we have to imprint "ocean"
    if force_ocean:
        inspect(message="imprinting ocean")
        # check that we have the needed band
        if "ocean" not in input_band_names:
            raise ValueError("no ocean band in cube - please add a ocean band to the input cube")

        cube_ocean = cube.sel(bands="ocean")
        ocean_value = 255

        if typology_schema == "EUNIS2021plus":
            imprint_band = "Level1_class-0_habitat-MH-20000"
        elif typology_schema == "IUCNGET":
            imprint_band = "Level1_class-0_habitat-M-80000"
        else:
            raise Exception("unknown typology schema - please adjust the code base of UDF")

        # check that we have the needed output band, if not create
        if imprint_band not in input_band_names:
            inspect(message=f"+ create extra proba layer for ocean ({imprint_band})")
            output_band_names.append(imprint_band)
            # Create zero-filled band with same spatial dimensions as cube
            zero_band = xr.zeros_like(cube.isel(bands=0))
            zero_band = zero_band.assign_coords(bands=imprint_band)
            # Add the new band to the cube
            cube = xr.concat([cube, zero_band], dim="bands")

        # now we imprint the ocean in band "Level1_class-0_habitat-M-80000"
        ocean_mask = cube_ocean == ocean_value
        inspect(message=f"+ imprint ocean mask into band {imprint_band}")
        cube.loc[dict(bands=imprint_band)] = xr.where(ocean_mask, 100, cube.loc[dict(bands=imprint_band)])

        # reset the values of all other bands on level 1 to ZERO
        other_bands_level1 = [x for x in input_band_names if x.startswith("Level1_class-0")]
        if imprint_band in other_bands_level1:
            other_bands_level1.remove(imprint_band)
        inspect(message=f"+ imprint ocean mask into non-marine bands ({other_bands_level1})")
        cube.loc[dict(bands=other_bands_level1)] = xr.where(ocean_mask, 0, cube.loc[dict(bands=other_bands_level1)])

        # now mask areas wrongly classified as ocean (e.g. in the case of the ocean mask)
        # NOTE: currently only needed for IUCN-GET typology
        if mask_non_ocean and (typology_schema == "IUCNGET"):
            inspect(message=f"+ reset areas in the {imprint_band} band which are not really ocean. "
                            f"(level1 second winner will be used)")
            # get the mask of the non-ocean areas
            non_ocean_mask = ~ocean_mask
            # now mask the non-ocean areas
            cube.loc[dict(bands=imprint_band)] = xr.where(non_ocean_mask, 0, cube.loc[dict(bands=imprint_band)])

    # check that we have to imprint "snow"
    if force_snow:
        inspect(message="imprinting snow")
        # check that we have the needed band
        if "snow" not in input_band_names:
            raise ValueError("no snow band in cube - please add a snow band to the input cube")

        snow_cube = cube.sel(bands="snow")
        snow_value = 110  # value in LCFM 10m maps for permanent snow & ice

        if typology_schema == "EUNIS2021plus":
            imprint_band = "Level1_class-0_habitat-U-90000"
        elif typology_schema == "IUCNGET":
            imprint_band = "Level1_class-0_habitat-T-10000"
        else:
            raise Exception("unknown typology schema - please adjust the code base of UDF")

        # check that we have the needed output band, if not create
        if imprint_band not in input_band_names:
            inspect(message=f"+ create extra proba layer for snow ({imprint_band})")
            output_band_names.append(imprint_band)
            # Create zero-filled band with same spatial dimensions as cube
            zero_band = xr.zeros_like(cube.isel(bands=0))
            zero_band = zero_band.assign_coords(bands=imprint_band)
            # Add the new band to the cube
            cube = xr.concat([cube, zero_band], dim="bands")

        # create mask and imprint snow into band
        snow_mask = snow_cube == snow_value
        inspect(message=f"+ imprint snow mask into band {imprint_band}")
        cube.loc[dict(bands=imprint_band)] = xr.where(snow_mask, 100, cube.loc[dict(bands=imprint_band)])

        # reset the values of all other bands on level 1 to ZERO
        other_bands_level1 = [x for x in input_band_names if x.startswith("Level1_class-0")]
        if imprint_band in other_bands_level1:
            other_bands_level1.remove(imprint_band)
        inspect(message=f"+ imprint snow mask into non-snow bands ({other_bands_level1})")
        cube.loc[dict(bands=other_bands_level1)] = xr.where(snow_mask, 0, cube.loc[dict(bands=other_bands_level1)])

    # check that we have to mask mangrove
    if mask_non_mangrove:
        inspect(message="imprinting mangroves")
        # check that we have the needed band
        if "mangrove" not in input_band_names:
            raise ValueError("no mangrove band in cube - please add a mangrove band to the input cube")

        mango_cube = cube.sel(bands="mangrove")
        mango_value = 1   # raster value for potential areas of mangrove

        if typology_schema == "IUCNGET":
            imprint_band = "Level1_class-0_habitat-MFT-100000"
        else:
            raise Exception("currently mangrove masking is only possible in IUCNGET. set parameter to False for "
                            "EUNIS or adjust the code base of UDF for new typology.")

        # check that we have the needed output band, if not create
        if imprint_band not in input_band_names:
            inspect(message=f"+ create extra proba layer for mangrove ({imprint_band})")
            output_band_names.append(imprint_band)
            # Create zero-filled band with same spatial dimensions as cube
            zero_band = xr.zeros_like(cube.isel(bands=0))
            zero_band = zero_band.assign_coords(bands=imprint_band)
            # Add the new band to the cube
            cube = xr.concat([cube, zero_band], dim="bands")

        # We have to first determine the level3 winner in the level1 MFT results. When the winner is mangrove (MFT1.2) and
        # outside the mangrove potential area mask THEN we can set the level1 pixel to ZERO. (Again under the assumption that
        # these areas can not be MFT1.1 or MFT1.3)
        # since level2 is a single class model we do not have to check it

        level3_bands = ['Level3_class-MFT1_habitat-MFT1.1-100101',
                        'Level3_class-MFT1_habitat-MFT1.2-100102',
                        'Level3_class-MFT1_habitat-MFT1.3-100103']

        if all(band in input_band_names for band in level3_bands):
            # get the level3 winner
            level3_data = cube.sel(bands=level3_bands).copy()
            level3_data = level3_data.fillna(0)
            level3_winner = level3_data.sel(bands=level3_bands).argmax('bands')
            # get mask of MFT1.2 winner
            level3_mangrove_winner = level3_winner == 1
            # check if value is bigger than 0
            level3_mangrove_winner_value = level3_data.sel(bands='Level3_class-MFT1_habitat-MFT1.2-100102') > 0

            # create mask where mangrove can exist
            mango_mask = mango_cube == mango_value

            # create the removal mask -> MFT1.2 outside the mangrove potential area mask
            removal_mask = ~mango_mask & level3_mangrove_winner & level3_mangrove_winner_value

            n_pixels_to_remove = int(removal_mask.sum().values)

            # reset the level1 MFT values to ZERO in these areas
            inspect(
                message=f"+ reset areas in the {imprint_band} band were wrongly mangroves were detected ({n_pixels_to_remove} pixel)"
                        f"(level1 second winner will be chosen)")
            cube.loc[dict(bands=imprint_band)] = xr.where(removal_mask, 0, cube.loc[dict(bands=imprint_band)])
        else:
            inspect(message=f"+ no level3 bands found in cube - cannot mask mangroves")

    # filter the cube to only the bands we need
    cube = cube.sel(bands=output_band_names)
    inspect(message=f"output cube band names {len(cube.indexes['bands'].values)}: {cube.indexes['bands'].values}")
    # final check that we have the right number of bands
    if len(cube.indexes['bands'].values) != len(output_band_names):
        raise ValueError("wrong number of bands in output cube - please adjust the code base of UDF")

    return cube