import xarray as xr
from typing import Dict, List
from openeo.udf import inspect
from openeo.metadata import CubeMetadata

LEVEL1_PREFIX = "Level1_class-0"

# option -> {typology_schema: level1 band that gets imprinted / masked}
IMPRINT_BANDS = {
    "ocean":    {"EUNIS2021plus": "Level1_class-0_habitat-MH-20000",
                 "IUCNGET": "Level1_class-0_habitat-M-80000"},
    "snow":     {"EUNIS2021plus": "Level1_class-0_habitat-U-90000",
                 "IUCNGET": "Level1_class-0_habitat-T-10000"},
    "mangrove": {"IUCNGET": "Level1_class-0_habitat-MFT-100000"},
    "fm":       {"IUCNGET": "Level1_class-0_habitat-FM-70000"},
}
# option -> context flag (the order defines the order in which missing bands are appended)
FLAGS = {"ocean": "force_ocean",
         "snow": "force_snow",
         "mangrove": "mask_non_mangrove",
         "fm": "mask_fm"}

OCEAN_VALUE = 255
SNOW_VALUE = 110       # value in LCFM 10m maps for permanent snow & ice
MANGROVE_VALUE = 1     # raster value for potential areas of mangrove
FM_VALUE = 1           # raster value for potential areas of FM

MFT_MANGROVE_BAND = "Level3_class-MFT1_habitat-MFT1.2-100102"
MFT_LEVEL3_BANDS = ["Level3_class-MFT1_habitat-MFT1.1-100101",
                    MFT_MANGROVE_BAND,
                    "Level3_class-MFT1_habitat-MFT1.3-100103"]


def _imprint_band(option: str, schema: str) -> str:
    """ level1 band belonging to an option for the given typology schema """
    try:
        return IMPRINT_BANDS[option][schema]
    except KeyError:
        raise ValueError(f"'{FLAGS[option]}' is not supported for typology_schema '{schema}'. "
                         f"Set the parameter to False or adjust the code base of UDF.") from None


def _required_bands(context: Dict) -> List[str]:
    """ level1 bands needed by the enabled options (fixed order: ocean, snow, mangrove, fm) """
    schema = context.get("typology_schema")
    return [_imprint_band(option, schema) for option, flag in FLAGS.items() if context.get(flag)]


def _output_band_names(context: Dict) -> List[str]:
    """ band_names from context (copy, context is never mutated) + all required bands which are missing """
    names = list(context.get("band_names") or [])
    if not names:
        raise ValueError("context['band_names'] must not be empty")
    return names + [band for band in _required_bands(context) if band not in names]


def _ancillary(cube: xr.DataArray, name: str) -> xr.DataArray:
    if name not in cube.indexes["bands"]:
        raise ValueError(f"no {name} band in cube - please add a {name} band to the input cube")
    return cube.sel(bands=name)


def _imprint_level1(cube: xr.DataArray, band: str, mask: xr.DataArray) -> None:
    """ in place: set band to 100 and all other level1 bands to 0 where mask is True """
    others = [b for b in cube.indexes["bands"] if b.startswith(LEVEL1_PREFIX) and b != band]
    inspect(message=f"+ imprint mask into band {band} and reset non-matching level1 bands ({others})")
    cube.loc[dict(bands=band)] = xr.where(mask, 100, cube.loc[dict(bands=band)])
    if others:
        cube.loc[dict(bands=others)] = xr.where(mask, 0, cube.loc[dict(bands=others)])


def _zero_where(cube: xr.DataArray, band: str, mask: xr.DataArray) -> None:
    """ in place: set band to 0 where mask is True """
    cube.loc[dict(bands=band)] = xr.where(mask, 0, cube.loc[dict(bands=band)])


def apply_metadata(metadata: CubeMetadata, context: Dict) -> CubeMetadata:
    """ Rename the bands by using openeo apply_metadata function
    :param metadata: Metadata of the input data cube
    :param context: Context of the UDF
    :return: renamed labels
    """
    return metadata.rename_labels(dimension="bands", target=_output_band_names(context))


def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """ imprint or mask external data into the probability cube

    :param cube: data cube
    :param context: dictionary with band_names, typology_schema and the force_/mask_ switches
    :return: data cube with imprinted / masked values (new cube)
    """
    schema = context.get("typology_schema")
    output_band_names = _output_band_names(context)

    inspect(message=f"settings: typology_schema: {schema}, force_ocean: {context.get('force_ocean')}, "
                    f"force_snow: {context.get('force_snow')}, mask_non_mangrove: {context.get('mask_non_mangrove')}, "
                    f"mask_non_ocean: {context.get('mask_non_ocean')}, mask_fm: {context.get('mask_fm')}, "
                    f"output_band_names ({len(output_band_names)}): {output_band_names}")
    input_band_names = cube.indexes["bands"].values
    inspect(message=f"input cube band names ({len(input_band_names)}): {input_band_names}")

    # add all missing proba bands in one step (zero filled); otherwise copy so the input cube is not edited in place
    missing = [band for band in _required_bands(context) if band not in cube.indexes["bands"]]
    if missing:
        inspect(message=f"+ create extra proba layers: {missing}")
        zero_bands = (xr.zeros_like(cube.isel(bands=0, drop=True))
                      .expand_dims(bands=missing)
                      .transpose(*cube.dims))
        cube = xr.concat([cube, zero_bands], dim="bands")
    else:
        cube = cube.copy()

    if context.get("force_ocean"):
        inspect(message="imprinting ocean")
        band = _imprint_band("ocean", schema)
        ocean_mask = _ancillary(cube, "ocean") == OCEAN_VALUE
        _imprint_level1(cube, band, ocean_mask)

        # mask areas wrongly classified as ocean - only needed for IUCN-GET typology
        # (in EUNIS ocean is only imprinted and never a classification result)
        if context.get("mask_non_ocean") and schema == "IUCNGET":
            inspect(message=f"+ reset areas in the {band} band which are not really ocean "
                            f"(level1 second winner will be used)")
            _zero_where(cube, band, ~ocean_mask)

    if context.get("force_snow"):
        inspect(message="imprinting snow")
        band = _imprint_band("snow", schema)
        _imprint_level1(cube, band, _ancillary(cube, "snow") == SNOW_VALUE)

    if context.get("mask_non_mangrove"):
        inspect(message="masking wrong mangrove areas")
        band = _imprint_band("mangrove", schema)
        mango_mask = _ancillary(cube, "mangrove") == MANGROVE_VALUE

        # Level3 winner in the level1 MFT results: when the winner is mangrove (MFT1.2) with proba > 0 and it lies
        # outside the mangrove potential area, level1 MFT is set to ZERO (assumption: these areas can not be
        # MFT1.1 or MFT1.3). Level2 is a single class model, so no check needed.
        if all(b in cube.indexes["bands"] for b in MFT_LEVEL3_BANDS):
            level3_data = cube.sel(bands=MFT_LEVEL3_BANDS).fillna(0)
            mangrove_winner = level3_data.argmax("bands") == 1
            mangrove_positive = level3_data.sel(bands=MFT_MANGROVE_BAND) > 0
            removal_mask = ~mango_mask & mangrove_winner & mangrove_positive
            n_pixels_to_remove = int(removal_mask.sum().values)
            inspect(message=f"+ reset areas in the {band} band where mangroves were wrongly detected "
                            f"({n_pixels_to_remove} pixel) (level1 second winner will be chosen)")
            _zero_where(cube, band, removal_mask)
        else:
            inspect(message="+ no level3 bands found in cube - cannot mask wrong mangroves areas")

    if context.get("mask_fm"):
        inspect(message="masking wrong FM areas")
        band = _imprint_band("fm", schema)
        removal_mask = _ancillary(cube, "fm_mask") != FM_VALUE
        inspect(message=f"+ mask areas in the {band} band where FM was wrongly detected")
        _zero_where(cube, band, removal_mask)

    # filter the cube to only the bands we need
    missing_out = [b for b in output_band_names if b not in cube.indexes["bands"]]
    if missing_out:
        raise ValueError(f"bands missing in cube, cannot build output cube: {missing_out}")
    cube = cube.sel(bands=output_band_names)
    inspect(message=f"output cube band names {len(cube.indexes['bands'])}: {cube.indexes['bands'].values}")

    return cube
