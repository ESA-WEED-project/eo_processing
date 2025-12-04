from __future__ import annotations
from skimage.morphology import disk
from openeo.rest.datacube import DataCube
import openeo
from openeo.processes import if_, eq, array_element, gte
from typing import Union, List, TYPE_CHECKING

if TYPE_CHECKING:
    from eo_processing.config.data_formats import openEO_bbox_format

def convolve(img: DataCube, radius: int) -> DataCube:
    """
    Perform 2D convolution on the data cube using a disk-shaped kernel.

    This function applies a convolution to an input data cube using
    a kernel generated with a specified radius. The kernel is disk-shaped.
    The operation modifies the data cube by blending pixel values
    within the shape defined by the kernel to produce a smoothed
    or averaged result depending on the input parameters.

    :param img: Input data cube on which the convolution will be applied.
    :param radius: Radius to generate the disk-shaped kernel for convolution.
    :return: A new data cube after applying the convolution operation.
    """
    kernel = disk(radius)
    img = img.apply_kernel(kernel)
    return img

def classify_udm2(udm_array):
    """
    Classifies a pixel based on 6 UDM flags in PlanetScope data according to priority rules.

    :param udm_array: list or array of 6 integers
        Each element is a flag indicating a specific condition, arranged in this order:
        Index 0: Clear
        Index 1: Snow
        Index 2: Cloud Shadow
        Index 3: Light Haze	
        Index 4: Heavy Haze	
        Index 5: Cloud

    :returns: A single classification value representing the pixel class:
        - 7 = Cloud
        - 6 = Heavy Haze
        - 5 = Light Haze
        - 4 = Cloud shadow
        - 3 = Snow
        - 2 = Unclear
        - 1 = Clear
        - 0 = No condition matched (default/unknown)

    Priority:
    ---------
    The higher the class number, the higher the priority
    """
    snow = if_(eq(array_element(udm_array, 1), 1), 1, 0)
    cloud_shadow = if_(eq(array_element(udm_array, 2), 1), 1, 0)
    light_haze = if_(eq(array_element(udm_array, 3), 1), 1, 0)
    heavy_haze = if_(eq(array_element(udm_array, 4), 1), 1, 0)
    cloud = if_(eq(array_element(udm_array, 5), 1), 1, 0)
    confident = if_(gte(array_element(udm_array, 6), 0.80), 1, 0)
    
    # Return the classification based on priority: 
    return if_(cloud == 1, 7,
                if_(heavy_haze == 1, 6,
                    if_(light_haze == 1, 5,
                        if_(cloud_shadow == 1, 4,
                            if_(snow == 1, 3,
                                if_(confident == 0, 2, 1))))))

def scl_mask_erode_dilate(
        session: openeo.Connection,
        bbox: openEO_bbox_format,
        scl_layer_band: str ="SENTINEL2_L2A:SCL",
        erode_r: int =3,
        dilate_r: int =21,
        target_crs: Union[int, None] =None) -> DataCube:
    """
    Generates a binary mask from the Sentinel-2 Scene Classification Layer (SCL) by performing
    controlled erosion and dilation operations. This function applies a sequence of convolution
    operations to refine the mask based on specified erosion and dilation radii.

    The workflow involves:
    1. Resampling the classification layer to a resolution of 10m in the specified coordinate
       reference system (CRS).
    2. Creating an initial mask based on given classification values.
    3. Applying erosion to the inverted mask by dilating it with a specified radius.
    4. Reverting the erosion effect by binary inversion.
    5. Finally, dilating the refined mask with a specified dilation radius.
    6. Ensuring binary output by thresholding the processed mask to handle small oscillation
       effects post-convolution.

    :param session: OpenEO session used to load and process the Sentinel-2 Scene Classification Layer.
    :param bbox: Bounding box defining the spatial extent of the area of interest for the mask
        generation.
    :param scl_layer_band: Name of the Sentinel-2 Scene Classification collection and band,
        provided in the format "collection:band". For example, "SENTINEL2_L2A:SCL".
    :param erode_r: Radius to be used for erosion. This value dictates the size of the inverted
        mask dilation.
    :param dilate_r: Radius to be used for dilation. This determines the size of the
        final mask dilation applied after erosion.
    :param target_crs: Target coordinate reference system (CRS) of the output data. If None,
        the existing CRS is used.

    :return: A DataCube object containing the final binary mask after erosion and dilation.
    """

    layer_band = scl_layer_band.split(':')
    s2_sceneclassification = session.load_collection(
        layer_band[0], bands=[layer_band[1]], spatial_extent=bbox)

    classification = s2_sceneclassification.band(layer_band[1])

    # Force to go to 10m resolution for controlled erosion/dilation
    classification = classification.resample_spatial(projection=target_crs,
                                                     resolution=10.0)

    first_mask = (classification == 0)
    for mask_value in [1, 3, 8, 9, 10, 11]:
        first_mask = ((first_mask == 1) | (classification == mask_value))

    # Invert mask for erosion
    first_mask = first_mask.apply(lambda x: (x == 1).not_())

    # Apply erosion by dilation the inverted mask
    erode_cube = convolve(first_mask, erode_r)

    # Invert again
    erode_cube = (erode_cube > 0.1)
    erode_cube = erode_cube.apply(lambda x: (x == 1).not_())

    # Now dilate the mask
    dilate_cube = convolve(erode_cube, dilate_r)

    # Get binary mask. NOTE: >0.1 is a fix to avoid being triggered
    # by small non-zero oscillations after applying convolution
    dilate_cube = (dilate_cube > 0.1)

    return dilate_cube.rename_labels("bands", ["S2-CLOUD-MASK"])

def udm2_mask_erode_dilate(
        stac_url: str, 
        session: openeo.Connection, 
        bbox: openEO_bbox_format,
        temporal_extent: List[str],
        erode_r: int = 3, 
        dilate_r: int = 21, 
        target_crs: Union[int, None] = None) -> DataCube:
    """
    Applies erosion and dilation on a valid pixel mask extracted from UDM2 (Unusable Data Mask) bands.
    The process involves interpreting STAC items, resampling, identifying valid pixels through bitwise
    operations, and applying morphological operations to refine the mask.

    :param stac_url: URL of the STAC item to process.
    :param session: An established openEO connection used to access and process satellite imagery data.
    :param bbox: Dictionary defining the spatial extent with keys such as 'west', 'south', 'east', and 'north'.
    :param temporal_extent: List with start and end datetime strings defining the temporal range to query data.
    :param erode_r: Radius for erosion operation. Default value is 3.
    :param dilate_r: Radius for dilation operation. Default value is 21.
    :param target_crs: Target coordinate reference system for resampling spatial data. Default is None.
    :return: A binary DataCube object representing the final usable pixel mask.
    """

    bands = ["B01", "B02", "B03", "B04", "B05", "B06", "B07"]
    udm = session.load_stac(stac_url, 
                            bands=bands,
                            spatial_extent=bbox,
                            temporal_extent=temporal_extent,
                            properties=None)

    udm = udm.resample_spatial(projection=target_crs, resolution=3.0)

    # Assign each band to a variable
    clear      = udm.band("B01") == 1
    snow       = udm.band("B02") == 0
    shadow     = udm.band("B03") == 0
    light_haze = udm.band("B04") == 0
    heavy_haze = udm.band("B04") == 0
    cloud      = udm.band("B06") == 0
    unusable   = udm.band("B07") == 0  # All bits 0 = usable, can also be below a threshold

    # Combine all criteria for usable pixels
    valid_mask = clear & snow & shadow & light_haze & heavy_haze & cloud & unusable

    # Invert for erosion
    erode_input = valid_mask.apply(lambda x: (x == 1).not_())

    # Erosion
    eroded = convolve(erode_input, erode_r)

    # Invert back
    eroded = (eroded > 0.1).apply(lambda x: (x == 1).not_())

    # Dilation
    dilated = convolve(eroded, dilate_r)

    # Get binary mask. NOTE: >0.1 is a fix to avoid being triggered
    # by small non-zero oscillations after applying convolution
    final_mask = dilated > 0.1
    return final_mask
