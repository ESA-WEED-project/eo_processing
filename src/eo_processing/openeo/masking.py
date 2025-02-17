from skimage.morphology import disk
from openeo.rest.datacube import DataCube
import openeo
from eo_processing.utils.geoprocessing import openEO_bbox_format
from typing import Union

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

def scl_mask_erode_dilate(session: openeo.Connection, bbox: openEO_bbox_format,
                          scl_layer_band: str ="SENTINEL2_L2A:SCL",
                          erode_r: int =3, dilate_r: int =21, target_crs: Union[int, None] =None) -> DataCube:
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

    return dilate_cube
