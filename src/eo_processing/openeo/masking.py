from skimage.morphology import disk

def convolve(img, radius: int):
    '''OpenEO method to apply convolution
    with a circular kernel of `radius` pixels.
    NOTE: make sure the resolution of the image
    matches the expected radius in pixels!
    '''
    kernel = disk(radius)
    img = img.apply_kernel(kernel)
    return img

def scl_mask_erode_dilate(session, bbox,
                          scl_layer_band="SENTINEL2_L2A:SCL",
                          erode_r=3, dilate_r=21, target_crs=None):
    """OpenEO method to construct a Sentinel-2 mask based on SCL.
    It involves an erosion step followed by a dilation step.

    Args:
        session (openeo.Session): the connection openeo session
        scl_layer_band (str, optional): Which SCL band to use.
                Defaults to "TERRASCOPE_S2_TOC_V2:SCL".
        erode_r (int, optional): Erosion radius (pixels). Defaults to 3.
        dilate_r (int, optional): Dilation radius (pixels). Defaults to 13.

    Returns:
        DataCube: DataCube containing the resulting mask
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
