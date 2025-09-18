# -*- coding: utf-8 -*-
# !/usr/bin/env python

import pyproj
from math import trunc, floor
from typing import Union, Tuple
import warnings

def latitude_to_zone_letter(latitude: float) -> Union[str, None]:
    """
    Converts a given latitude into its corresponding UTM (Universal Transverse Mercator) zone letter.
    This is determined based on the latitude's position within predefined zone bands.

    :param latitude: Latitude in degrees. Should be in the range -80 to 84.
    :return: The UTM zone letter corresponding to the latitude if within the valid range.
             If the latitude is outside the valid range (-80 to 84), returns None.
    """
    ZONE_LETTERS = "CDEFGHJKLMNPQRSTUVWXX"
    if -80 <= latitude <= 84:
        return ZONE_LETTERS[int(latitude + 80) >> 3]
    else:
        return None

def latlon_to_zone_number(longitude: float, latitude: float) -> int:
    """
    Determines the UTM (Universal Transverse Mercator) zone number based on the
    provided geographic coordinates (longitude and latitude). Special handling
    is applied for certain areas, such as Norway and Svalbard regions, to account
    for their unique zone allocations.

    :param longitude: The geographic longitude in decimal degrees.
    :param latitude: The geographic latitude in decimal degrees.
    :return: The UTM zone number corresponding to the location.
    """
    if 56 <= latitude < 64 and 3 <= longitude < 12:
        return 32

    if 72 <= latitude <= 84 and longitude >= 0:
        if longitude < 9:
            return 31
        elif longitude < 21:
            return 33
        elif longitude < 33:
            return 35
        elif longitude < 42:
            return 37

    return int((longitude + 180) / 6) + 1

def MGRS_100k_letters(easting: float, northing:float, zone_number: int) -> str:
    """
    Generates the two-letter 100-km grid square designator for a given easting,
    northing and zone number in the Military Grid Reference System (MGRS).

    The MGRS grid is based on the UTM coordinate system. The given easting and
    northing values are used to determine the specific 100-km grid square within
    a UTM zone. The function calculates two letters: one representing the easting
    sub-grid and the other representing the northing sub-grid. These letters are
    calculated using predefined repeating patterns and the zone number.

    :param easting: The easting coordinate in meters within a UTM zone.
    :param northing: The northing coordinate in meters within a UTM zone.
    :param zone_number: The UTM zone number (1 to 60).
    :return: A two-character string representing the 100-km grid square designator.
    """
    ## ini some constants
    # 100km in meter
    _100km = 100e3
    # 100 km sub-grid easting (‘e’) letters repeat every third zone
    _Le100k = 'ABCDEFGH', 'JKLMNPQR', 'STUVWXYZ'
    # 100 km sub-grid northing (‘n’) letters repeat every other zone
    _Ln100k = 'ABCDEFGHJKLMNPQRSTUV', 'FGHJKLMNPQRSTUVABCDE'

    ## get the correct combination out of easting and northing value
    E, _ = divmod(easting, _100km)
    N, _ = divmod(northing, _100km)
    # easting sub-grid letters in zone 1 are A-H, zone 2 J-R, zone 3 S-Z, then
    # repeating every 3rd zone (note -1 because eastings start
    # at 166e3 due to 500km false origin)
    # northing sub-grid letters in even zones are A-V, in odd zones are F-E
    en = (_Le100k[(zone_number - 1) % 3][int(E) - 1] + _Ln100k[(zone_number - 1) % 2][int(N) % len(_Ln100k[0])])
    return en

def MGRS_2Mil_letter(northing: float, zone_letter: str) -> str:
    """ calculate the 2 million m northern identifier for the MGRS 100K square identification. The MGRS 100K square
        is repeated every 2 million meter in the northing and therefore not unique standalone.

    :param northing: int, URM northing value of the coordinate
    :param zone_letter: string, UTM zone letter of the coordinate representing the GZL
    :return: string, greek letter representing the 2 million meter northern identifier
    """
    ## ini constants
    # 2 million northing grid letters for northern & southern hemisphere (ordered from south to north)
    _Ln2million_north = 'λπστφ'
    _Ln2million_south = 'αßΓδε'

    # get the correct 2mio letter from northing value
    index = int(northing // 2e6)

    if zone_letter >= 'N':
        return _Ln2million_north[index]
    else:
        return _Ln2million_south[index]

def LL_2_UTM(lon: float, lat: float, forced_epsg: Union[int, None]=None) -> Tuple[float, float, int, str]:
    """
    Converts geographic coordinates (longitude, latitude) to UTM (Universal Transverse Mercator) coordinates.

    This function calculates the UTM zone number and zone letter based on the given longitude and latitude.
    It supports overriding the calculated EPSG (European Petroleum Survey Group) code using the optional
    `forced_epsg` parameter. Additionally, the function performs coordinate transformation using the calculated
    or overridden EPSG code to convert geographic coordinates into UTM coordinates.

    :param lon: Longitude of the geographic coordinate.
    :param lat: Latitude of the geographic coordinate.
    :param forced_epsg: Optional parameter to override the calculated EPSG code with a specific one.
    :return: A tuple containing the UTM easting, UTM northing, UTM zone number, and UTM zone letter.
    """

    # calculate the UTM zone_number and zone_letter from lon, lat
    zone_number = latlon_to_zone_number(lon, lat)
    zone_letter = latitude_to_zone_letter(lat)

    # figure out from zone_letter if N or S
    northern = (zone_letter >= 'N')

    # get EPSG number from zone number and northern
    if northern:
        target_EPSG = 32600 + zone_number
    else:
        target_EPSG = 32700 + zone_number

    if forced_epsg is not None:
        target_EPSG = forced_epsg
        # also override the zone_number and zone_letter
        zone_number = int(str(forced_epsg)[-2:])
        if int(str(forced_epsg)[2]) == 6:
            zone_letter = 'Z'
        else:
            zone_letter = 'A'

    # do coordinat transformation
    transformer = pyproj.Transformer.from_crs('EPSG:4326', f'EPSG:{target_EPSG}', always_xy=True)
    target_easting, target_northing = transformer.transform(lon, lat)

    return target_easting, target_northing, zone_number, zone_letter

def UTM_2_LL(easting: float, northing: float, zone_number: int, zone_letter:str) -> Tuple[float, float]:
    """
    Converts UTM (Universal Transverse Mercator) coordinates to latitude and longitude
    (WGS84 coordinate system). This function takes easting, northing, zone number, and
    zone letter as inputs and transforms them into corresponding geographic coordinates.

    :param easting: The easting value (x-coordinate) of the UTM position.
    :param northing: The northing value (y-coordinate) of the UTM position.
    :param zone_number: The UTM zone number which designates the longitudinal zone.
    :param zone_letter: The UTM zone letter which specifies the latitude band.
    :return: A tuple of the form (longitude, latitude) representing the geographic
             coordinates in decimal degrees (WGS84 datum).
    """
    # get source_crs
    northern = (zone_letter >= 'N')
    if northern:
        source_EPSG = 32600 + zone_number
    else:
        source_EPSG = 32700 + zone_number

    # do coordinat transformation
    transformer = pyproj.Transformer.from_crs(f'EPSG:{source_EPSG}', 'EPSG:4326', always_xy=True)
    target_lon, target_lat = transformer.transform(float(easting), float(northing))

    return target_lon, target_lat

def LL_2_MGRSid(lon: float, lat: float) -> str:
    """
    Converts geographic coordinates (longitude and latitude) to a Military Grid Reference System (MGRS) identifier.
    This function uses the UTM coordinates as an intermediary step to calculate the MGRS identifier,
    providing a standardized grid reference for geographic locations.

    :param lon: Longitude of the location in decimal degrees.
    :param lat: Latitude of the location in decimal degrees.
    :return: The MGRS identifier as a string representing the geographic location.
    """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone_number, zone_letter = LL_2_UTM(lon, lat)

    return UTM_2_MGRSid(easting, northing, zone_number, zone_letter)

def UTM_2_MGRSid(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """
    Converts UTM (Universal Transverse Mercator) coordinates to an MGRS (Military Grid
    Reference System) coordinate identifier. This function takes in UTM parameters such as
    easting, northing, zone number, and zone letter and returns the corresponding MGRS
    coordinate string.

    :param easting: The easting value (meters from the central meridian) for the UTM
        coordinates.
    :param northing: The northing value (meters from the equator) for the UTM coordinates.
    :param zone_number: The zone number for the UTM coordinates; a number between 1 and 60.
    :param zone_letter: The zone letter for the UTM coordinates; a single-character string
        denoting the latitude band.
    :return: The converted MGRS coordinate identifier as a string.
    """
    # run directly helper function
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone_number)

    return str("{0:0>2}".format(zone_number) + zone_letter + letters_100kgrid)

# TODO replace all floor_to_nearest_5 references by compute_pixel_center in pipelines
def floor_to_nearest_5(value: float) -> int:
    """ Floor the given value to the nearest 10 and add 5.

    :param value: The input floating point number to be floored.
    :return: The nearest integer to the input value that is a multiple of 5.
    """
    warnings.warn(
        "floor_to_nearest_5 is deprecated and will be removed in a future release. "
        "Use compute_pixel_center(value, resolution=10.0) instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return (trunc(value / 10.0) * 10) + 5

def compute_pixel_center(value: float, resolution: float=10.0) -> float:
    """Floor the given value to the nearest resolution size and add half of the resolution.

    :param value: The input floating point number to be floored.
    :param resolution: The pixel/bin size.
    :return: The center of the resolution-sized bin in which the input value lies.
    """
    half_res = resolution / 2
    return (value // resolution * resolution) + half_res

def UTM_2_MGRSid10(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """ Returns the 13-character Military Grid Reference System (MGRS) 10m identifier as a string.

    :param easting: The easting value of the UTM coordinate.
    :param northing: The northing value of the UTM coordinate.
    :param zone_number: The UTM zone number.
    :param zone_letter: The UTM latitude band letter.
    :return: MGRSid10 string.
    """
    mgrs_id = UTM_2_MGRSid(easting, northing, zone_number, zone_letter)
    formatted_easting = f"{int(easting):05d}"[-5:-1]
    formatted_northing = f"{int(northing):05d}"[-5:-1]
    return f'{mgrs_id}{formatted_easting}{formatted_northing}'

def LL_2_MGRSid10(longitude: float, latitude: float) -> str:
    """ Returns the 13-character Military Grid Reference System (MGRS) 10m identifier as a string.

    :param longitude: Longitude of the point in decimal degrees
    :param latitude: Latitude of the point in decimal degrees
    :return: MGRSid10 string
    """
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    return UTM_2_MGRSid10(easting, northing, zone_number, zone_letter)

def get_MGRSid10_centerLL(longitude: float, latitude: float) -> Tuple[float, float]:
    """
    Calculate the MGRSid10 Geolocation center coordinates in latitude and longitude of the corresponding
    reference point location in MGRS 10-meter grid.

    :param longitude: Longitude of the location in decimal degrees.
    :param latitude: Latitude of the location in decimal degrees.
    :return: center longitude, center latitude in decimal degrees.
    """
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    rounded_easting = compute_pixel_center(easting, 10.0)
    rounded_northing = compute_pixel_center(northing, 10.0)
    center_lon, center_lat = UTM_2_LL(rounded_easting, rounded_northing, zone_number, zone_letter)

    return round(center_lon, 7), round(center_lat, 7)

def UTM_2_MGRSid1(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """ Returns the 15-character Military Grid Reference System (MGRS) 1m identifier as a string.

    :param easting: The easting value of the UTM coordinate.
    :param northing: The northing value of the UTM coordinate.
    :param zone_number: The UTM zone number.
    :param zone_letter: The UTM latitude band letter.
    :return: MGRSid1 string.
    """
    mgrs_id = UTM_2_MGRSid(easting, northing, zone_number, zone_letter)
    formatted_easting = f"{int(easting):05d}"[-5:0]
    formatted_northing = f"{int(northing):05d}"[-5:0]
    return f'{mgrs_id}{formatted_easting}{formatted_northing}'

def LL_2_MGRSid1(longitude: float, latitude: float) -> str:
    """ Returns the 15-character Military Grid Reference System (MGRS) 1m identifier as a string.

    :param longitude: Longitude of the point in decimal degrees
    :param latitude: Latitude of the point in decimal degrees
    :return: MGRSid1 string
    """
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    return UTM_2_MGRSid1(easting, northing, zone_number, zone_letter)

def get_MGRSid1_centerLL(longitude: float, latitude: float) -> Tuple[float, float]:
    """
    Calculate the MGRSid1 Geolocation center coordinates in latitude and longitude of the corresponding
    reference point location in MGRS 1-meter grid.

    :param longitude: Longitude of the location in decimal degrees.
    :param latitude: Latitude of the location in decimal degrees.
    :return: center longitude, center latitude in decimal degrees.
    """
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    rounded_easting = compute_pixel_center(easting, 1.0)
    rounded_northing = compute_pixel_center(northing, 1.0)
    center_lon, center_lat = UTM_2_LL(rounded_easting, rounded_northing, zone_number, zone_letter)

    return round(center_lon, 7), round(center_lat, 7)

def UTM_2_grid100id(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """ The grid100id represents a unique, non-overlapping UTM 100x100km tiling grid for processing in openEO. It differs
        from the MGRS system that the Grid Zone Designation letter (third digit) is a greek letter representing the
        2 million meter northern subgrid to get unique 100k square identifiers. This is needed since in the MGRS system
        the GZD_letter is based on longitude and therefore can split 100k grids cells into two names.

    :param easting: The easting value in meters for the UTM coordinate.
    :param northing: The northing value in meters for the UTM coordinate.
    :param zone_number: The UTM zone number.
    :param zone_letter: The UTM zone letter.
    :return: A string representing the 100-km grid square identifier in UTM processign grid.
    """
    # run directly helper function
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone_number)
    letter_2mgrid = MGRS_2Mil_letter(northing, zone_letter)

    return str("{0:0>2}".format(zone_number) + letter_2mgrid + letters_100kgrid)

def LL_2_grid100id(lon: float, lat: float) -> str:
    """ warper around UTM_2_grid100id function to start from lat/lon coordinate.

    :param lon: Longitude in decimal degrees
    :param lat: Latitude in decimal degrees
    :return: Grid 100 ID based on the provided longitude and latitude
    """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone_number, zone_letter = LL_2_UTM(lon, lat)
    return UTM_2_grid100id(easting, northing, zone_number, zone_letter)

def UTM_2_grid20id(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """ The grid20id represents a unique, non-overlapping UTM 20x20km tiling grid for processing in openEO. It differs
        from the MGRS system that the Grid Zone Designation letter (third digit) is a greek letter representing the
        2 million meter northern subgrid to get unique 100k square identifiers. This is needed since in the MGRS system
        the GZD_letter is based on longitude and therefore can split 100k grids cells into two names.
        The last two digits in the grid20id represent the eating and northing position of the 20x20km subgrid in the
        100k square grid (00 representing the lower left 20x20km subgrid AND 44 the upper right 20x20km subgrid).

    :param easting: The easting value of the UTM coordinate in meters.
    :param northing: The northing value of the UTM coordinate in meters.
    :param zone_number: The zone number of the UTM coordinate.
    :param zone_letter: The zone letter of the UTM coordinate.
    :return: A string representing the 20k grid ID.
    """
    # run directly helper function - get the grid100id
    grid100id = UTM_2_grid100id(easting, northing, zone_number, zone_letter)

    ## get the row / column number for 20k subgrid in MGRS_100k_grid
    # Calculate the remaining component within the 100k grid interval
    formatted_easting = easting % 100000
    formatted_northing = northing % 100000

    # Calculate the subgrid indices (0-4) within the 20x20 km grid
    e = int(formatted_easting // 20000)
    n = int(formatted_northing // 20000)

    return f"{grid100id}{e}{n}"

def LL_2_grid20id(lon: float, lat: float) -> str:
    """ warper around UTM_2_grid20id function to start from lat/lon coordinate.

    :param lon: Longitude in decimal degrees
    :param lat: Latitude in decimal degrees
    :return: Grid 20 ID based on the provided longitude and latitude
    """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone_number, zone_letter = LL_2_UTM(lon, lat)
    return UTM_2_grid20id(easting, northing, zone_number, zone_letter)

def gridID_2_epsg(gridid: str) -> int:
    """ Converts the grid100id or grid20id to the corresponding EPSG code of this 100x100km or 20x20km UTM grid.

    :param gridid: A string representing the grid20id or grid100id.
    :return: The corresponding EPSG code as an integer.
    """
    northern = (gridid[2] > 'ε')

    # get EPSG number from zone number and northern
    if northern:
        return 32600 + int(gridid[:2])
    else:
        return 32700 + int(gridid[:2])

def MGRSid_2_epsg(MGRSid: str) -> int:
    """ Converts the MGRSid, MGRSid10 or MGRSid1 to the corresponding EPSG code of this 100x100km, 10x10m or 1mx1m UTM grid.
    :param MGRSid: String representing the MGRS tile ID (or S2 tile id).
    :return: EPSG code as an integer corresponding to tile ID.
    """
    northern = (MGRSid[2] >= 'N')

    # get EPSG number from zone number and northern
    if northern:
        return 32600 + int(MGRSid[:2])
    else:
        return 32700 + int(MGRSid[:2])
