# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
some functions to retrieve the MGRS identifier for a coordinate and LL-2-UTM coordinate translation
"""

import pyproj
from math import trunc

def latitude_to_zone_letter(latitude):
    """ Helper function to get the UTM zone letter
        from a given latitude """
    ZONE_LETTERS = "CDEFGHJKLMNPQRSTUVWXX"
    if -80 <= latitude <= 84:
        return ZONE_LETTERS[int(latitude + 80) >> 3]
    else:
        return None

def latlon_to_zone_number(longitude, latitude):
    """ Helper function to get the UTM zone number
        from a given latitude and longitude """
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

def MGRS_100k_letters(easting, northing, zone_number):
    """ Helper function to get the MGRS 100 kilometer
        sub-grid letter for easting and northing value of UTM coordinate """
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

def MGRS_2Mil_letter(northing, zone_letter):
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
    N, _ = divmod(northing, 2e6)

    if zone_letter >= 'N':
        return _Ln2million_north[int(N)]
    else:
        return _Ln2million_south[int(N)]

def LL_2_UTM(lon, lat, forced_epsg=None):
    """ Function to calculate the UTM coordinates plus
        zone_number and zone_letter from a given
        longitude and latitude.
        NOTE: UTM zone can be forced by given the EPSG number (int) directly.
        Note2: if you force epsg the zone_letter will be a faked one - for
               northern_hemisphere = Z and for southern_hemisphere = A !!!!
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

def UTM_2_LL(easting, northing, zone_number, zone_letter):
    """ Function to calculate longitude and latitude coordinates
        from a given UTM eastin and northing value plus
        the UTM zone number and zone letter.
        NOTE: if you are not sure of the zone_letter since you come
              from random UTM coordinates then set 'Z' for northern hemisphere
              and 'A' for southern hemisphere
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

def LL_2_MGRSid(lon, lat):
    """Function to calculate the MGRS identifier (used in
       Sentinel-2 tile naming and PROBAV_UTM tile naming)
       from a given longitude and latitude.
    """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone_number, zone_letter = LL_2_UTM(lon, lat)

    return UTM_2_MGRSid(easting, northing, zone_number, zone_letter)

def UTM_2_MGRSid(easting, northing, zone_number, zone_letter):
    """Function to calculate the MGRS identifier (used in
       Sentinel-2 tile naming and PROBAV_UTM tile naming)
       from a given UTM coordinate tuple.
    """
    # run directly helper function
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone_number)

    return str("{0:0>2}".format(zone_number) + zone_letter + letters_100kgrid)

def floor_to_nearest_5(value: float) -> int:
    """ Floor the given value to the nearest 10 and add 5.

    :param value: The input floating point number to be floored.
    :return: The nearest integer to the input value that is a multiple of 5.
    """
    return (trunc(value / 10.0) * 10) + 5

def calculate_MGRSid10(easting: float, northing: float, zone_number: int, zone_letter: str) -> str:
    """ Returns the 13-character Military Grid Reference System (MGRS) 10m identifier as a string.

    :param easting: The easting value of the UTM coordinate.
    :param northing: The northing value of the UTM coordinate.
    :param zone: The UTM zone number.
    :param band: The UTM latitude band letter.
    :return: MGRSid10 string.
    """
    mgrs_id = UTM_2_MGRSid(easting, northing, zone_number, zone_letter)
    formatted_easting = str(int(easting))[-5:-1]
    formatted_northing = str(int(northing))[-5:-1]
    return f'{mgrs_id}{formatted_easting}{formatted_northing}'

def get_MGRSid10_center(longitude: float, latitude: float) -> tuple[str, float, float]:
    """
    Calculate the MGRSid10 Geolocation Index and center coordinates in latitude and longitude of the corresponding
    reference point location in MGRS 10-meter grid.

    :param longitude: Longitude of the location in decimal degrees.
    :param latitude: Latitude of the location in decimal degrees.
    :return: MGRSid10, center longitude, center latitude in decimal degrees.
    """
    try:
        easting, northing, zone_number, zone_letter = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    rounded_easting = floor_to_nearest_5(easting)
    rounded_northing = floor_to_nearest_5(northing)
    center_lon, center_lat = UTM_2_LL(rounded_easting, rounded_northing, zone_number, zone_letter)

    MGRSid10 = calculate_MGRSid10(rounded_easting, rounded_northing, zone_number, zone_letter)

    return MGRSid10, round(center_lon, 7), round(center_lat, 7)

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
    # run directly helper function
    grid100id = UTM_2_grid100id(easting, northing, zone_number, zone_letter)

    # get the row / column number for 20k subgrid in MGRS_100k_grid
    formatted_easting = str(int(easting))[-5:]
    formatted_northing = str(int(northing))[-5:]
    e = int(divmod(int(formatted_easting), 20000)[0])
    n = int(divmod(int(formatted_northing), 20000)[0])

    return grid100id + str(e) + str(n)

def LL_2_grid20id(lon: float, lat: float) -> str:
    """ warper around UTM_2_grid20id function to start from lat/lon coordinate.

    :param lon: Longitude in decimal degrees
    :param lat: Latitude in decimal degrees
    :return: Grid 20 ID based on the provided longitude and latitude
    """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone_number, zone_letter = LL_2_UTM(lon, lat)
    return UTM_2_grid20id(easting, northing, zone_number, zone_letter)

def grid20id_2_epsg(grid20id: str) -> int:
    """ Converts the grid20id to the corresponding EPSG code of this 20x20km UTM grid.

    :param grid20id: A string representing the grid20id.
    :return: The corresponding EPSG code as an integer.
    """
    northern = (grid20id[2] > 'ε')

    # get EPSG number from zone number and northern
    if northern:
        return 32600 + int(grid20id[:2])
    else:
        return 32700 + int(grid20id[:2])

def tileID_2_epsg(MGRSid: str) -> int:
    """
    :param MGRSid: String representing the MGRS tile ID (or S2 tile id).
    :return: EPSG code as an integer corresponding to tile ID.
    """
    northern = (MGRSid[2] >= 'N')

    # get EPSG number from zone number and northern
    if northern:
        return 32600 + int(MGRSid[:2])
    else:
        return 32700 + int(MGRSid[:2])
