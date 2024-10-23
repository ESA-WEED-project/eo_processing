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


def MGRS_100k_letters(easting, northing, zone):
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
    en = (_Le100k[(zone - 1) % 3][int(E) - 1] + _Ln100k[(zone - 1) % 2][int(N) % len(_Ln100k[0])])
    return en


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
    easting, northing, zone, band = LL_2_UTM(lon, lat)
    # get the 100k subgrid letters
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone)

    return str("{0:0>2}".format(zone) + band + letters_100kgrid)


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


def calculate_MGRSid10(easting: float, northing: float, zone: int, band: str) -> str:
    """ Returns the 13-character Military Grid Reference System (MGRS) 10m identifier as a string.

    :param easting: The easting value of the UTM coordinate.
    :param northing: The northing value of the UTM coordinate.
    :param zone: The UTM zone number.
    :param band: The UTM latitude band letter.
    :return: MGRSid10 string.
    """
    mgrs_id = UTM_2_MGRSid(easting, northing, zone, band)
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
        easting, northing, zone, band = LL_2_UTM(longitude, latitude)
    except Exception:
        raise ValueError('Given coordinates did not follow the required longitude, latitude standard.')

    rounded_easting = floor_to_nearest_5(easting)
    rounded_northing = floor_to_nearest_5(northing)
    center_lon, center_lat = UTM_2_LL(rounded_easting, rounded_northing, zone, band)

    MGRSid10 = calculate_MGRSid10(rounded_easting, rounded_northing, zone, band)

    return MGRSid10, round(center_lon, 7), round(center_lat, 7)

def tileID_2_epsg(S2_tileID: str) -> int:
    """
    :param S2_tileID: String representing the S2 tile ID.
    :return: EPSG code as an integer corresponding to the S2 tile ID.
    """
    northern = (S2_tileID[2] >= 'N')

    # get EPSG number from zone number and northern
    if northern:
        return 32600 + int(S2_tileID[:2])
    else:
        return 32700 + int(S2_tileID[:2])
