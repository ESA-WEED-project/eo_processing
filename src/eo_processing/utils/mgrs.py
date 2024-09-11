# -*- coding: utf-8 -*-
# !/usr/bin/env python
"""
some functions to retrieve the MGRS identifier for a coordinate and LL-2-UTM coordinate translation
"""

import pyproj

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
        Function uses rasterio built-in functions"""

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
        Function uses rasterio built-in functions """
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
       Fuction uses rasterio build-in functions """
    # first calculate UTM coordinates out of the LL
    easting, northing, zone, band = LL_2_UTM(lon, lat)
    # get the 100k subgrid letters
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone)

    return str("{0:0>2}".format(zone) + band + letters_100kgrid)


def UTM_2_MGRSid(easting, northing, zone_number, zone_letter):
    """Function to calculate the MGRS identifier (used in
       Sentinel-2 tile naming and PROBAV_UTM tile naming)
       from a given UTM coordinate tuple.
       Fuction uses rasterio build-in functions """
    # run directly helper function
    letters_100kgrid = MGRS_100k_letters(easting, northing, zone_number)

    return str("{0:0>2}".format(zone_number) + zone_letter + letters_100kgrid)
