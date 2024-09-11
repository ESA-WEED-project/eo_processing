""" we assign each LAEA grid cell either a direct Sentinel-2 tileID or a Sentinel-2 tileID with wildcard"""

import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from extentmapping.utils.mgrs import LL_2_MGRSid
import itertools


def create_wildcard_tileid_from_doublematch(tiles):
    """ create tile id with wildcard for double matches"""

    tile1 = tiles[0]
    tile2 = tiles[1]

    if tile1[0] == tile2[0]:
        one = tile1[0]
    else:
        one = '*'

    if tile1[1] == tile2[1]:
        two = tile1[1]
    else:
        two = '*'

    if tile1[2] == tile2[2]:
        three = tile1[2]
    else:
        three = '*'

    if tile1[3] == tile2[3]:
        four = tile1[3]
    else:
        four = '*'

    if tile1[4] == tile2[4]:
        five = tile1[4]
    else:
        five = '*'

    return one + two + three + four + five


def create_wildcard_tileid_from_triplematch(tiles):
    """ create tile id with wildcard for double matches"""

    tile1 = tiles[0]
    tile2 = tiles[1]
    tile3 = tiles[2]

    if tile1[0] == tile2[0] == tile3[0]:
        one = tile1[0]
    else:
        one = '*'

    if tile1[1] == tile2[1] == tile3[1]:
        two = tile1[1]
    else:
        two = '*'

    if tile1[2] == tile2[2] == tile3[2]:
        three = tile1[2]
    else:
        three = '*'

    if tile1[3] == tile2[3] == tile3[3]:
        four = tile1[3]
    else:
        four = '*'

    if tile1[4] == tile2[4] == tile3[4]:
        five = tile1[4]
    else:
        five = '*'

    return one + two + three + four + five


def create_wildcard_tileid_from_quadruplematch(tiles):
    """ create tile id with wildcard for double matches"""

    tile1 = tiles[0]
    tile2 = tiles[1]
    tile3 = tiles[2]
    tile4 = tiles[3]

    if tile1[0] == tile2[0] == tile3[0] == tile4[0]:
        one = tile1[0]
    else:
        one = '*'

    if tile1[1] == tile2[1] == tile3[1] == tile4[1]:
        two = tile1[1]
    else:
        two = '*'

    if tile1[2] == tile2[2] == tile3[2] == tile4[2]:
        three = tile1[2]
    else:
        three = '*'

    if tile1[3] == tile2[3] == tile3[3] == tile4[3]:
        four = tile1[3]
    else:
        four = '*'

    if tile1[4] == tile2[4] == tile3[4] == tile4[4]:
        five = tile1[4]
    else:
        five = '*'

    return one + two + three + four + five


# load the files
gdf_laea = gpd.read_file(r"C:\Users\BUCHHORM\Downloads\LAEA_20km_tiling_grid_EU_high_res_EPSG3035.gpkg")
gdf_s2 = gpd.read_file(r"C:\Users\buchhorm\Downloads\Sentinel2_tiling_grid_EU_high_res_EPSG3035_optimized.gpkg")


results = []

# run over LAEA grid
for row in gdf_laea.itertuples():
    print(f'* process LAEA tile {row.tile_id}')
    #filter the s2 tiles to buffered BBOX of LAEA grid
    aoi = row.geometry.buffer(100)
    xmin, ymin, xmax, ymax = aoi.bounds
    s2_aoi = gdf_s2.cx[xmin:xmax, ymin:ymax]

    # case when no S2 intersecting tiles are found for 20km grid cell - error
    if s2_aoi.empty:
        print(f'--- error: no S2 tile matches the bounds of LAEA grid {row.tile_id}')
        results.append([row.tile_id, 'error', None, None])
        continue

    # SINGLE WINNER
    if s2_aoi.shape[0] == 1:
        print(f' - single winner in first attempt.')
        results.append([row.tile_id, 'single', s2_aoi.tile_id.iloc[0], None])
        continue

    # DECISION FOR MULTIPLE SINGLE WINNER
    single_match = []
    for tile in s2_aoi.itertuples():
        if tile.geometry.contains(row.geometry):
            single_match.append([row.tile_id, 'single', tile.tile_id, None])
    if len(single_match) == 0:
        pass
    elif len(single_match) == 1:
        print(f' - single winner in second attempt.')
        results.append(single_match[0])
        continue
    elif len(single_match) > 1:
        print(f' - multiple single winners - we chose the best match')
        # calculate the MGRS identifier of centroid of LAEA grid cell
        ## get the latlon of centroid of the LAEA grid cell
        lon, lat = gpd.GeoSeries(row.geometry.centroid, crs=3035).to_crs(epsg=4326).get_coordinates().iloc[0]
        MGRSid = LL_2_MGRSid(lon, lat)

        iDone = False
        # try first if we have a direct hit to MGRS grid
        for element in single_match:
            if element[2] == MGRSid:
                results.append(element)
                iDone = True
                break
        if iDone:
            continue

        # now we try if we get a zone + band hit
        for element in single_match:
            if element[2][:3] == MGRSid[:3]:
                results.append(element)
                iDone = True
                break
        if iDone:
            continue

        # now we try just a zone hit
        for element in single_match:
            if element[2][:2] == MGRSid[:2]:
                results.append(element)
                iDone = True
                break
        if iDone:
            continue

        # damm - fuck it we just take the first hit from the single_match list
        results.append(single_match[0])
        continue

    # more sufisticate approach - we have to check the possibilities

    # TWO TILEID COVERING LAEA GRID TOGETHER
    if s2_aoi.shape[0] == 2:
        # we recheck if the combination of both s2 tiles completly cover the LEAE grid tile
        double_aoi = unary_union(s2_aoi.geometry.tolist())
        if double_aoi.contains(row.geometry):
            print(f' - double winner - use wildcard tileid')
            results.append([row.tile_id, 'double',
                            create_wildcard_tileid_from_doublematch(s2_aoi.tile_id.unique().tolist()),
                            ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
            continue
        else:
            print(f'--- error: double S2 tile matches do not cover LAEA grid {row.tile_id} completly')
            results.append([row.tile_id, 'error - double result but no coverage even when combined',
                            None, ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
            continue

    # THREE S2 TILEID INTERSECTION DECISION TREE
    if s2_aoi.shape[0] == 3:
        # do we have multiple epsg zones in the result
        epsg_test = s2_aoi.epsg.unique().tolist()

        # if we have TWO epsg zones we can check if the double of the one epsg zone covers all
        if len(epsg_test) == 2:
            s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]]
            s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]]

            if s2_sub_a.shape[0] == 2:
                s2_sub = s2_sub_a
            else:
                s2_sub = s2_sub_b

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner - use wildcard tileid')
                results.append([row.tile_id, 'double',
                                create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                continue

        # OK, we do not have two epsg zones OR we have two epsg zones and the double of the one zone was not enough
        # we check now all double combination of the triplet - first combi wins
        lS2tiles = s2_aoi.tile_id.unique().tolist()
        lCombi = list(itertools.combinations(lS2tiles, 2))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner - use wildcard tileid')
                results.append([row.tile_id, 'double',
                                create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # damm we really need all three Sentinel-2 tiles to cover the area
        results.append([row.tile_id, 'triplet',
                        create_wildcard_tileid_from_triplematch(s2_aoi.tile_id.unique().tolist()),
                        ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
        continue

    # FOUR S2 TILEID INTERSECTION DECISION TREE
    if s2_aoi.shape[0] == 4:
        # do we have multiple epsg zones in the result
        epsg_test = s2_aoi.epsg.unique().tolist()

        # if we have TWO epsg zones we check separatly
        # we can have two or three tileids in one epsg zone max
        if len(epsg_test) == 2:
            # all tests for the first epsg zone
            s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]]

            # now we have to check if we have only 2 tiles in the epsg filter or three
            if s2_sub_a.shape[0] == 2:

                # now we have only the two tileIDs with the same epsg number
                double_aoi = unary_union(s2_sub_a.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub_a.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                    continue

            if s2_sub_a.shape[0] == 3:
                # now we check all double combinations of the three results
                lS2tiles = s2_sub_a.tile_id.unique().tolist()
                lCombi = list(itertools.combinations(lS2tiles, 2))

                iDone = False
                for element in lCombi:
                    s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                    double_aoi = unary_union(s2_sub.geometry.tolist())
                    if double_aoi.contains(row.geometry):
                        print(f' - double winner - use wildcard tileid')
                        results.append([row.tile_id, 'double',
                                        create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                        ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                        iDone = True
                        break
                if iDone:
                    continue

                # we check if the triplet can cover the areas
                tripple_aoi = unary_union(s2_sub_a.geometry.tolist())
                if tripple_aoi.contains(row.geometry):
                    print(f' - tripple winner - use wildcard tileid')
                    results.append([row.tile_id, 'triplet',
                                    create_wildcard_tileid_from_triplematch(s2_sub_a.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                    continue

            # all tests for the second zone
            s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]]

            # now we have to check if we have only 2 tiles in the epsg filter or three
            if s2_sub_b.shape[0] == 2:

                # now we have only the two tileIDs with the same epsg number
                double_aoi = unary_union(s2_sub_b.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub_b.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                    continue

            if s2_sub_b.shape[0] == 3:
                # now we check all double combinations of the three results
                lS2tiles = s2_sub_b.tile_id.unique().tolist()
                lCombi = list(itertools.combinations(lS2tiles, 2))

                iDone = False
                for element in lCombi:
                    s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                    double_aoi = unary_union(s2_sub.geometry.tolist())
                    if double_aoi.contains(row.geometry):
                        print(f' - double winner - use wildcard tileid')
                        results.append([row.tile_id, 'double',
                                        create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                        ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                        iDone = True
                        break
                if iDone:
                    continue

                # we check if the triplet can cover the areas
                tripple_aoi = unary_union(s2_sub_b.geometry.tolist())
                if tripple_aoi.contains(row.geometry):
                    print(f' - tripple winner - use wildcard tileid')
                    results.append([row.tile_id, 'triplet',
                                    create_wildcard_tileid_from_triplematch(s2_sub_b.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                    continue

        # we have more than two epsg zones OR the tiles within one single epsg zone are not enough
        # we check all double combinations - first wins
        lS2tiles = s2_aoi.tile_id.unique().tolist()
        lCombi = list(itertools.combinations(lS2tiles, 2))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner - use wildcard tileid')
                results.append([row.tile_id, 'double',
                                create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # we check all tripple combinations - first wins
        lCombi = list(itertools.combinations(lS2tiles, 3))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
            triple_aoi = unary_union(s2_sub.geometry.tolist())
            if triple_aoi.contains(row.geometry):
                print(f' - triple winner - use wildcard tileid')
                results.append([row.tile_id, 'triplet',
                                create_wildcard_tileid_from_triplematch(s2_sub.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # damm we really need all four S2 tiles - a quadruple
        results.append([row.tile_id, 'quadruple',
                        create_wildcard_tileid_from_quadruplematch(s2_aoi.tile_id.unique().tolist()),
                        ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
        continue

    # now we deal with the rest - we should have not more than quadruples per epsg zone

    # do we have multiple epsg zones in the result
    epsg_test = s2_aoi.epsg.unique().tolist()

    # if we have TWO epsg zones we check separatly
    if len(epsg_test) == 2:
        # all tests for the first epsg zone
        s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]]

        # now we have to check if we have only 2 tiles in the epsg filter or three
        if s2_sub_a.shape[0] == 2:

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub_a.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner - use wildcard tileid')
                results.append([row.tile_id, 'double',
                                create_wildcard_tileid_from_doublematch(s2_sub_a.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        if s2_sub_a.shape[0] == 3:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_a.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check if the triplet can cover the areas
            tripple_aoi = unary_union(s2_sub_a.geometry.tolist())
            if tripple_aoi.contains(row.geometry):
                print(f' - tripple winner - use wildcard tileid')
                results.append([row.tile_id, 'triplet',
                                create_wildcard_tileid_from_triplematch(s2_sub_a.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        # check quadruple
        if s2_sub_a.shape[0] == 4:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_a.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check all tripple combinations - first wins
            lCombi = list(itertools.combinations(lS2tiles, 3))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                triple_aoi = unary_union(s2_sub.geometry.tolist())
                if triple_aoi.contains(row.geometry):
                    print(f' - triple winner - use wildcard tileid')
                    results.append([row.tile_id, 'triplet',
                                    create_wildcard_tileid_from_triplematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            quad_aoi = unary_union(s2_sub_a.geometry.tolist())
            if quad_aoi.contains(row.geometry):
                print(f' - quadruple winner - use wildcard tileid')
                results.append([row.tile_id, 'quadruple',
                                create_wildcard_tileid_from_quadruplematch(s2_sub_a.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        # all tests for the second epsg zone
        s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]]

        # now we have to check if we have only 2 tiles in the epsg filter or three
        if s2_sub_b.shape[0] == 2:

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub_b.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner - use wildcard tileid')
                results.append([row.tile_id, 'double',
                                create_wildcard_tileid_from_doublematch(s2_sub_b.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue

        if s2_sub_b.shape[0] == 3:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_b.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check if the triplet can cover the areas
            tripple_aoi = unary_union(s2_sub_b.geometry.tolist())
            if tripple_aoi.contains(row.geometry):
                print(f' - tripple winner - use wildcard tileid')
                results.append([row.tile_id, 'triplet',
                                create_wildcard_tileid_from_triplematch(s2_sub_b.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue

        # check quadruple
        if s2_sub_b.shape[0] == 4:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_b.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner - use wildcard tileid')
                    results.append([row.tile_id, 'double',
                                    create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check all tripple combinations - first wins
            lCombi = list(itertools.combinations(lS2tiles, 3))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
                triple_aoi = unary_union(s2_sub.geometry.tolist())
                if triple_aoi.contains(row.geometry):
                    print(f' - triple winner - use wildcard tileid')
                    results.append([row.tile_id, 'triplet',
                                    create_wildcard_tileid_from_triplematch(s2_sub.tile_id.unique().tolist()),
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            quad_aoi = unary_union(s2_sub_b.geometry.tolist())
            if quad_aoi.contains(row.geometry):
                print(f' - quadruple winner - use wildcard tileid')
                results.append([row.tile_id, 'quadruple',
                                create_wildcard_tileid_from_quadruplematch(s2_sub_b.tile_id.unique().tolist()),
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue


    # we check all double combinations - first wins
    lS2tiles = s2_aoi.tile_id.unique().tolist()
    lCombi = list(itertools.combinations(lS2tiles, 2))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
        double_aoi = unary_union(s2_sub.geometry.tolist())
        if double_aoi.contains(row.geometry):
            print(f' - double winner - use wildcard tileid')
            results.append([row.tile_id, 'double',
                            create_wildcard_tileid_from_doublematch(s2_sub.tile_id.unique().tolist()),
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # we check all tripple combinations - first wins
    lCombi = list(itertools.combinations(lS2tiles, 3))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
        triple_aoi = unary_union(s2_sub.geometry.tolist())
        if triple_aoi.contains(row.geometry):
            print(f' - triple winner - use wildcard tileid')
            results.append([row.tile_id, 'triplet',
                            create_wildcard_tileid_from_triplematch(s2_sub.tile_id.unique().tolist()),
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # we check all quadruple combinations - first wins
    lCombi = list(itertools.combinations(lS2tiles, 4))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))]
        triple_aoi = unary_union(s2_sub.geometry.tolist())
        if triple_aoi.contains(row.geometry):
            print(f' - quadruple winner - use wildcard tileid')
            results.append([row.tile_id, 'quadruple',
                            create_wildcard_tileid_from_quadruplematch(s2_sub.tile_id.unique().tolist()),
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # fuck - still no winner..... I already tested too much and made it way to complicated.... just put all in
    results.append([row.tile_id, 'multiple - write filter by hand',
                    None, ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])

# merge the results and write out
df_result = pd.DataFrame(results, columns=['laea_tileid', 'match', 's2_tileid', 's2_multi_list'])

if gdf_laea.shape[0] != df_result.shape[0]:
    print(' -- error: we have a mismatch between number of LAEA grids and the number of results')

gdf_result = gdf_laea.merge(df_result, how='left', left_on='tile_id', right_on='laea_tileid')

#write out
gdf_result.to_file(r'C:\Users\buchhorm\Downloads\LAEA_20km_tiling_grid_S2info.gpkg')