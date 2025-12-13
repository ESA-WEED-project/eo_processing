"""
we assign each LAEA grid cell one or multiple Sentinel-2 tileIDs which are needed to load the LAEA tile

Note: we only run this approach for 50km and 20km tiles.... the 100K is created out of the lists of the
     20K and 50K grids (use one and check against the other)

"""

import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union
from eo_processing.utils.mgrs import LL_2_MGRSid
import itertools

### declaration
# load the files
gdf_s2 = gpd.read_file(r"C:\Users\buchhorm\Downloads\new_grids\Sentinel2_tiling_grid_EU_high_res_EPSG3035.gpkg")

gdf_laea = gpd.read_file(r"C:\Users\BUCHHORM\Downloads\new_grids\LAEA_20km_tiling_grid_EU_high_res_EPSG3035.gpkg")
path_out = r'C:\Users\buchhorm\Downloads\new_grids\S2_tile_info_20K_grid.gpkg'

results = []

# run over LAEA grid
for row in gdf_laea.itertuples():
    print(f'* process LAEA tile {row.name}')
    #filter the s2 tiles to buffered BBOX of LAEA grid
    aoi = row.geometry.buffer(100)
    xmin, ymin, xmax, ymax = aoi.bounds
    gdf_s2_c = gdf_s2.copy()
    s2_aoi = gdf_s2_c.cx[xmin:xmax, ymin:ymax]

    # case when no S2 intersecting tiles are found for grid cell - error
    if s2_aoi.empty:
        print(f'--- error: no S2 tile matches the bounds of LAEA grid {row.name}')
        results.append([row.name, 'error', None, None])
        continue

    # SINGLE WINNER
    if s2_aoi.shape[0] == 1:
        print(f' - single winner in first attempt.')
        results.append([row.name, 'single', s2_aoi.tile_id.iloc[0], s2_aoi.tile_id.iloc[0]])
        continue

    # DECISION FOR MULTIPLE SINGLE WINNER
    single_match = []
    for tile in s2_aoi.itertuples():
        if tile.geometry.contains(row.geometry):
            single_match.append([row.name, 'single', tile.tile_id, tile.tile_id])
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
            print(f' - double winner ')
            results.append([row.name, 'double',
                            None,
                            ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
            continue
        else:
            print(f'--- error: double S2 tile matches do not cover LAEA grid {row.name} completly')
            results.append([row.name, 'error - double result but no full coverage even when combined',
                            None, ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
            continue

    # THREE S2 TILEID INTERSECTION DECISION TREE
    if s2_aoi.shape[0] == 3:
        # do we have multiple epsg zones in the result
        epsg_test = s2_aoi.epsg.unique().tolist()

        # if we have TWO epsg zones we can check if the double of the one epsg zone covers all
        if len(epsg_test) == 2:
            s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]].copy()
            s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]].copy()

            if s2_sub_a.shape[0] == 2:
                s2_sub = s2_sub_a
            else:
                s2_sub = s2_sub_b

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner ')
                results.append([row.name, 'double',
                                None,
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                continue

        # OK, we do not have two epsg zones OR we have two epsg zones and the double of the one zone was not enough
        # we check now all double combination of the triplet - first combi wins
        lS2tiles = s2_aoi.tile_id.unique().tolist()
        lCombi = list(itertools.combinations(lS2tiles, 2))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner ')
                results.append([row.name, 'double',
                                None,
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # damm we really need all three Sentinel-2 tiles to cover the area
        results.append([row.name, 'triplet',
                        None,
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
            s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]].copy()

            # now we have to check if we have only 2 tiles in the epsg filter or three
            if s2_sub_a.shape[0] == 2:

                # now we have only the two tileIDs with the same epsg number
                double_aoi = unary_union(s2_sub_a.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                    continue

            if s2_sub_a.shape[0] == 3:
                # now we check all double combinations of the three results
                lS2tiles = s2_sub_a.tile_id.unique().tolist()
                lCombi = list(itertools.combinations(lS2tiles, 2))

                iDone = False
                for element in lCombi:
                    s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                    double_aoi = unary_union(s2_sub.geometry.tolist())
                    if double_aoi.contains(row.geometry):
                        print(f' - double winner ')
                        results.append([row.name, 'double',
                                        None,
                                        ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                        iDone = True
                        break
                if iDone:
                    continue

                # we check if the triplet can cover the areas
                tripple_aoi = unary_union(s2_sub_a.geometry.tolist())
                if tripple_aoi.contains(row.geometry):
                    print(f' - tripple winner ')
                    results.append([row.name, 'triplet',
                                    None,
                                    ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                    continue

            # all tests for the second zone
            s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]].copy()

            # now we have to check if we have only 2 tiles in the epsg filter or three
            if s2_sub_b.shape[0] == 2:

                # now we have only the two tileIDs with the same epsg number
                double_aoi = unary_union(s2_sub_b.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                    continue

            if s2_sub_b.shape[0] == 3:
                # now we check all double combinations of the three results
                lS2tiles = s2_sub_b.tile_id.unique().tolist()
                lCombi = list(itertools.combinations(lS2tiles, 2))

                iDone = False
                for element in lCombi:
                    s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                    double_aoi = unary_union(s2_sub.geometry.tolist())
                    if double_aoi.contains(row.geometry):
                        print(f' - double winner ')
                        results.append([row.name, 'double',
                                        None,
                                        ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                        iDone = True
                        break
                if iDone:
                    continue

                # we check if the triplet can cover the areas
                tripple_aoi = unary_union(s2_sub_b.geometry.tolist())
                if tripple_aoi.contains(row.geometry):
                    print(f' - tripple winner ')
                    results.append([row.name, 'triplet',
                                    None,
                                    ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                    continue

        # we have more than two epsg zones OR the tiles within one single epsg zone are not enough
        # we check all double combinations - first wins
        lS2tiles = s2_aoi.tile_id.unique().tolist()
        lCombi = list(itertools.combinations(lS2tiles, 2))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
            double_aoi = unary_union(s2_sub.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner ')
                results.append([row.name, 'double',
                                None,
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # we check all tripple combinations - first wins
        lCombi = list(itertools.combinations(lS2tiles, 3))

        iDone = False
        for element in lCombi:
            s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
            triple_aoi = unary_union(s2_sub.geometry.tolist())
            if triple_aoi.contains(row.geometry):
                print(f' - triple winner ')
                results.append([row.name, 'triplet',
                                None,
                                ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                iDone = True
                break
        if iDone:
            continue

        # damm we really need all four S2 tiles - a quadruple
        results.append([row.name, 'quadruple',
                        None,
                        ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])
        continue

    # now we deal with the rest - we should have not more than quadruples per epsg zone

    # do we have multiple epsg zones in the result
    epsg_test = s2_aoi.epsg.unique().tolist()

    # if we have TWO epsg zones we check separatly
    if len(epsg_test) == 2:
        # all tests for the first epsg zone
        s2_sub_a = s2_aoi[s2_aoi.epsg == epsg_test[0]].copy()

        # now we have to check if we have only 2 tiles in the epsg filter or three
        if s2_sub_a.shape[0] == 2:

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub_a.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner ')
                results.append([row.name, 'double',
                                None,
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        if s2_sub_a.shape[0] == 3:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_a.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check if the triplet can cover the areas
            tripple_aoi = unary_union(s2_sub_a.geometry.tolist())
            if tripple_aoi.contains(row.geometry):
                print(f' - tripple winner ')
                results.append([row.name, 'triplet',
                                None,
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        # check quadruple
        if s2_sub_a.shape[0] == 4:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_a.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check all tripple combinations - first wins
            lCombi = list(itertools.combinations(lS2tiles, 3))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                triple_aoi = unary_union(s2_sub.geometry.tolist())
                if triple_aoi.contains(row.geometry):
                    print(f' - triple winner ')
                    results.append([row.name, 'triplet',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            quad_aoi = unary_union(s2_sub_a.geometry.tolist())
            if quad_aoi.contains(row.geometry):
                print(f' - quadruple winner ')
                results.append([row.name, 'quadruple',
                                None,
                                ",".join(str(element) for element in s2_sub_a.tile_id.unique().tolist())])
                continue

        # all tests for the second epsg zone
        s2_sub_b = s2_aoi[s2_aoi.epsg == epsg_test[1]].copy()

        # now we have to check if we have only 2 tiles in the epsg filter or three
        if s2_sub_b.shape[0] == 2:

            # now we have only the two tileIDs with the same epsg number
            double_aoi = unary_union(s2_sub_b.geometry.tolist())
            if double_aoi.contains(row.geometry):
                print(f' - double winner ')
                results.append([row.name, 'double',
                                None,
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue

        if s2_sub_b.shape[0] == 3:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_b.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check if the triplet can cover the areas
            tripple_aoi = unary_union(s2_sub_b.geometry.tolist())
            if tripple_aoi.contains(row.geometry):
                print(f' - tripple winner ')
                results.append([row.name, 'triplet',
                                None,
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue

        # check quadruple
        if s2_sub_b.shape[0] == 4:
            # we check all double combinations - first wins
            lS2tiles = s2_sub_b.tile_id.unique().tolist()
            lCombi = list(itertools.combinations(lS2tiles, 2))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                double_aoi = unary_union(s2_sub.geometry.tolist())
                if double_aoi.contains(row.geometry):
                    print(f' - double winner ')
                    results.append([row.name, 'double',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            # we check all tripple combinations - first wins
            lCombi = list(itertools.combinations(lS2tiles, 3))

            iDone = False
            for element in lCombi:
                s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
                triple_aoi = unary_union(s2_sub.geometry.tolist())
                if triple_aoi.contains(row.geometry):
                    print(f' - triple winner ')
                    results.append([row.name, 'triplet',
                                    None,
                                    ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
                    iDone = True
                    break
            if iDone:
                continue

            quad_aoi = unary_union(s2_sub_b.geometry.tolist())
            if quad_aoi.contains(row.geometry):
                print(f' - quadruple winner ')
                results.append([row.name, 'quadruple',
                                None,
                                ",".join(str(element) for element in s2_sub_b.tile_id.unique().tolist())])
                continue


    # we check all double combinations - first wins
    lS2tiles = s2_aoi.tile_id.unique().tolist()
    lCombi = list(itertools.combinations(lS2tiles, 2))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
        double_aoi = unary_union(s2_sub.geometry.tolist())
        if double_aoi.contains(row.geometry):
            print(f' - double winner ')
            results.append([row.name, 'double',
                            None,
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # we check all tripple combinations - first wins
    lCombi = list(itertools.combinations(lS2tiles, 3))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
        triple_aoi = unary_union(s2_sub.geometry.tolist())
        if triple_aoi.contains(row.geometry):
            print(f' - triple winner ')
            results.append([row.name, 'triplet',
                            None,
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # we check all quadruple combinations - first wins
    lCombi = list(itertools.combinations(lS2tiles, 4))

    iDone = False
    for element in lCombi:
        s2_sub = s2_aoi[s2_aoi['tile_id'].isin(list(element))].copy()
        triple_aoi = unary_union(s2_sub.geometry.tolist())
        if triple_aoi.contains(row.geometry):
            print(f' - quadruple winner ')
            results.append([row.name, 'quadruple',
                            None,
                            ",".join(str(element) for element in s2_sub.tile_id.unique().tolist())])
            iDone = True
            break
    if iDone:
        continue

    # fuck - still no winner..... I already tested too much and made it way to complicated.... just put all in
    print(f' - multiple winner - use all tileid')
    results.append([row.name, 'multiple - write filter by hand',
                    None, ",".join(str(element) for element in s2_aoi.tile_id.unique().tolist())])

# merge the results and write out
df_result = pd.DataFrame(results, columns=['laea_tileid', 'match', 's2_tileid', 's2_multi_list'])

if gdf_laea.shape[0] != df_result.shape[0]:
    print(' -- error: we have a mismatch between number of LAEA grids and the number of results')

gdf_result = gdf_laea.merge(df_result, how='left', left_on='name', right_on='laea_tileid')

#write out
gdf_result.to_file(path_out)