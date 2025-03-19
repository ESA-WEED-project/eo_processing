"""
here we list the possible temporal aggregator possibilities for the S1/S2 pipelines
defined my aggregation method for a chosen temporal interval

IMPORTANT: if we want to run dekad-sliding THEN there must be a check that we adapt the start and end date of the requestede
datacube to the 20-day window min and max date!
- check if we will do that also for season and tropical-season?


IMPORTANT2: also run a pretest that the temporal interval is one of the possible options for the aggregator
(e.g. max-ndvi) which is not supporting the dekad-sliding option yet!

still a MOC and has to be cleaned up

"""

from typing import Union
from datetime import timedelta
from datetime import datetime
import openeo
from typing import List, Tuple

INTERVAL_OPTIONS = ['day', 'quintad', 'week', 'dekad', 'dekad-sliding', 'month', 'season', 'tropical-season', 'year']

TEMP_AGGREGATOR_OPTIONS = ['median', 'mean', 'sum', 'max-ndvi', 'bap-score']

def temporal_aggregation(cube: openeo.DataCube, period: str, aggregator: str, start: str, end: str) -> openeo.DataCube:
    """
    That is the entrance function for the temporal aggregation.

    here all has to be handled when the request comes from the S1 or S2 pre-processing function

    NOte: run a check that the keyword is one of the possible options (Interval and aggregator).... otherwise error.... this check should be
          also implemented in the processing_option check!

    """

    # check that we have valid period (in case that was set to None or False by mistake)
    if (period is None) or (period == False) or (period == 'none'):
        period = 'dekad'
    elif period not in INTERVAL_OPTIONS:
        raise ValueError(f"Invalid temporal interval option: {period}")

    if (aggregator == 'none') or (aggregator == False) or (aggregator is None):
        return cube
    elif aggregator not in TEMP_AGGREGATOR_OPTIONS:
        raise ValueError(f"Invalid temporal aggregation option: {aggregator}")

    # here we have to get the period lists if we have quintad or dekad-sliding
    # NOTE: instead of a keyword we have now date intervals > use aggregate_temporal instead of aggregate_temporal_period
    if period == 'quintad':
        period = quintad_intervals(start, end)
    elif period == 'dekad-sliding':
        period = get_20_day_window_extents(start, end)




    # go into the cases
    if aggregator == 'median':
        cube = median_compositing(cube, period)
    elif aggregator == 'mean':
        cube = mean_compositing(cube, period)
    elif aggregator == 'sum':
        cube = sum_compositing(cube, period)
    elif aggregator == 'max-ndvi':
        cube = max_ndvi_compositing(cube, period)
    elif aggregator == 'bap-score':
        print('bap-score not yet implemented')


    return cube


def median_compositing(
    cube: openeo.DataCube, period: Union[str, list]
) -> openeo.DataCube:
    """Perfrom median compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="median", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(
            intervals=period, reducer="median", dimension="t"
        )


def mean_compositing(
    cube: openeo.DataCube, period: Union[str, list]
) -> openeo.DataCube:
    """Perfrom mean compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="mean", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="mean", dimension="t")


def sum_compositing(cube: openeo.DataCube, period: Union[str, list]) -> openeo.DataCube:
    """Perform sum compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="sum", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="sum", dimension="t")


def max_ndvi_compositing(cube: openeo.DataCube, period: str) -> openeo.DataCube:
    """Perform compositing by selecting the observation with the highest NDVI value over the
    given compositing window."""

    def max_ndvi_selection(ndvi: openeo.DataCube):
        max_ndvi = ndvi.max()
        return ndvi.array_apply(lambda x: x != max_ndvi)

    if isinstance(period, str):
        ndvi = cube.ndvi(nir="S2-L2A-B08", red="S2-L2A-B04")

        rank_mask = ndvi.apply_neighborhood(
            max_ndvi_selection,
            size=[
                {"dimension": "x", "unit": "px", "value": 1},
                {"dimension": "y", "unit": "px", "value": 1},
                {"dimension": "t", "value": period},
            ],
            overlap=[],
        )

        cube = cube.mask(mask=rank_mask).aggregate_temporal_period(period, "first")

    else:
        raise ValueError(
            "Custom temporal intervals are not yet supported for max NDVI compositing."
        )
    return cube

def quintad_intervals(start, end) -> list:
    """Returns a list of tuples (start_date, end_date) of quintad intervals
    from the input temporal extent. Quintad intervals are intervals of
    generally 5 days, that never overlap two months.

    All months are divided in 6 quintads, where the 6th quintad might
    contain 6 days for months of 31 days.
    For the month of February, the 6th quintad is only of three days, or
    four days for the leap year.
    """
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    quintads = []

    current_date = start_date

    # Compute the offset of the first day on the start of the last quintad
    if start_date.day != 1:
        offset = (start_date - timedelta(days=1)).day % 5
        current_date = current_date - timedelta(days=offset)
    else:
        offset = 0

    while current_date <= end_date:
        # Get the last day of the current month
        last_day = current_date.replace(day=28) + timedelta(days=4)
        last_day = last_day - timedelta(days=last_day.day)

        # Get the last day of the current quintad
        last_quintad = current_date + timedelta(days=4)

        # Add a day if the day is the 30th and there is the 31th in the current month
        if last_quintad.day == 30 and last_day.day == 31:
            last_quintad = last_quintad + timedelta(days=1)

        # If the last quintad is after the last day of the month, then
        # set it to the last day of the month
        if last_quintad > last_day:
            last_quintad = last_day
        # In the case the last quintad is after the end date, then set it to the end date
        elif last_quintad > end_date:
            last_quintad = end_date

        quintads.append((current_date, last_quintad))

        # Set the current date to the next quintad
        current_date = last_quintad + timedelta(days=1)

    # Fixing the offset issue for intervals starting in the middle of a quintad
    quintads[0] = (quintads[0][0] + timedelta(days=offset), quintads[0][1])

    # Returns to string with the YYYY-mm-dd format
    return [
        (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        for start_date, end_date in quintads
    ]

def get_20_day_window_extents(
    start, end, window_size: int = 20, window_step: int = 10
) -> List[Tuple[str, str]]:
    """
    For a whole year, return the start and end dates of 20-day windows for each 10-day period.
    """

    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")


    current_date = start_date + timedelta(days=window_size // 2)

    window_extents = []
    while current_date < end_date:
        start_date_window = current_date - timedelta(days=window_size // 2)
        end_date_window = start_date_window + timedelta(
            days=window_size - 1
        )  # TODO check if -1 is needed
        end_date_window = min(end_date_window, end_date)

        window_extents.append(
            (start_date_window.isoformat(), end_date_window.isoformat())
        )

        current_date += timedelta(days=window_step)
    return window_extents
