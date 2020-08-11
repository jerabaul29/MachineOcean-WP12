"""Some helpers for working with dates."""

import datetime
from datetime import timedelta
from motools import logger


def date_to_datetime(date, aware=True):
    """Convert a date to the datetime corresponding to the start of the day.

    Input:
        - date: a datetime.date
        - aware: wether the resulting datetime should be time zone aware (True) or not
            (False). Default is True.

    Output:
        - datetime: a time zone aware datetime.datetime
    """

    if not isinstance(date, datetime.date):
        raise ValueError("Expected type {}, got {}".format(datetime.date, type(date)))

    if aware:
        return(datetime.datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=datetime.timezone.utc))
    else:
        return(datetime.datetime(date.year, date.month, date.day, 0, 0, 0))

def datetime_range(start, end, step_timedelta):
    """Yield datetimes in the range [start, end[, with step
    step_timedelta.

    Input:
        - start: the first datetime to yield
        - end: the last datetime to yield, not included (follow python conventions)
        - step_timedelta: the timedelta between yielded values

    Note: this is a generator.
    """

    if not isinstance(start, datetime.datetime):
        raise ValueError("start must be a datetime, received {} with type {}".format(start, type(start)))
    if not isinstance(end, datetime.datetime):
        raise ValueError("end must be a datetime, received {} with type {}".format(start, type(end)))
    if not isinstance(step_timedelta, datetime.timedelta):
        raise ValueError("step_timedelta must be a timedelta, received {} with type {}".format(step_timedelta, type(step_timedelta)))
    if not (end > start):
        raise ValueError("should have end > start, got {} - {}".format(start, end))
    if not step_timedelta > datetime.timedelta(seconds=0):
        raise ValueError("should have a position timedelta")

    crrt_time = start
    yield crrt_time

    while True:
        crrt_time += step_timedelta
        if crrt_time < end:
            yield crrt_time
        else:
            break

def date_range(start, end, step_days=1):
    """Yield dates in the range [start, end[, with step
    step_days (default is 1 day).

    Input:
        - start: the first date to yield
        - end: the last date to yield, not included (follow python conventions)
        - step_days: number of days between each yield date,
            int.

    Note: this is a generator."""

    if not (isinstance(start, datetime.date)):
        raise ValueError("start must be a date, received {} with type {}".format(start, type(start)))
    if not (isinstance(end, datetime.date)):
        raise ValueError("end must be a date, received {} with type {}".format(end, type(end)))
    if not (isinstance(step_days, int) and step_days > 0):
        raise ValueError("step must be a positive integer, received {} with type {}".format(step_days, type(step_days)))

    span = (end - start).days

    if not span > 0:
        raise ValueError("number of days spanned is {}".format(span))

    for i in range(0, span, step_days):
        yield start + timedelta(days=i)


def find_dropouts(list_datetimes, delta_time_s, behavior="raise_exception"):
    """Find dropouts in a list of datetimes. Can either print a warning
    or raise an exception. Returns the position where the dropouts take
    place.

    Input:
        - list_datetimes: the list to check.
        - delta_time_s: the delta time expected, in seconds, integer.
        - behavior: either warning (print a warning), or raise_exception (raise an exception).

    Output:
        - list_dropout_after_indexes: the list of indexes after which there is dropout.
    """

    if not isinstance(delta_time_s, int):
        raise ValueError("delta_time_s shoud be an integer, received {}".format(delta_time_s))

    if not isinstance(list_datetimes, list):
        raise ValueError("list_datetimes should be a list, received {}".format(list_datetimes))

    if behavior not in ["warning", "raise_exception"]:
        raise ValueError("behavior should be either warning or raise_exception, got {}".format(behavior))

    list_dropout_after_indexes = []

    for crrt_ind in range(len(list_datetimes)-1):
        delta_time_next_point = (list_datetimes[crrt_ind+1] - list_datetimes[crrt_ind]).seconds

        if delta_time_next_point != delta_time_s:
            list_dropout_after_indexes.append(crrt_ind)

            if behavior == "warning":
                logger.warning("detected dropout")
            else:
                raise ValueError("detected dropout")

    return list_dropout_after_indexes
