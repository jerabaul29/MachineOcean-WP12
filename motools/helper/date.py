"""Some helpers for working with dates."""

import datetime
from datetime import timedelta


def date_range(start, end, step_days=1):
    """Yield dates in the range [start, end[, with step
    step_days (default is 1 day).

    Input:
        - start: the first date to yield
        - end_ the last date to yield, not included (follow python conventions)
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
