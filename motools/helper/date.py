"""Some helpers for working with dates."""

import datetime
from datetime import timedelta


def datetime_range(start, end):
    """Yield dates in the range [start, end].

    Input:
        - start: the first date to yield
        - end_ the last date to yield, INCLUDED

    Note: this is a generator."""

    assert isinstance(start, datetime.date), "start must be a date, received {} with type {}".format(start, type(start))
    assert isinstance(end, datetime.date), "end must be a date, received {} with type {}".format(end, type(end))

    span = end - start
    for i in range(span.days + 1):
        yield start + timedelta(days=i)
