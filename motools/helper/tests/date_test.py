"""tests"""

import datetime
from motools.helper.date import date_range, datetime_range, find_dropouts


def test_date_range_step_1():
    """test with default range."""
    start = datetime.date(2020, 1, 2)
    end = datetime.date(2020, 1, 4)
    answer = [datetime.date(2020, 1, 2),
              datetime.date(2020, 1, 3)]

    assert list(date_range(start, end)) == answer


def test_date_range_step_2():
    """test with custom range"""
    start = datetime.date(2020, 1, 2)
    end = datetime.date(2020, 1, 5)
    answer = [datetime.date(2020, 1, 2),
              datetime.date(2020, 1, 4)]

    assert list(date_range(start, end, 2)) == answer


def test_datetime_range_1():
    start = datetime.datetime(2020, 1, 2, 23, 10, 00)
    end = datetime.datetime(2020, 1, 2, 23, 50, 10)
    step_timedelta = datetime.timedelta(minutes=10)
    answer = [datetime.datetime(2020, 1, 2, 23, 10, 00),
              datetime.datetime(2020, 1, 2, 23, 20, 00),
              datetime.datetime(2020, 1, 2, 23, 30, 00),
              datetime.datetime(2020, 1, 2, 23, 40, 00),
              datetime.datetime(2020, 1, 2, 23, 50, 00),
              ]

    assert list(datetime_range(start, end, step_timedelta)) == answer


def test_datetime_range_2():
    start = datetime.datetime(2020, 1, 2, 23, 10, 00)
    end = datetime.datetime(2020, 1, 2, 23, 50, 00)
    step_timedelta = datetime.timedelta(minutes=10)
    answer = [datetime.datetime(2020, 1, 2, 23, 10, 00),
              datetime.datetime(2020, 1, 2, 23, 20, 00),
              datetime.datetime(2020, 1, 2, 23, 30, 00),
              datetime.datetime(2020, 1, 2, 23, 40, 00),
              ]

    assert list(datetime_range(start, end, step_timedelta)) == answer


def test_find_dropouts_1():
    list_datetimes = [datetime.datetime(2020, 1, 2, 23, 10, 00),
                      datetime.datetime(2020, 1, 2, 23, 20, 00),
                      datetime.datetime(2020, 1, 2, 23, 30, 00),
                      datetime.datetime(2020, 1, 2, 23, 40, 00),
                      ]

    time_resolution_s = 10*60

    assert find_dropouts(list_datetimes, time_resolution_s) == []


def test_find_dropouts_2():
    list_datetimes = [datetime.datetime(2020, 1, 2, 23, 10, 00),
                      datetime.datetime(2020, 1, 2, 23, 20, 00),
                      datetime.datetime(2020, 1, 2, 23, 30, 00),
                      datetime.datetime(2020, 1, 2, 23, 41, 00),
                      ]

    time_resolution_s = 10*60

    assert find_dropouts(list_datetimes, time_resolution_s, behavior="warning") == [2]
