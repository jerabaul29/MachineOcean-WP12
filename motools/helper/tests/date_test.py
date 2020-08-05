"""tests"""

import datetime
from motools.helper.date import date_range


def test_step_1():
    """test with default range."""
    start = datetime.date(2020, 1, 2)
    end = datetime.date(2020, 1, 4)
    answer = [datetime.date(2020, 1, 2),
              datetime.date(2020, 1, 3)]

    assert list(date_range(start, end)) == answer


def test_step_2():
    """test with custom range"""
    start = datetime.date(2020, 1, 2)
    end = datetime.date(2020, 1, 5)
    answer = [datetime.date(2020, 1, 2),
              datetime.date(2020, 1, 4)]

    assert list(date_range(start, end, 2)) == answer
