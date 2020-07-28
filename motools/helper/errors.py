"""Some helper functions for tracking errors."""

import sys
import traceback


def detailed_assert_repr(assert_except):
    assert isinstance(assert_except, AssertionError), ("non assert error, use 'detailed_assert_repr' after an 'except AssertionError as e:'")

    print(repr(assert_except))
    _, _, tb = sys.exc_info()
    traceback.print_tb(tb) # Fixed format
