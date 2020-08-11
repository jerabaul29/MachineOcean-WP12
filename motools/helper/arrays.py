"""Some helpers for working with arrays."""

import numpy as np
import motools.config as moc


mo_config = moc.Config()
fill_value = mo_config.getSetting("params", "fillValue")


def masked_array_to_filled_array(array_in, fill_value=fill_value):
    """If array_in is a masked array, convert to a normal array
    applying the fill_value."""

    assert isinstance(array_in, (np.ndarray, np.ma.MaskedArray)), ("array_in must be a numpy array or masked array, but received {}".format(type(array_in)))

    if isinstance(array_in, np.ma.MaskedArray):
        array_in.fill_value = fill_value
        array_in = np.ma.filled(array_in)

    return(array_in)


def check_strict_monotonic(array, list_dimensions=None):
    """Check that an array is strictly monotonic. Raise a
    ValueError if not.

    Input:
        array: a numpy array of any dimension.
        list_dimensions: the list of dimensions on which to do
            the check. Check all dimensions if None (default).

    Output: None.

    Can raise:
        a ValueError indicating the first non monotonic dimension.
    """

    if list_dimensions is None:
        n_dim = len(np.shape(array))
        list_dimensions = range(n_dim)
    else:
        assert isinstance(list_dimensions, list)

    for dim in list_dimensions:
        dim_diff = np.diff(array, axis=dim)
        if not (np.all(dim_diff < 0) or np.all(dim_diff > 0)):
            raise ValueError("Array non stricly monotonic on dim {}".format(dim))


def index_ranges_within_bounds(x_coords, y_coords, x_bounds, y_bounds, comp_epsilon=1e-12):
    """Select the index ranges on the x and y directions that contain all data having
    x and y coordinates with prescribed ranges.

    Input:
        x_coords: the 2D array containing the x coordinate of the points
        y_coords: idem, y coordinate
        x_bounds=[min_x, max_x]: the min and max desired values for x coordinates
        y_bounds: idem, y coordinate
        comp_epsilon: tolerance for performing floating comparisons

    Output:
        (lower_0_index, upper_0_index, lower_1_index, upper_1_index):
            the lower and upper bounds for ranges on the 0 and 1 axis indexes so that all data
            that have coordinates within the prescribed x and y bounds are captured.

            Note: these are Python bounds, to be used with the convention [lower:upper[,
            for example for index slicing.

    Can raise:
        This can raise an IndexError in case there are no points having coordinates
        within the prescsribed bounds.
    """

    assert x_bounds[0] < x_bounds[1]
    assert y_bounds[0] < y_bounds[1]
    check_strict_monotonic(x_coords, [1])
    check_strict_monotonic(y_coords, [0])

    valid_x_locs = np.logical_and(x_coords + comp_epsilon >= x_bounds[0],
                                  x_coords <= x_bounds[1] + comp_epsilon)

    valid_y_locs = np.logical_and(y_coords + comp_epsilon >= y_bounds[0],
                                  y_coords <= y_bounds[1] + comp_epsilon)

    valid_x_y_locs = np.logical_and(valid_x_locs,
                                    valid_y_locs)

    if not np.any(valid_x_y_locs):
        raise IndexError("There are no points within the x y range prescrived.")

    index_valid_range_0 = np.where(np.any(valid_x_y_locs, axis=1))
    index_valid_range_1 = np.where(np.any(valid_x_y_locs, axis=0))

    # Note: +1 on the maxima because these should be bounds for ranges,
    # i.e. [low_bound, upper_bound+1[ in python
    return(index_valid_range_0[0][0], index_valid_range_0[0][-1] + 1,
           index_valid_range_1[0][0], index_valid_range_1[0][-1] + 1
           )


def find_index_first_greater_or_equal(np_array, value):
    """Find the index of the first value of np_array that is
    greater or equal than value.

    Input:
        - np_array: numpy array in which to look, should
            be monotonic.
        - value: the value to use as a reference for
            comparison.

    Output:
        - the index of the first entry that is greater
            or equal to value.
    If no valid value, raise an error.
    """

    if not isinstance(np_array, np.ndarray):
        raise ValueError("np_array must be a numpy array")

    check_strict_monotonic(np_array)

    if np_array[-1] < value:
        raise ValueError("no entry greater or equal than the asked value")

    first_index = np.argmax(np_array >= value)

    return first_index
