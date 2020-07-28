"""Helper functions to read storm surge data from the kyststasjoner_norge nc files."""

import netCDF4 as nc4
import numpy as np
import motools.config as moc
from datetime import date
import os
from motools.helper import arrays as moa


def kyststasjoner_path(datetime_day, run_time, basepath=None):
    """Provide the path to the storm surge nc file for the corresponding
    day and run time.

    Input:
        - datetime_day: date from datetime object of the day
        - run_time: the run time to consider, either 00 or 12
        - basepath: either the path to the folder containing the data if None,
            or a specific path if specified by the user.

    Output:
        - nc_path: the full path to the nc file. Note that this may be a non-existent
            path, for example if the time is outside of the interval when the model
            has been run, or the model was not run this specific date and time for
            whatever reason.
    """

    admissible_run_time = ["00", "12"]
    assert run_time in admissible_run_time

    assert type(datetime_day) is date

    if basepath is None:
        mo_config = moc.Config()
        basepath = mo_config.getSetting("data", "stormSurgeModelArchive")
        basepath += "/"
    else:
        raise RuntimeError("only supporting basepath from mo config for now")

    nc_path = basepath + \
        "kyststasjoner_norge.nc{}{:02}{:02}{}".format(datetime_day.year,
                                                      datetime_day.month,
                                                      datetime_day.day,
                                                      run_time)

    return(nc_path)


def get_kyststasjoner_data(path_to_nc):
    """Get the relevant training data from the nc file at location path_to_nc.

    Input:
        - path_to_nc: the path to the nc storm surge file to read.

    Output:
        - (nc_water_station_notide, nc_water_model_mean_notide, nc_water_model_std)
        tuple containing the data, where the elements are:
            - obs_notide: the observation at the stations, corrected for the tide
            - model_mean_notide: the model outuput at the stations, corrected
                for the tide, averaged over ensemble members.
            - model_std_notide: the standard deviation of the model output at the stations,
                based on the ensemble members.

        All the output fields have a dimension 2. First index is time, second index is
        station. The arrays returned are numpy normal arrays, not masked arrays as present
        in some old files. A value of 1.0e37 is used for filling masked values, similar
        to what is used in recent files.
    """

    assert os.path.isfile(path_to_nc), ("invalid path to nc file: {}".format(path_to_nc))

    nc_content = nc4.Dataset(path_to_nc, 'r')

    datafield_model = "totalwater"
    datafield_stations = "observed"
    datafield_tide = "tide"

    nbr_ensemble = 52

    nbr_stations = 23
    nbr_stations_updated = 26

    nbr_time_values = 121

    nc_water_model = nc_content[datafield_model][:]
    nc_water_station = nc_content[datafield_stations][:]
    nc_water_tide = nc_content[datafield_tide][:]

    assert nc_water_model.shape[1] == nbr_ensemble, "assert ensemble size"

    # NOTE: the number of stations went from 23 to 26 in early 2020.
    assert nc_water_model.shape[3] == nbr_stations or nc_water_model.shape[3] == nbr_stations_updated, "assert number of stations"

    assert nc_water_model.shape[0] == nbr_time_values, "assert number of time values"

    # water level at the measurement stations from observations, when tide effect is subtracted
    nc_water_station_notide = nc_water_station - nc_water_tide

    nc_water_model_mean = np.mean(nc_water_model, axis=1)

    # water level at the measurement stations from model, tide effect subtracted, ensemble average
    nc_water_model_mean_notide = nc_water_model_mean - nc_water_tide

    # water level at the measurement stations from model, tide effect subtracted, ensemble std
    nc_water_model_std = np.std(nc_water_model, axis=1)

    obs_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_station_notide[:, 0, :]))
    model_mean_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_model_mean_notide[:, 0, :]))
    model_std_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_model_std[:, 0, :]))

    return(obs_notide,
           model_mean_notide,
           model_std_notide)
