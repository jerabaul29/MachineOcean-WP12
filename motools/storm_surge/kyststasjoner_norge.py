"""Helper functions to read storm surge data from the kyststasjoner_norge nc files."""

import netCDF4 as nc4
import numpy as np
# import motools.config as moc
from datetime import date
import os


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
        # mo_config = moc.Config()
        # TODO: FIXME with the updated config setup
        basepath = "/lustre/storeB/project/fou/hi/stormsurge_eps/2dEPS_archive"
        basepath += "/"
    else:
        raise RuntimeError("only supporting basepath from mo config for now")

    nc_path = basepath + \
        "kyststasjoner_norge.nc{}{:02}{:02}{}".format(datetime_day.year,
                                                      datetime_day.month,
                                                      datetime_day.day,
                                                      run_time)

    return(nc_path)


def get_kyststasjoner_average_data(path_to_nc):
    """Get the relevant training data from the nc file at location path_to_nc.

    Input:
        - path_to_nc: the path to the nc storm surge file to read.

    Output:
        - (nc_water_station_notide, nc_water_model_mean_notide, nc_water_model_std)
        tuple containing the data, where the elements are:
            - nc_water_station_notide: the observation at the stations, corrected for the tide
            - nc_water_model_mean_notide: the model outuput at the stations, corrected
                for the tide, averaged over ensemble members.
            - nc_water_model_std: the standard deviation of the model output at the stations,
                based on the ensemble members.

        All the output fields have a dimension 2. First index is time, second index is
        station.
    """
    assert os.path.isfile(path_to_nc)

    nc_content = nc4.Dataset(path_to_nc, 'r')

    datafield_model = "totalwater"
    datafield_stations = "observed"
    datafield_tide = "tide"

    nbr_ensemble = 52
    nbr_stations = 23
    nbr_stations_updated = 26

    nc_water_model = nc_content[datafield_model][:]
    nc_water_station = nc_content[datafield_stations][:]
    nc_water_tide = nc_content[datafield_tide][:]

    assert nc_water_model.shape[1] == nbr_ensemble

    # NOTE: the number of stations went from 23 to 26 in early 2020.
    assert nc_water_model.shape[3] == nbr_stations or nc_water_model.shape[3] == nbr_stations_updated

    # water level at the measurement stations from observations, when tide effect is subtracted
    nc_water_station_notide = nc_water_station - nc_water_tide

    nc_water_model_mean = np.mean(nc_water_model, axis=1)

    # water level at the measurement stations from model, tide effect subtracted, ensemble average
    nc_water_model_mean_notide = nc_water_model_mean - nc_water_tide

    # water level at the measurement stations from model, tide effect subtracted, ensemble std
    nc_water_model_std = np.std(nc_water_model, axis=1)

    return(np.squeeze(nc_water_station_notide[:, 0, :]),
           np.squeeze(nc_water_model_mean_notide[:, 0, :]),
           np.squeeze(nc_water_model_std[:, 0, :]))
