"""Helper functions to read storm surge data from the kyststasjoner_norge nc files."""

from datetime import date
import datetime
import os

import numpy as np
import matplotlib.pyplot as plt
import netCDF4 as nc4

import motools.config as moc
from motools.helper import arrays as moa

lat_23_coast_stations = \
    np.array([59.05473, 59.683765, 59.683765, 59.029335, 58.02723, 59.01525,
              60.434822, 61.93313, 62.458206, 63.16086, 63.444027, 63.45151,
              64.78673, 67.3244, 68.451836, 68.19022, 69.32801, 68.813736,
              69.722496, 70.64632, 70.97274, 70.33625, 78.92994])

lon_23_coast_stations = \
    np.array([10.92997, 10.632921, 10.632921, 9.84748, 7.564182,
              5.746805, 5.1184344, 5.1168604, 6.082075, 7.7331095,
              9.124409, 10.455196, 11.231044, 14.360688, 17.376715,
              14.486947, 16.06782, 16.649126, 19.036953, 23.619656,
              25.970213, 31.086498, 12.014665])

assert lat_23_coast_stations.shape == lon_23_coast_stations.shape, \
    ("lat and lon arrays for the 23 coast stations should have same size")


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
    assert run_time in admissible_run_time, "runs at 00 and 12"

    assert type(datetime_day) is date, "take a datetime.date"

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


def get_kyststasjoner_data(path_to_nc, model_field="stormsurge", inspect=False):
    """Get the relevant training data from the nc file at location path_to_nc.

    Input:
        - path_to_nc: the path to the nc storm surge file to read.
        - model_field: which model field to use. There are two options: 1) "stormsurge",
            in which case the output is the storm surge effect due to wind and pressure,
            without tide or observation-based correction, and 2) "totalwater", which
            includes effect of wind, pressure, tide, and observation-correction. See
            discussion from issue #6 on the MO WP12 repo and the comments by NilsMK.
        - inspect: boolean, decide if should display the data generated for inspection
            or not.

    Output:
        - (unix_time, tide, obs_notide, model_mean_notide, model_std_notide, model_members_notide)
        tuple containing the data, where the elements are:
            - unix_time: the epoch time
            - tide: the tide prediction
            - obs_notide: the observation at the stations, without the tide
            - model_mean_notide: the model outuput at the stations, without
                the tide, averaged over ensemble members.
            - model_std_notide: the standard deviation of the model output at the stations,
                based on the ensemble members.
            - model_members_notide: the model output at the stations, without the tide.
                for each ensemble member

        tide, obs_notide, model_mean_notide, model_std_notide have a dimension 2. First index is time, second index is
        station.

        model_members_notide has a dimension 3. First index is time, second is ensemble member, third is station.

        The arrays returned are numpy normal arrays, not masked arrays as present
        in some old files. The config fill value is used for filling masked values, similar
        to what is used in recent files.
    """

    assert os.path.isfile(path_to_nc), ("invalid path to nc file: {}".format(path_to_nc))
    assert model_field in ["stormsurge", "totalwater"], "only stormsurge and totalwater are valid args."
    assert isinstance(inspect, bool), "inspect must be a bool."

    nc_content = nc4.Dataset(path_to_nc, 'r')

    datafield_model = model_field
    datafield_stations = "observed"
    datafield_tide = "tide"

    nbr_ensemble = 52
    dummy = 0

    nbr_stations = 23
    nbr_stations_updated = 26

    nbr_time_values = 121

    nc_water_model = nc_content[datafield_model][:]
    nc_water_station = nc_content[datafield_stations][:]
    nc_water_tide = nc_content[datafield_tide][:]

    nc_time = nc_content["time"][:]
    unix_time = nc_time
    nc_datetime = [datetime.datetime.fromtimestamp(crrt_time) for crrt_time in nc_time]

    assert nc_water_model.shape[1] == nbr_ensemble, "ensemble size"

    # NOTE: the number of stations went from 23 to 26 in early 2020.
    assert nc_water_model.shape[3] == nbr_stations or nc_water_model.shape[3] == nbr_stations_updated, "number of stations"

    assert nc_water_model.shape[0] == nbr_time_values, "number of time values"

    # check that the stations are still in the same order
    assert np.allclose(lat_23_coast_stations, nc_content["latitude"][dummy][:23]), ("make sure the stations are always in same order; compare {} and {}".format(lat_23_coast_stations, nc_content["latitude"][dummy][:23]))
    assert np.allclose(lon_23_coast_stations, nc_content["longitude"][dummy][:23]), ("make sure the stations are always in same order, compare {} and {}".format(lon_23_coast_stations, nc_content["longitude"][dummy][:23]))

    # in all the following, we want to ignore the effect of the tide, which is well described by
    # the tide models

    # water level at the measurement stations from observations, when tide effect is subtracted
    nc_water_station_notide = nc_water_station - nc_water_tide

    nc_water_model_mean = np.mean(nc_water_model, axis=1)
    nc_water_model_notide = np.zeros(nc_water_model.shape)

    if datafield_model == "totalwater":
        nc_water_model_mean_notide = nc_water_model_mean - nc_water_tide
        for crrt_member in range(nbr_ensemble):
            nc_water_model_notide[:, crrt_member, dummy, :] = nc_water_model[:, crrt_member, dummy, :] - nc_water_tide[:, dummy, :]
    elif datafield_model == "stormsurge":
        nc_water_model_mean_notide = nc_water_model_mean
        nc_water_model_notide = nc_water_model

    # water level at the measurement stations from model, tide effect subtracted, ensemble std
    nc_water_model_std = np.std(nc_water_model, axis=1)

    # depending on the date of production of the nc datasets, data is either numpy array or masked array; turn all to numpy array
    # remove singleton dimensions
    tide = moa.masked_array_to_filled_array(np.squeeze(nc_water_tide[:, dummy, :]))
    obs_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_station_notide[:, dummy, :]))
    model_mean_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_model_mean_notide[:, dummy, :]))
    model_std_notide = moa.masked_array_to_filled_array(np.squeeze(nc_water_model_std[:, dummy, :]))
    model_members_notide = moa.masked_array_to_filled_array((np.squeeze(nc_water_model_notide[:, :, dummy, :])))

    if inspect:
        # show the ensemble data for each station
        for crrt_station in range(nbr_stations):
            fig, ax = plt.subplots()

            for ensemble_nbr in range(nbr_ensemble):
                plt.plot(nc_datetime, model_members_notide[:, ensemble_nbr, crrt_station],
                        color='k', linewidth=0.5, label="member {}".format(ensemble_nbr))

            plt.plot(nc_datetime, model_mean_notide[:, crrt_station],
                    label="model mean no tide, mean, 3 std",
                    linewidth=2.5, color="blue")

            ax.fill_between(nc_datetime,
                            model_mean_notide[:, crrt_station] - 3 * model_std_notide[:, crrt_station],
                            model_mean_notide[:, crrt_station] + 3* model_std_notide[:, crrt_station],
                            alpha=0.2,
                            color="blue")

            plt.plot(nc_datetime, obs_notide[:, crrt_station],
                    label="obs notide", color='r',
                    linewidth=2.5)

            plt.legend()
            plt.ylabel("notide storm surge, station {}".format(crrt_station))

        # show, for all stations, the error between observations vs. predictions, in notide fasion
        fig, ax = plt.subplots()

        for station in range(nbr_stations):
            plt.plot(nc_datetime, model_mean_notide[:, station] - obs_notide[:, station],
                    label="station {}".format(station))

        plt.legend()
        plt.ylabel("model error from {}".format(datafield_model))

        plt.show()

    return(unix_time,
           tide,
           obs_notide,
           model_mean_notide,
           model_std_notide,
           model_members_notide)
