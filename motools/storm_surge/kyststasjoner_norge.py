"""Helper functions to read storm surge data from the kyststasjoner_norge nc files."""

import netCDF4 as nc4
import numpy as np


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
    nc_content = nc4.Dataset(path_to_nc, 'r')

    datafield_model = "totalwater"
    datafield_stations = "observed"
    datafield_tide = "tide"

    nbr_ensemble = 52
    nbr_stations = 23

    nc_water_model = nc_content[datafield_model][:]
    nc_water_station = nc_content[datafield_stations][:]
    nc_water_tide = nc_content[datafield_tide][:]

    assert nc_water_model.shape[1] == nbr_ensemble
    assert nc_water_model.shape[3] == nbr_stations

    # water level at the measurement stations from observations, when tide effect is subtracted
    nc_water_station_notide = nc_water_station - nc_water_tide

    nc_water_model_mean = np.mean(nc_water_model, axis=1)

    # water level at the measurement stations from model, tide effect subtracted, ensemble average
    nc_water_model_mean_notide = nc_water_model_mean - nc_water_tide

    # water level at the measurement stations from model, tide effect subtracted, ensemble std
    nc_water_model_std = np.std(nc_water_model, axis=1)

    return(nc_water_station_notide[:, 0, :],
           nc_water_model_mean_notide[:, 0, :],
           nc_water_model_std[:, 0, :])
