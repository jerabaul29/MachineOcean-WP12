"""Script to plot the storm surge model prediction vs actual measurements"""

import datetime
import netCDF4 as nc4
import numpy as np
import matplotlib.pyplot as plt

import motools

# get the data from the example file
folder = "../../example_data/StormSurge/NorwegianStations/"
path = folder + "kyststasjoner_norge.nc2019110700"

nc_content = nc4.Dataset(path, 'r')

nc_time = nc_content["time"][:]
nc_datetime = [datetime.datetime.fromtimestamp(crrt_time) for crrt_time in nc_time]

for date in nc_datetime:
    print(date)

# there are 2 potential candidates for taking data from the model; probably, the second one,
# which does not include the observation-based correction, would be more natural for doing
# some error learning.
# datafield_model = "totalwater" # storm surge + tide + correction
datafield_model = "stormsurge" # storm surge, no tide, no correction

# the observation from the field includes all effects
datafield_stations = "observed" # true value from the field

# as a first approximation, consider the tide as a separate, perfectly predicted quantity
datafield_tide = "tide" # the pure tide component

nbr_ensemble = 52 # number of members in the ensemble calculation
nbr_stations = 23 # the number of coast stations
dummy = 0 # dummy index
station_nbr = 6 # the station being considered

nc_water_model = nc_content[datafield_model][:]
nc_water_station = nc_content[datafield_stations][:]
nc_water_tide = nc_content[datafield_tide][:]

# in all the following, we want to ignore the effect of the tide, which is well described by
# the tide models
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
else:
    raise RuntimeError("datefield_model {} not supported".format(datafield_model))

nc_water_model_std = np.std(nc_water_model, axis=1)

nc_water_model_error = nc_water_station_notide - nc_water_model_mean_notide

# show, for 1 specific station, the observation vs. predictions, both in notide fashion
fig, ax = plt.subplots()

show_ensemble_simulations = True

if show_ensemble_simulations:
    for ensemble_nbr in range(nbr_ensemble):
        plt.plot(nc_datetime, nc_water_model_notide[:, ensemble_nbr, dummy, station_nbr],
                 color='k', linewidth=0.5, label="ens {}".format(ensemble_nbr))

plt.plot(nc_datetime, nc_water_model_mean_notide[:, dummy, station_nbr],
         label="{} no tide, mean, 3 std".format(datafield_model),
         linewidth=2.5, color="blue")

ax.fill_between(nc_datetime,
                nc_water_model_mean_notide[:, dummy, station_nbr] - 3 * nc_water_model_std[:, dummy, station_nbr],
                nc_water_model_mean_notide[:, dummy, station_nbr] + 3* nc_water_model_std[:, dummy, station_nbr],
                alpha=0.2,
                color="blue")

plt.plot(nc_datetime, nc_water_station_notide[:, dummy, station_nbr],
         label="{} - tide".format(datafield_stations), color='r',
         linewidth=2.5)

plt.legend()
plt.ylabel("notide storm surge, station {}".format(station_nbr))

plt.show()

# show, for all stations, the error between observations vs. predictions, in notide fasion
fig, ax = plt.subplots()

for station in range(nbr_stations):
    plt.plot(nc_datetime, nc_water_model_error[:, dummy, station],
             label="station {}".format(station))

plt.legend()
plt.ylabel("model error from {}".format(datafield_model))

plt.show()
