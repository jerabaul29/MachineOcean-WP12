""""""

import datetime
import motools.storm_surge.kyststasjoner_norge as kn
import motools.config as moc
import netCDF4 as nc4
from motools.helper import date as mod
from motools.helper import errors as moe


"""
NOTES:
    - the dataset is quite small; is there a way to get longer time series?
"""

##################################################
# time aspect of the data preparation
##################################################

# the time period which is spanned
date_start = datetime.date(2017, 12, 1)
# date_end = datetime.date(2020, 7, 1)
date_end = datetime.date(2017, 12, 15)

# the number of entries per day
entries_per_day = ["00", "12"]
nbr_entries_per_day = len(entries_per_day)

duration_day = datetime.timedelta(days=1)
number_of_days = (date_end - date_start).days
number_of_entries = number_of_days * nbr_entries_per_day

print("generating data for storm surge between dates {} and {}".format(date_start, date_end))
print("corresponding to a theoretical number of entries: {}".format(number_of_entries))

##################################################
# data properties
##################################################

number_of_stations = 23  # use 23 stations, which is what is present on the older files

##################################################
# generate the dataset
##################################################

mo_config = moc.Config()
folder_prepared_storm_surge = mo_config.getSetting("data", "stormSurgePreparedData")
name_dataset = "kystdata_{}_{}.nc".format(date_start, date_end)
nc_path_out = folder_prepared_storm_surge + "/" + name_dataset
print("dataset will be written to: {}".format(nc_path_out))

# root_grp = nc4.Dataset(nc_path_out, 'w', format='NETCDF4')
# root_grp.description = "storm surge data for learning"

for crrt_date in mod.datetime_range(date_start, date_end):
    for crrt_day_entry in entries_per_day:
        print("generate data day {} entry {}".format(crrt_date, crrt_day_entry))
        # now looking at the entry corresponding to crrt_date at the time crrt_day_entry
        # there are some data / runs missing, corrupted, etc.
        try:
            path_to_kyst_data = kn.kyststasjoner_path(crrt_date, crrt_day_entry)
            obs, model_mean, model_std = kn.get_kyststasjoner_data(path_to_kyst_data)

        except AssertionError as e:
            moe.detailed_assert_repr(e)

        except Exception as e:
            print(repr(e))
