"""Prepare the data for a simple error learning, on a single station.

- 1 entry per run of the storm surge model, i.e., 2 entries per day, 1 at 00hr and 1 at 12hr.

- for each entry, predictor is several arrays of data concatenated. Each array of data spans 5 days in the future, with an interval of 1 hour.

- for each entry, label is storm surge prediction error, each hour, for the next 5 days.
"""

"""
NOTES:
    - the dataset is quite small; is there a way to get longer time series?
"""

import datetime
import motools.storm_surge.kyststasjoner_norge as kn
import sys
import traceback

##################################################
# the time period which is spanned for performing the learning
date_start = datetime.date(2017, 12, 1)
duration_day = datetime.timedelta(days=1)
# date_end = datetime.date(2020, 7, 1)
date_end = datetime.date(2017, 12, 15)

number_of_days = (date_end - date_start).days
number_of_entries = number_of_days * 2

print("generating data for storm surge between dates {} and {}".format(date_start, date_end))
print("corresponding to a theoretical number of entries: {}".format(number_of_entries))

# there should be 2 entries per day, corresponding to the 2 runs of the storm surge model
entries_per_day = ["00", "12"]

# first make a list_entries, then add one by one the valid entries, then make it into a numpy array.
# each entry is a tuple, (predictors, labels)
list_entries = []

##################################################
# at present, train on a single station
# the station used
# this should be Bergen
station_used = 6

##################################################
# generate the dataset


crrt_date = date_start

while True:
    for crrt_day_entry in entries_per_day:
        print("generate data day {} entry {}".format(crrt_date, crrt_day_entry))
        # now looking at the entry corresponding to crrt_date at the time crrt_day_entry
        # there are some data / runs missing, corrupted, etc.
        try:
            path_to_kyst_data = kn.kyststasjoner_path(crrt_date, crrt_day_entry)
            obs, model_mean, model_std = kn.get_kyststasjoner_average_data(path_to_kyst_data)
            print(obs)

        except AssertionError as e:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]

            print('An error occurred on line {} in statement {}'.format(line, text))
            print("continue data generation")

        except Exception as e:
            print(repr(e))


    crrt_date += duration_day

    if crrt_date >= date_end:
        break
