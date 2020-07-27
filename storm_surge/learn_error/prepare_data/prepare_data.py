"""Prepare the data for a simple error learning, on a single station.

- 1 entry per run of the storm surge model, i.e., 2 entries per day, 1 at 00hr and 1 at 12hr.

- for each entry, predictor is several arrays of data concatenated. Each array of data spans 5 days in the future, with an interval of 1 hour.

- for each entry, label is storm surge prediction error, each hour, for the next 5 days.
"""

"""NOTES:
    - the dataset is quite small; is there a way to get longer time series?
"""

import json
import datetime

with open("../../../config/config.json", 'r') as fh:
    config = json.load(fh)

version = '1.0'

lustre_root_path = config['config'][version]['path']['dataRoot']
print("base path on lustre: {}".format(lustre_root_path))

##################################################
# the time period which is spanned for performing the learning
date_start = datetime.date(2017, 12, 1)
duration_day = datetime.timedelta(days=1)
# date_end = datetime.date(2020, 7, 1)
date_end = datetime.date(2017, 12, 15)

number_of_days = (date_end - date_start).days
number_of_entries = number_of_days * 2

# there should be 2 entries per day, corresponding to the 2 runs of the storm surge model
entries_per_day = ["00", "12"]

# first make a list_entries, then add one by one the valid entries, then make it into a numpy array.
# each entry is a tuple, (predictors, labels)
list_entries = []

##################################################
# the station used
# this should be Bergen
station_used = 6

##################################################
# getting the label


crrt_date = date_start

while True:
    for crrt_day_entry in entries_per_day:
        # now looking at the entry corresponding to crrt_date at the time crrt_day_entry
        pass

        # there are some data / runs missing, corrupted, etc.
        try:
            pass

        except:
            pass

    crrt_date += duration_day

    if crrt_date >= date_end:
        break
