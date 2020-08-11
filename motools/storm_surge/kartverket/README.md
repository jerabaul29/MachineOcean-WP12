# This folder contains tools for accessing the API from kartverket.no about storm surge observations.

The main document documenting the API is attached, and available at:

http://api.sehavniva.no/tideapi_protocol.pdf

In addition, for an example of something that has been used in ROMS query of data:
wget --tries=5 -O /lustre/storeB/project/metproduction/products/util/vannstand/sehavniva.xml "https://api.sehavniva.no/tideapi.php?stationcode=&fromtime=2020-07-01T11:00:00&totime=2020-08-20T11:00:00&datatype=all&refcode=msl&place=&file=&lang=nn&interval=60&dst=2&tzone=utc&tide_request=stationdata"

As a side note, there is also a web-based html interface to the API:
http://api.sehavniva.no/tideapi_en.html

And the entry point for finding this documentation was:
https://www.kartverket.no/en/sehavniva/api-for-water-level-data-and-widget/

## About the code

The api.py module contains two main classes:

- KartverketAPI: use the kartverket http-based API as described in the pdf to 1) query data, 2) assemble the data into consistent datasets, 3) dump into netCDF format. The API requests are made in such a way not too be unpolite to the server: 1) do not perform more than 1 request per second per active API interaction class, 2) split the request in small chunks so that the size of each message is reasonable. As a consequence, building the full dataset may take quite a while!

- AccessStormSurgeNetCDF: access, slice data, visualize data, from the netCDF dump generated at the previous stage.

Example of use: see following, or the test folder.

```python
import datetime
from motools.storm_surge.kartverket.api import KartverketAPI, AccessStormSurgeNetCDF

kartverket_api = KartverketAPI()

# time limits for the kartverket API data query
date_start = datetime.date(2009, 12, 8)
date_end = datetime.date(2010, 1, 4)

path_dump = "./example_dataset.nc4"

# query the data and dump as netCDF file
kartverket_api.generate_netcdf_dataset(date_start, date_end, path=path_dump)

# time slice to extract from the netCDF file
datetime_start_data = datetime.datetime(2009, 12, 15, 9, 00, 00)
datetime_end_data = datetime.datetime(2009, 12, 15, 10, 00, 00)

# get the data from the netCDF file
kartverket_nc4 = AccessStormSurgeNetCDF()
# we are interested in BGO station, on a datetime range that is smaller than the extent of the full dataset
timestamps, observation, prediction = kartverket_nc4.get_data("BGO", datetime_start_data, datetime_end_data, path_to_netCDF=path_dump)
datetime_timestamps = [datetime.datetime.fromtimestamp(crrt_timestamp) for crrt_timestamp in timestamps]
```

## About the data

The data for observation and precition correspond to what is explained p.11/16 of the api documentation:

"
datatype (optional, default=all):
TAB = tide table (high tide and low tide)
PRE = predictions = astronomic tide
OBS = observations = measured water level
ALL = both predictions, observations, weathereffect and forecast will be
returned.
"

So the observation is the value actually measured, while the prediction is the component coming from the astronomic tide. All units are in cm.
