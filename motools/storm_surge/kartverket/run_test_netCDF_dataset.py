import shutil
import tempfile
import numpy as np
import datetime
from motools.storm_surge.kartverket.api import NetCDFTester, KartverketAPI, AccessStormSurgeNetCDF
from motools.helper.date import date_to_datetime

temp_dir = tempfile.TemporaryDirectory()
tmpdirname = temp_dir.name
# with tempfile.TemporaryDirectory() as tmpdirname:
if True:
    kartverket_api = KartverketAPI(short_test=False, cache_folder=tmpdirname)

    date_start = datetime.date(2009, 12, 8)
    date_end = datetime.date(2010, 1, 4)

    path_to_NetCDF = "{}/netcdf_test.nc4".format(tmpdirname)

    kartverket_api.generate_netcdf_dataset(date_start, date_end, path=path_to_NetCDF)

    datetime_start_data = datetime.datetime(2009, 12, 15, 9, 00, 00)
    datetime_end_data = datetime.datetime(2009, 12, 15, 10, 00, 00)

    kartverket_nc4 = AccessStormSurgeNetCDF(path_to_NetCDF=path_to_NetCDF)
    timestamps, observation, prediction = kartverket_nc4.get_data("BGO", datetime_start_data, datetime_end_data)
    datetime_timestamps = [datetime.datetime.fromtimestamp(crrt_timestamp) for crrt_timestamp in timestamps]

    # the answer is obtained from doing the request by hand:
    # https://api.sehavniva.no/tideapi.php?stationcode=BGO&fromtime=2009-12-15T09:00:00+00&totime=2009-12-15T10:00:00+00&datatype=obs&refcode=&place=&file=&lang=&interval=10&dst=&tzone=utc&tide_request=stationdata
    # <tide>
    # <stationdata>
    # <location name="BERGEN" code="BGO" latitude="60.398046" longitude="5.320487">
    # <data type="observation" unit="cm" reflevelcode="CD">
    # <waterlevel value="141.5" time="2009-12-15T09:00:00+00:00" flag="obs"/>
    # <waterlevel value="141.5" time="2009-12-15T09:10:00+00:00" flag="obs"/>
    # <waterlevel value="141.1" time="2009-12-15T09:20:00+00:00" flag="obs"/>
    # <waterlevel value="140.3" time="2009-12-15T09:30:00+00:00" flag="obs"/>
    # <waterlevel value="138.9" time="2009-12-15T09:40:00+00:00" flag="obs"/>
    # <waterlevel value="137.0" time="2009-12-15T09:50:00+00:00" flag="obs"/>
    # <waterlevel value="134.5" time="2009-12-15T10:00:00+00:00" flag="obs"/>
    # </data>
    # <data type="prediction" unit="cm" reflevelcode="CD">
    # <waterlevel value="151.6" time="2009-12-15T09:00:00+00:00" flag="pre"/>
    # <waterlevel value="151.2" time="2009-12-15T09:10:00+00:00" flag="pre"/>
    # <waterlevel value="150.5" time="2009-12-15T09:20:00+00:00" flag="pre"/>
    # <waterlevel value="149.4" time="2009-12-15T09:30:00+00:00" flag="pre"/>
    # <waterlevel value="147.9" time="2009-12-15T09:40:00+00:00" flag="pre"/>
    # <waterlevel value="146.1" time="2009-12-15T09:50:00+00:00" flag="pre"/>
    # <waterlevel value="144.0" time="2009-12-15T10:00:00+00:00" flag="pre"/>
    # </data>
    # </location>
    # </stationdata>
    # </tide>

    correct_datetime_timestamps = [datetime.datetime(2009, 12, 15, 9, 0),
                                    datetime.datetime(2009, 12, 15, 9, 10),
                                    datetime.datetime(2009, 12, 15, 9, 20),
                                    datetime.datetime(2009, 12, 15, 9, 30),
                                    datetime.datetime(2009, 12, 15, 9, 40),
                                    datetime.datetime(2009, 12, 15, 9, 50),
                                    datetime.datetime(2009, 12, 15, 10, 0)]

    correct_observation = [141.5, 141.5, 141.1, 140.3, 138.9, 137. , 134.5]
    correct_prediction = [151.6, 151.2, 150.5, 149.4, 147.9, 146.1, 144. ]

    assert correct_datetime_timestamps == datetime_timestamps
    assert np.allclose(np.array(correct_observation), observation)
    assert np.allclose(np.array(correct_prediction), prediction)

    tester = NetCDFTester(path_to_ordered_cache="{}/".format(tmpdirname), path_to_netCDF=path_to_NetCDF, limit_datetimes=(date_to_datetime(date_start, aware=False), date_to_datetime(date_end, aware=False)))

    tester.perform_random_tests(n_tests=50)

shutil.rmtree(temp_dir.name)
