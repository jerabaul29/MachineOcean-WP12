import tempfile
import datetime
import numpy as np
from motools.storm_surge.kartverket.api import KartverketAPI, AccessStormSurgeNetCDF


def test_single_station_request():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(short_test=True, cache_folder=tmpdirname)

        date_start = datetime.date(2020, 1, 1)
        date_end = datetime.date(2020, 1, 25)

        kartverket_api.get_one_station_over_time_extent("OSL", date_start, date_end)


def test_all_stations_request():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(cache_folder=tmpdirname)

        date_start = datetime.date(2018, 1, 1)
        date_end = datetime.date(2018, 1, 25)

        kartverket_api.get_all_stations_over_time_extent(date_start, date_end)


def test_fill_start_data():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(short_test=True, cache_folder=tmpdirname)

        date_start = datetime.date(2006, 12, 12)
        date_end = datetime.date(2007, 1, 3)

        kartverket_api.get_one_station_over_time_extent("OSL", date_start, date_end)


def test_generate_netcdf_dataset():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(short_test=True, cache_folder=tmpdirname)

        date_start = datetime.date(2006, 12, 12)
        date_end = datetime.date(2007, 1, 3)

        kartverket_api.generate_netcdf_dataset(date_start, date_end)


def test_correctness_netcdf_generation_extraction():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(short_test=True, cache_folder=tmpdirname)

        date_start = datetime.date(2008, 12, 12)
        date_end = datetime.date(2009, 1, 3)

        kartverket_api.generate_netcdf_dataset(date_start, date_end)

        datetime_start_data = datetime.datetime(2008, 12, 15, 9, 00, 00)
        datetime_end_data = datetime.datetime(2008, 12, 15, 10, 00, 00)

        kartverket_nc4 = AccessStormSurgeNetCDF()
        timestamps, observation, prediction = kartverket_nc4.get_data("OSL", datetime_start_data, datetime_end_data)
        datetime_timestamps = [datetime.datetime.fromtimestamp(crrt_timestamp) for crrt_timestamp in timestamps]

        # the answer is obtained from doing the request by hand:
        # https://api.sehavniva.no/tideapi.php?stationcode=OSL&fromtime=2008-12-15T09:00:00+00&totime=2008-12-15T10:00:00+00&datatype=obs&refcode=&place=&file=&lang=&interval=10&dst=&tzone=utc&tide_request=stationdata
        # <tide>
        # <stationdata>
        # <location name="OSLO" code="OSL" latitude="59.908559" longitude="10.734510">
        # <data type="observation" unit="cm" reflevelcode="CD">
        # <waterlevel value="27.3" time="2008-12-15T09:00:00+00:00" flag="obs"/>
        # <waterlevel value="26.4" time="2008-12-15T09:10:00+00:00" flag="obs"/>
        # <waterlevel value="25.6" time="2008-12-15T09:20:00+00:00" flag="obs"/>
        # <waterlevel value="25.0" time="2008-12-15T09:30:00+00:00" flag="obs"/>
        # <waterlevel value="24.6" time="2008-12-15T09:40:00+00:00" flag="obs"/>
        # <waterlevel value="24.2" time="2008-12-15T09:50:00+00:00" flag="obs"/>
        # <waterlevel value="23.8" time="2008-12-15T10:00:00+00:00" flag="obs"/>
        # </data>
        # <data type="prediction" unit="cm" reflevelcode="CD">
        # <waterlevel value="56.1" time="2008-12-15T09:00:00+00:00" flag="pre"/>
        # <waterlevel value="54.8" time="2008-12-15T09:10:00+00:00" flag="pre"/>
        # <waterlevel value="53.6" time="2008-12-15T09:20:00+00:00" flag="pre"/>
        # <waterlevel value="52.7" time="2008-12-15T09:30:00+00:00" flag="pre"/>
        # <waterlevel value="51.9" time="2008-12-15T09:40:00+00:00" flag="pre"/>
        # <waterlevel value="51.2" time="2008-12-15T09:50:00+00:00" flag="pre"/>
        # <waterlevel value="50.6" time="2008-12-15T10:00:00+00:00" flag="pre"/>
        # </data>
        # </location>
        # </stationdata>
        # </tide>

        correct_datetime_timestamps = [datetime.datetime(2008, 12, 15, 9, 0),
                                       datetime.datetime(2008, 12, 15, 9, 10),
                                       datetime.datetime(2008, 12, 15, 9, 20),
                                       datetime.datetime(2008, 12, 15, 9, 30),
                                       datetime.datetime(2008, 12, 15, 9, 40),
                                       datetime.datetime(2008, 12, 15, 9, 50),
                                       datetime.datetime(2008, 12, 15, 10, 0)]

        correct_observation = [27.3, 26.4, 25.6, 25. , 24.6, 24.2, 23.8]
        correct_prediction = [56.1, 54.8, 53.6, 52.7, 51.9, 51.2, 50.6]

        assert correct_datetime_timestamps == datetime_timestamps
        assert np.allclose(np.array(correct_observation), observation)
        assert np.allclose(np.array(correct_prediction), prediction)


def test_correctness_netcdf_generation_extraction_long_test():
    with tempfile.TemporaryDirectory() as tmpdirname:
        kartverket_api = KartverketAPI(short_test=False, cache_folder=tmpdirname)

        date_start = datetime.date(2009, 12, 8)
        date_end = datetime.date(2010, 1, 4)

        kartverket_api.generate_netcdf_dataset(date_start, date_end)

        datetime_start_data = datetime.datetime(2009, 12, 15, 9, 00, 00)
        datetime_end_data = datetime.datetime(2009, 12, 15, 10, 00, 00)

        kartverket_nc4 = AccessStormSurgeNetCDF()
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

