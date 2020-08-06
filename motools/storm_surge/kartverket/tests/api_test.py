import tempfile
import datetime
from motools.storm_surge.kartverket.api import KartverketAPI


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
