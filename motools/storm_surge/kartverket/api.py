import urllib.request
from bs4 import BeautifulSoup as bfls


def perform_request(request):
    assert isinstance(request, str)

    with urllib.request.urlopen(request) as response:
        status = response.status
        html_string = response.read()

    assert status == 200, ("received status {} on request {}".format(status, request))
    assert isinstance(html_string, bytes)

    return(html_string)


class KartverketAPI(object):
    def __init__(self):
        self.stations_IDs = None
        self.stations_info_dict = None
        # TODO: read from conf fill value

    def get_stations_info(self):
        request = "http://api.sehavniva.no/tideapi.php?tide_request=stationlist&type=perm"
        html_string = perform_request(request)
        soup = bfls(html_string, features="lxml")
        list_tags = soup.find('stationinfo').select('location')

        dict_all_stations_info = {}

        for crrt_tag in list_tags:
            dict_tag = crrt_tag.attrs
            dict_all_stations_info[dict_tag["code"]] = dict_tag

        # TODO: add Time interval of available water level data from this station
        # TODO: add Year of first and last statistics for station

        self.stations_IDs = list(dict_all_stations_info.keys())
        self.stations_info_dict = dict_all_stations_info

        return(dict_all_stations_info)

    def get_one_station_over_time_extent(self):
        # TODO: station ID, time start, time stop, frequency
        # TODO: check that frequency is lower or equal to the available frequency
        # TODO: check that the time interval is compatible with time extent
        pass

    def get_all_stations_over_time_extent(self):
        # TODO: choose sampling frequency, start, end time
        # TODO: loop through all stations
        # TODO: check that sampling frequency is ok
        # TODO: check that time of availability is ok
        # TODO: populate the data

        pass

# TODO: use a request timer to not overload the server; have it as a helper? / for the whole class
# TODO: check the water level change, and similar corrections

