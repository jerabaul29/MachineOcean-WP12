"""documentation is at:
http://api.sehavniva.no/tideapi_protocol.pdf
"""

import logging
import datetime
from pprint import pformat
from bs4 import BeautifulSoup as bfls
import motools.config as moc
from motools.helper.url_request import NicedUrlRequest

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)


class KartverketAPI():
    def __init__(self):
        self.stations_IDs = None
        self.dict_all_stations_info = None

        self.mo_config = moc.Config()
        self.url_requester = NicedUrlRequest()

        self.fill_value = self.mo_config.getSetting("params", "fillValue")

    def get_stations_info(self):
        # request the list of stations with information
        request = "http://api.sehavniva.no/tideapi.php?tide_request=stationlist&type=perm"
        html_string = self.url_requester.perform_request(request)
        soup = bfls(html_string, features="lxml")
        list_tags = soup.find('stationinfo').select('location')

        # turn the tags into some dict entries for ordering the information
        self.dict_all_stations_info = {}

        for crrt_tag in list_tags:
            dict_tag = crrt_tag.attrs
            self.dict_all_stations_info[dict_tag["code"]] = dict_tag

        self.stations_IDs = list(self.dict_all_stations_info.keys())
        log.info("got {} stations: {}".format(len(self.stations_IDs), self.stations_IDs))

        # also request the start and end of data for each station
        for crrt_station in self.stations_IDs:
            request = "http://api.sehavniva.no/tideapi.php?tide_request=obstime&stationcode={}".format(crrt_station)
            html_string = self.url_requester.perform_request(request)
            soup = bfls(html_string, features="lxml")

            self.dict_all_stations_info[crrt_station]["time_bounds"] = {}

            for crrt_time in ["first", "last"]:
                crrt_time_str = soup.select("obstime")[0][crrt_time]
                crrt_datetime = datetime.datetime.fromisoformat(crrt_time_str)
                self.dict_all_stations_info[crrt_station]["time_bounds"][crrt_time] = crrt_datetime

        log.info("content of the stations dict: \n {}".format(pformat(self.dict_all_stations_info)))

        return(self.dict_all_stations_info)

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

if __name__ == "__main__":
    log.info("run an example of query")
    kartveket_api = KartverketAPI()
    all_stations_info = kartveket_api.get_stations_info()
