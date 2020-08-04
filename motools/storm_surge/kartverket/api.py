"""A class to query the storm surge data from kartverkets web API.

The full API documentation is at:
http://api.sehavniva.no/tideapi_protocol.pdf
"""

import logging
import datetime
from pprint import pformat
from bs4 import BeautifulSoup as bfls
import motools.config as moc
from motools.helper.url_request import NicedUrlRequest
from motools import logger


class KartverketAPI():
    """Query class for the Kartverkets storm surge web API."""
    def __init__(self):
        self.stations_IDs = None
        self.dict_all_stations_info = None

        self.mo_config = moc.Config()
        self.url_requester = NicedUrlRequest()

        self.fill_value = self.mo_config.getSetting("params", "fillValue")

        logger.info("Populate the stations info; this may take a few seconds...")
        self.populate_stations_info()
        logger.info("...done.")

    def populate_stations_info(self):
        """Get the metadata about all registered stations. Necessary to perform individual
        station queries."""
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
        logger.info("got {} stations: {}".format(len(self.stations_IDs), self.stations_IDs))

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

        logger.info("content of the stations dict: \n {}".format(pformat(self.dict_all_stations_info)))

    def get_one_station_over_time_extent(self, station_ID, time_start, time_end):
        """Query information for one individual station, between two dates.

        Input:
            - station_ID: a valid station ID (those are the 3 letters IDs used by the API)
            - time_start and time_end: the start and end dates for the data query

        Notes:
            - the maximum data size is low enough that the result can be stored in a single variable.
            - however, to limit the size of individual queries answers, the query is broken in segments
                of maximum length 10 days.
        """

        if not station_ID in self.stations_IDs:
            raise ValueError("station {} is not in {}".format(station_ID, self.stations_IDs))

        if not (isinstance(time_start, datetime.date) and isinstance(time_end, datetime.date)):
            raise ValueError("time_start and time_end must be datetime dates")

        if not (time_start > self.dict_all_stations_info[station_ID]["time_bounds"]["first"] and \
                time_end < self.dict_all_stations_info[station_ID]["time_bounds"]["last"]):
            raise ValueError("the time interval is not within the station logging span {} to {}".format(self.dict_all_stations_info[station_ID]["time_bounds"]["first"], self.dict_all_stations_info[station_ID]["time_bounds"]["last"]))

        # TODO: get as segments of duration 10 days maximum if too long duration
        # TODO: update the time_range function to allow doing the 10 days intervalling
        # TODO: by default use the highest resolution (10 minutes).
        pass

    def all_stations_over_time_extent(self):
        # TODO: choose start, end time
        # TODO: loop through all stations
        # TODO: check that time of availability is ok
        # TODO: populate the data

        pass

# TODO: check the water level change, and similar corrections

if __name__ == "__main__":
    logger.info("run an example of query")
    logger.setLevel(logging.INFO)
    kartveket_api = KartverketAPI()
