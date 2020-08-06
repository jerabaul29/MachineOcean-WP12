"""A class to query the storm surge data from kartverkets web API.

The full API documentation is at:
http://api.sehavniva.no/tideapi_protocol.pdf
"""

import logging
import datetime
import pprint
from pprint import pformat
import bs4
from bs4 import BeautifulSoup as bfls
import motools.config as moc
from motools.helper.url_request import NicedUrlRequest
from motools.helper.date import date_range
from motools import logger

pp = pprint.PrettyPrinter(indent=4).pprint


class KartverketAPI():
    """Query class for the Kartverkets storm surge web API."""
    def __init__(self, short_test=False):
        """short_test_ boolean, if only OSL station should be run."""

        self.short_test = short_test
        logger.info("short_test is {}".format(short_test))
        self.stations_ids = None
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

        if self.short_test:
            self.stations_ids = ["OSL"]
        else:
            self.stations_ids = list(self.dict_all_stations_info.keys())

        logger.info("got {} stations: {}".format(len(self.stations_ids), self.stations_ids))

        # also request the start and end of data for each station
        for crrt_station in self.stations_ids:
            request = "http://api.sehavniva.no/tideapi.php?tide_request=obstime&stationcode={}".format(crrt_station)
            html_string = self.url_requester.perform_request(request)
            soup = bfls(html_string, features="lxml")

            self.dict_all_stations_info[crrt_station]["time_bounds"] = {}

            for crrt_time in ["first", "last"]:
                crrt_time_str = soup.select("obstime")[0][crrt_time]
                crrt_datetime = datetime.datetime.fromisoformat(crrt_time_str)
                self.dict_all_stations_info[crrt_station]["time_bounds"][crrt_time] = crrt_datetime
                self.dict_all_stations_info[crrt_station]["time_bounds"]["{}_date".format(crrt_time)] = datetime.date(crrt_datetime.year, crrt_datetime.month, crrt_datetime.day)


        logger.info("content of the stations dict: \n {}".format(pformat(self.dict_all_stations_info)))

    def get_one_station_over_time_extent(self, station_id, date_start, date_end, max_request_length_days=10, time_resolution_minutes=10):
        # TODO: use a cache folder for putting already queried segments; this way, do not loose all data if something crashes. Should be a part of the "NicedURL" class.
        """Query information for one individual station, between two dates.

        Input:
            - station_id: a valid station ID (those are the 3 letters IDs used by the API)
            - date_start and date_end: the start and end dates for the data query
            - max_request_length_days: the maximum number of days duration of one individual server
                request (default 10), to avoid asking for too large data sets at the same time.
            - time_resolution_minutes: the time resolution of the acquired data; may be either
                10 or 60 minutes (defined by the API). Default is 10.

        Notes:
            - the maximum data size is low enough that the result can be stored in a single variable.
            - however, to limit the size of individual queries answers, the query is broken in segments
                of maximum length 10 days.
        """

        if not station_id in self.stations_ids:
            raise ValueError("station {} is not in {}".format(station_id, self.stations_ids))

        if not (isinstance(date_start, datetime.date) and isinstance(date_end, datetime.date)):
            raise ValueError("date_start and date_end must be datetime dates")

        if not (date_start > self.dict_all_stations_info[station_id]["time_bounds"]["first_date"] and \
                date_end < self.dict_all_stations_info[station_id]["time_bounds"]["last_date"]):
            raise ValueError("the time interval is not within the station logging span {} to {}".format(self.dict_all_stations_info[station_id]["time_bounds"]["first_date"], self.dict_all_stations_info[station_id]["time_bounds"]["last_date"]))

        if not (isinstance(max_request_length_days, int) and max_request_length_days > 0):
            raise ValueError("max_request_length_days must be a positive int, received {}".format(max_request_length_days))

        time_bounds_requests = list(date_range(date_start, date_end, max_request_length_days))
        number_of_segments = len(time_bounds_requests)
        time_bounds_requests.append(date_end)

        dict_station_data = {}
        dict_station_data["station_id"] = station_id

        # request the data, segment by segment, organizing stuff through a dict
        for ind, (crrt_request_start, crrt_request_end) in enumerate(zip(time_bounds_requests[:-1], time_bounds_requests[1:])):
            logger.info("request kartverket data over dates {} - {}".format(crrt_request_start, crrt_request_end))

            last_segment = (crrt_request_end == date_end)
            logger.info("last_segment: {}".format(last_segment))

            strftime_format = "%Y-%m-%dT%H:%M:%S"
            utc_time_start = datetime.datetime(crrt_request_start.year, crrt_request_start.month, crrt_request_start.day,
                                               hour=0, minute=0, second=0
                                               ).strftime(strftime_format)
            utc_time_end = datetime.datetime(crrt_request_end.year, crrt_request_end.month, crrt_request_end.day,
                                               hour=0, minute=0, second=0
                                               ).strftime(strftime_format)
            request = "https://api.sehavniva.no/tideapi.php?stationcode={}&fromtime={}&totime={}&datatype=obs&refcode=&place=&file=&lang=&interval={}&dst=&tzone=utc&tide_request=stationdata".format(station_id, utc_time_start, utc_time_end, time_resolution_minutes)

            html_data = self.url_requester.perform_request(request)
            soup = bfls(html_data)

            dict_segment = {}
            dict_station_data[ind] = dict_segment

            # each segment has several datasets
            for crrt_dataset in soup.findAll("data"):
                data_type = crrt_dataset["type"]
                data_unit = crrt_dataset["unit"]
                data_reflevelcode = crrt_dataset["reflevelcode"]

                crrt_key = "{}_{}_{}".format(data_type, data_unit, data_reflevelcode)

                if crrt_key not in dict_segment:
                    logger.info("create entry {} in the current station data dict".format(crrt_key))
                    dict_segment[crrt_key] = []

                # individual entries are specific tags; note that the string content of each tag is empty, the data
                # is in the tag specification itself.
                for crrt_entry in crrt_dataset:
                    # effectively ignores the empty string contents, grab data from the tags
                    if type(crrt_entry) is bs4.element.Tag:
                        time = crrt_entry["time"]
                        value = crrt_entry["value"]
                        data_tuple = (datetime.datetime.fromisoformat(time), value)
                        dict_segment[crrt_key].append(data_tuple)

                # to avoid to duplicate the last measurement of each segment, pop once if not last segment
                if not last_segment:
                    _ = dict_segment[crrt_key].pop()

        list_entries_ref_segment = list(dict_station_data[0].keys())

        # check that the data are homogeneous across segments
        for crrt_segment in range(number_of_segments):
            if list(dict_station_data[crrt_segment].keys()) != list_entries_ref_segment:
                raise ValueError("incompatible data segments: segment {} has entries {} while first segment has {}]".format(crrt_segment, list(dict_station_data[crrt_segment].keys()), list_entries_ref_segment))

        # put the segments together to get data over the whole time
        for crrt_dataset in list_entries_ref_segment:
            dict_station_data[crrt_dataset] = []
            for crrt_segment in range(number_of_segments):
                dict_station_data[crrt_dataset].extend(dict_station_data[crrt_segment][crrt_dataset])

        # perform a few sanity checks on the final, concatenated data
        # no missing or duplicated timestamps
        for crrt_dataset in list_entries_ref_segment:
            crrt_data = dict_station_data[crrt_dataset]
            for crrt_ind in range(len(crrt_data)-1):
                delta_time_next_point = (crrt_data[crrt_ind+1][0] - crrt_data[crrt_ind][0]).seconds
                if not delta_time_next_point == time_resolution_minutes * 60:
                    raise ValueError("dataset {} at index {} to {} corresponds to delta time {}s while {}s was expected".format(crrt_dataset, crrt_ind, crrt_ind+1, delta_time_next_point, time_resolution_minutes*60))

        dict_result = {}
        for crrt_dataset in list_entries_ref_segment:
            dict_result[crrt_dataset] = dict_station_data[crrt_dataset]

        return(dict_result)





# TODO: check the water level change, and similar corrections
# TODO: do a loop through all stations
# TODO: put a bit of simple plotting tools

if __name__ == "__main__":
    logger.info("run an example of query")
    logger.setLevel(logging.INFO)
    kartveket_api = KartverketAPI(short_test=True)
    dict_station_data = kartveket_api.get_one_station_over_time_extent("OSL", datetime.date(2020, 1, 1), datetime.date(2020, 1, 25))
    pp(dict_station_data)
    print(dict_station_data.keys())
