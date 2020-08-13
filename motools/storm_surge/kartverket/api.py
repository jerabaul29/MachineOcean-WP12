"""Classes to query the storm surge data from kartverkets web API.

The full API documentation is at:
http://api.sehavniva.no/tideapi_protocol.pdf
"""

import os
from pathlib import Path
import logging
import datetime
import pprint
import math
from pprint import pformat
import random

import bs4
from bs4 import BeautifulSoup as bfls

import netCDF4 as nc4

import numpy as np

from tqdm import tqdm

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import cartopy.crs as ccrs
import cartopy.feature as cfeature

import motools.config as moc
from motools.helper.url_request import NicedUrlRequest
from motools.helper.date import date_range, datetime_range, date_to_datetime
from motools.helper.date import find_dropouts
from motools import logger
from motools.helper import arrays as moa

pp = pprint.PrettyPrinter(indent=4).pprint


def cache_organizer(request):
    # check that it is a data request; these are the ones we want to classify
    if ("fromtime" in request) and ("totime" in request) and ("obs" in request):
        return("{}/{}/{}/".format(request[49:52], request[62:66], request[67:69]))
    else:
        return("")


class KartverketAPI():
    """Query class for the Kartverkets storm surge web API."""
    def __init__(self, short_test=False, cache_folder="default"):
        """Inputs:
            - short_test_ boolean, if only OSL station should be run.
            - cache_folder: the cache folder to use for the NicedUrlRequest, "default" is home.
        """

        self.short_test = short_test
        logger.info("short_test is {}".format(short_test))
        self.stations_ids = None
        self.dict_all_stations_data = None

        self.cache_folder = cache_folder

        self.mo_config = moc.Config()
        self.url_requester = NicedUrlRequest(cache_folder=self.cache_folder, cache_organizer=cache_organizer)

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
        self.dict_all_stations_data = {}

        for crrt_tag in list_tags:
            dict_tag = crrt_tag.attrs
            self.dict_all_stations_data[dict_tag["code"]] = dict_tag

        if self.short_test:
            self.stations_ids = ["OSL"]
        else:
            self.stations_ids = sorted(list(self.dict_all_stations_data.keys()))

        logger.info("got {} stations: {}".format(len(self.stations_ids), self.stations_ids))

        # also request the start and end of data for each station
        for crrt_station in self.stations_ids:
            request = "http://api.sehavniva.no/tideapi.php?tide_request=obstime&stationcode={}".format(crrt_station)
            html_string = self.url_requester.perform_request(request)
            soup = bfls(html_string, features="lxml")

            self.dict_all_stations_data[crrt_station]["time_bounds"] = {}

            for crrt_time in ["first", "last"]:
                crrt_time_str = soup.select("obstime")[0][crrt_time]
                crrt_datetime = datetime.datetime.fromisoformat(crrt_time_str)
                self.dict_all_stations_data[crrt_station]["time_bounds"][crrt_time] = crrt_datetime
                self.dict_all_stations_data[crrt_station]["time_bounds"]["{}_date".format(crrt_time)] = datetime.date(crrt_datetime.year, crrt_datetime.month, crrt_datetime.day)

        logger.info("content of the stations dict: \n {}".format(pformat(self.dict_all_stations_data)))

    def generate_netcdf_dataset(self, date_start, date_end, path=None, time_resolution_minutes=10, list_stations=None):
        """Generate a netCDF dataset for the storm surge data.

        Input:
            - date_start: the start of the dataset.
            - date_end: the end of the dataset.
            - path: full path (including dir and filename). If None, use the cwd
                and name "data_kartverket_stormsurge.nc4"
            - time_resolution_minutes: the resolution of the data queried, should be
            either 10 or 60. Default: 10.
            - list_stations: the stations to generate data about. Default is None, i.e.
            all stations available. Can also specify a list of stations ids, ex: ["OSL", "BGO"]
        """

        if path is None:
            path = os.getcwd() + "/data_kartverket_stormsurge.nc4"

        if not (isinstance(date_start, datetime.date) and isinstance(date_end, datetime.date)):
            raise ValueError("date_start and date_end must be datetime dates")

        if date_start > date_end:
            raise ValueError("date_start should be before date_end, but got {} and {}".format(date_start, date_end))

        self.old_stations_ids = self.stations_ids
        if list_stations is not None:
            self.stations_ids = list_stations

        timedelta_step = datetime.timedelta(minutes=time_resolution_minutes)
        # need to append by hand the last timestamp, as the range is by default [[
        time_vector = np.array([time.timestamp() for time in datetime_range(date_to_datetime(date_start + datetime.timedelta(days=-1)), date_to_datetime(date_end + datetime.timedelta(days=+1)), timedelta_step)] + [date_to_datetime(date_end + datetime.timedelta(days=+1)).timestamp()])
        number_of_time_entries = time_vector.shape[0]

        with nc4.Dataset(path, "w", format="NETCDF4") as nc4_fh:
            nc4_fh.set_auto_mask(False)
            description_string = "storm surge dataset from the Norwegian coast, generated by MachineOcean-WP12/storm_surge/learn_error/prepare_data/prepare_data.py"

            nc4_fh.Conventions = "CF-X.X"
            nc4_fh.title = "storm surge from kartverket API"
            nc4_fh.description = description_string
            nc4_fh.institution = "IT department, Norwegian Meteorological Institute"
            nc4_fh.Contact = "jeanr@met.no"

            _ = nc4_fh.createDimension('station', len(self.stations_ids))
            _ = nc4_fh.createDimension('time', number_of_time_entries)

            stationid = nc4_fh.createVariable("stationid", str, ('station'))
            latitude = nc4_fh.createVariable('latitude', 'f4', ('station'))
            longitude = nc4_fh.createVariable('longitude', 'f4', ('station'))
            timestamp = nc4_fh.createVariable('timestamp', 'i8', ('time'))
            observation = nc4_fh.createVariable('observation', 'f4', ('station', 'time'))
            prediction = nc4_fh.createVariable('prediction', 'f4', ('station', 'time'))
            timestamp_start = nc4_fh.createVariable('timestamp_start', 'i8', ('station'))
            timestamp_end = nc4_fh.createVariable('timestamp_end', 'i8', ('station'))

            # TODO: document the different fields: unit, name, etc

            timestamp[:] = time_vector

            # fill with the stations data
            for ind, crrt_station in enumerate(tqdm(self.stations_ids, desc="station")):
                stationid[ind] = crrt_station
                latitude[ind] = self.dict_all_stations_data[crrt_station]["latitude"]
                longitude[ind] = self.dict_all_stations_data[crrt_station]["longitude"]
                timestamp_start[ind] = self.dict_all_stations_data[crrt_station]["time_bounds"]["first"].timestamp()
                timestamp_end[ind] = self.dict_all_stations_data[crrt_station]["time_bounds"]["last"].timestamp()

                crrt_data = self.get_one_station_over_time_extent(crrt_station, date_start, date_end, max_request_length_days=10, time_resolution_minutes=time_resolution_minutes)

                # check that the timestamp bases all agree
                for crrt_entry in crrt_data.keys():
                    crrt_timestamps = np.array([crrt_datetime.timestamp() for (crrt_datetime, data) in crrt_data[crrt_entry]])
                    if not np.all(np.isclose(crrt_timestamps, time_vector)):
                        logger.warning("print crrt_timestamps")
                        logger.warning(crrt_timestamps[:10])
                        logger.warning(crrt_timestamps[-10:])
                        logger.warning(len(crrt_timestamps))
                        logger.warning("print time_vector")
                        logger.warning(time_vector[:10])
                        logger.warning(time_vector[-10:])
                        logger.warning(len(time_vector))
                        raise ValueError("mismatch in timesteps between the pre-determined time vector {} and the one effectively obtained {} processing station {}".format(time_vector, crrt_timestamps, crrt_station))

                array_prediction = np.nan_to_num(np.array([data for (time, data) in crrt_data["prediction_cm_CD"]]), nan=self.fill_value)
                array_observation = np.nan_to_num(np.array([data for (time, data) in crrt_data["observation_cm_CD"]]), nan=self.fill_value)

                prediction[ind, :] = array_prediction
                observation[ind, :] = array_observation

        self.stations_ids = self.old_stations_ids


    def get_all_stations_over_time_extent(self, date_start, date_end):
        """Query information for all stations, between two dates.

        Input:
            - date_start: the start date
            - date_end: the end date
        """

        if not (isinstance(date_start, datetime.date) and isinstance(date_end, datetime.date)):
            raise ValueError("date_start and date_end must be datetime dates")

        for crrt_station in self.stations_ids:
            dict_data_station = self.get_one_station_over_time_extent(crrt_station, date_start, date_end)
            self.dict_all_stations_data[crrt_station]["data"] = dict_data_station

    def get_one_station_over_time_extent(self, station_id, date_start, date_end, max_request_length_days=10, time_resolution_minutes=10):
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

        if date_start > date_end:
            raise ValueError("date_start should be after date_end, but got {} and {}".format(date_start, date_end))

        start_padding_missing_timestamps = []
        end_padding_missing_timestamps = []

        true_date_start = date_start
        true_date_end = date_end

        # whether a request is needed at all, i.e., whether there is data at all available over the time range.
        request_needed = True

        if not (date_start > self.dict_all_stations_data[station_id]["time_bounds"]["first_date"] and \
                date_end < self.dict_all_stations_data[station_id]["time_bounds"]["last_date"]):
            logger.warning("the time interval {} to {} for station {} is not within the station logging span {} to {}; padding with NaNs".format(date_start, date_end, station_id, self.dict_all_stations_data[station_id]["time_bounds"]["first_date"], self.dict_all_stations_data[station_id]["time_bounds"]["last_date"]))

            # note: only need to append / prepend the last / first missing time, as the droupout check will fill the holes later

            if date_start < self.dict_all_stations_data[station_id]["time_bounds"]["first_date"]:
                date_start = self.dict_all_stations_data[station_id]["time_bounds"]["first_date"] + datetime.timedelta(days=1)

            if date_end > self.dict_all_stations_data[station_id]["time_bounds"]["last_date"]:
                date_end = self.dict_all_stations_data[station_id]["time_bounds"]["last_date"] + datetime.timedelta(days=-1)

            if date_start > self.dict_all_stations_data[station_id]["time_bounds"]["last_date"] or date_end < self.dict_all_stations_data[station_id]["time_bounds"]["first_date"]:
                request_needed = False

        # sometimes, a station returns data over a some time, but only observation or prediction are availalbe, which may disturb the time bases of the different fiels;
        # to make sure homogeneous, always append / prepend dummy dates from day before to day after.
        start_padding_missing_timestamps = [(date_to_datetime(true_date_start) + datetime.timedelta(days=-1), math.nan)]
        end_padding_missing_timestamps = [(date_to_datetime(true_date_end) + datetime.timedelta(days=+1), math.nan)]

        if not (isinstance(max_request_length_days, int) and max_request_length_days > 0):
            raise ValueError("max_request_length_days must be a positive int, received {}".format(max_request_length_days))

        if request_needed:
            time_bounds_requests = list(date_range(date_start, date_end, max_request_length_days))
        else:
            time_bounds_requests = []

        number_of_segments = len(time_bounds_requests)
        time_bounds_requests.append(date_end)

        dict_station_data = {}
        dict_station_data["station_id"] = station_id

        logger.info("obtaining data about station {}".format(station_id))

        # request the data, segment by segment, organizing the results through a dict
        for ind, (crrt_request_start, crrt_request_end) in enumerate(tqdm(zip(time_bounds_requests[:-1], time_bounds_requests[1:]), total=len(time_bounds_requests)-1, desc="segment", position=0, leave=True)):
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
            soup = bfls(html_data, features="lxml")

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
                        value = float(crrt_entry["value"])
                        data_tuple = (datetime.datetime.fromisoformat(time), value)
                        dict_segment[crrt_key].append(data_tuple)

                # to avoid to duplicate the last measurement of each segment, pop once if not last segment
                if not last_segment:
                    _ = dict_segment[crrt_key].pop()


        list_entries_ref_segment = ["observation_cm_CD", "prediction_cm_CD"]

        if not request_needed:
            logger.warning("Using default list of entries, as no data over the time range specified! This may break if data are not homogeneous over stations!")
            dict_station_data[0] = {}
            dict_station_data[0]["observation_cm_CD"] = []
            dict_station_data[0]["prediction_cm_CD"] = []

        # check that the data are homogeneous across segments
        for crrt_segment in range(number_of_segments):
            list_crrt_entries = list(dict_station_data[crrt_segment].keys())

            if list_crrt_entries != list_entries_ref_segment:
                logger.warning("incompatible data segments: segment {} has entries {} while first segment has {}]".format(crrt_segment, list_crrt_entries, list_entries_ref_segment))

                unknown_entries = list(set(list_crrt_entries) - set(list_entries_ref_segment))

                if len(unknown_entries) != 0:
                    logger.warning("there are some unknown entries: {} not in the reference segment; removing".format(unknown_entries))
                    for crrt_unknown_entry in unknown_entries:
                        del dict_station_data[crrt_segment][crrt_unknown_entry]

                missing_entries = list(set(list_entries_ref_segment) - set(list_crrt_entries))

                if len(missing_entries) != 0:
                    logger.warning("there are some missing entries: {} in reference segment but not current segment; adding dummy".format(missing_entries))
                    for crrt_missing_entry in missing_entries:
                        dict_station_data[crrt_segment][crrt_missing_entry] = []

            assert sorted(list_entries_ref_segment) == sorted(list(dict_station_data[crrt_segment].keys()))


        # put the segments together to get data over the whole time
        for crrt_dataset in list_entries_ref_segment:
            dict_station_data[crrt_dataset] = []
            for crrt_segment in range(number_of_segments):
                dict_station_data[crrt_dataset].extend(dict_station_data[crrt_segment][crrt_dataset])

            dict_station_data[crrt_dataset] = start_padding_missing_timestamps + dict_station_data[crrt_dataset] + end_padding_missing_timestamps

        # perform a few sanity checks on the final,check that concatenated data
        # have no missing or duplicated timestamps
        for crrt_dataset in list_entries_ref_segment:
            crrt_dataset_content = dict_station_data[crrt_dataset]
            crrt_datetimes = [crrt_date for (crrt_date, data) in crrt_dataset_content]

            # find dropouts
            list_locations_need_fill_after = find_dropouts(crrt_datetimes, time_resolution_minutes*60, behavior="warning")
            list_locations_need_fill_after.reverse()

            # fill dropouts with NaN
            for crrt_dropout_index in list_locations_need_fill_after:
                time_before = crrt_datetimes[crrt_dropout_index]
                time_after = crrt_datetimes[crrt_dropout_index+1]
                timedelta_step = datetime.timedelta(minutes=time_resolution_minutes)
                list_to_insert = list(datetime_range(time_before, time_after, timedelta_step))[1:]
                list_datapoints_to_insert = [(crrt_time, math.nan) for crrt_time in list_to_insert]
                crrt_dataset_content = crrt_dataset_content[:crrt_dropout_index+1] + list_datapoints_to_insert + crrt_dataset_content[crrt_dropout_index+1:]

            dict_station_data[crrt_dataset] = crrt_dataset_content

            # once insertion is performed, there should be no holes any longer
            crrt_dataset_content = dict_station_data[crrt_dataset]
            crrt_datetimes = [crrt_date for (crrt_date, data) in crrt_dataset_content]
            _ = find_dropouts(crrt_datetimes, time_resolution_minutes*60, behavior="raise_exception")

        dict_result = {}
        for crrt_dataset in list_entries_ref_segment:
            dict_result[crrt_dataset] = dict_station_data[crrt_dataset]

        # check that all time bases are identical
        time_base_ref = [crrt_datetime for (crrt_datetime, data) in dict_result[list_entries_ref_segment[0]]]
        for crrt_dataset in list_entries_ref_segment:
            crrt_time_base = [crrt_datetime for (crrt_datetime, data) in dict_result[crrt_dataset]]
            if not crrt_time_base == time_base_ref:
                logger.warning("some prints of crrt_time_base")
                logger.warning(crrt_time_base[:10])
                logger.warning(crrt_time_base[-10:])
                logger.warning(len(crrt_time_base))
                logger.warning("some prints of time_base_ref")
                logger.warning(time_base_ref[:10])
                logger.warning(time_base_ref[-10:])
                logger.warning(len(time_base_ref))
                raise ValueError("Mismatch between the current time base, and the reference time baseQ")

        return(dict_result)


class AccessStormSurgeNetCDF():
    """Simple access and visualization of NetCDF files generated through the KartverketAPI class."""

    def __init__(self, path_to_NetCDF=None):
        """Accessing and plotting storm surge data saved in a netcdf4 file by the KartverkerAPI
        class.

        Input:
            - path_to_NetCDF: the path to the data. If None (default), use the default path."""
        if path_to_NetCDF is None:
            self.path_to_NetCDF = os.getcwd() + "/data_kartverket_stormsurge.nc4"
        else:
            self.path_to_NetCDF = path_to_NetCDF

        self.explore_information()

    def explore_information(self):
        """print a few high-level informations about the netcdf data"""

        with nc4.Dataset(self.path_to_NetCDF, "r", format="NETCDF4") as nc4_fh:
            self.station_ids = nc4_fh["stationid"][:]
            self.number_of_stations = len(self.station_ids)
            self.first_timestamp = int(nc4_fh["timestamp"][0])
            self.last_timestamp = int(nc4_fh["timestamp"][-1])

        self.dict_metadata = self.get_dict_stations_metadata()

    def get_dict_stations_metadata(self):
        dict_stations_metadata = {}

        with nc4.Dataset(self.path_to_NetCDF, "r", format="NETCDF4") as nc4_fh:
            for crrt_ind in range(self.number_of_stations):
                crrt_station_id = nc4_fh["stationid"][crrt_ind]
                crrt_lat = nc4_fh["latitude"][crrt_ind]
                crrt_lon = nc4_fh["longitude"][crrt_ind]
                datetime_start = datetime.datetime.fromtimestamp((nc4_fh["timestamp_start"][crrt_ind].data))
                datetime_end = datetime.datetime.fromtimestamp((nc4_fh["timestamp_end"][crrt_ind].data))

                crrt_dict_metadata = {}
                crrt_dict_metadata["station_index"] = crrt_ind
                crrt_dict_metadata["nc4_dump_index"] = crrt_ind
                crrt_dict_metadata["latitude"] = crrt_lat
                crrt_dict_metadata["longitude"] = crrt_lon
                crrt_dict_metadata["datetime_start"] = datetime_start
                crrt_dict_metadata["datetime_end"] = datetime_end

                dict_stations_metadata[crrt_station_id] = crrt_dict_metadata

        return dict_stations_metadata

    def visualize_available_times(self, date_start=None, date_end=None):
        """Visualize the time over which data are available for each station.
        This is based on the specific API request about data availability.

        Inputs:
            - date_start, date_end: dates over which we want to check if data
                are available. If None (default), ignore time bounds for the
                and do no availability check.

        Output:
            Displays a plot showing the stations, the time extent over which
                data are available, and, if provided, the time bounds date_start
                and date_end."""

        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(18, 5))

        for crrt_station_id in self.station_ids:
            crrt_station_index = self.dict_metadata[crrt_station_id]["station_index"]

            if crrt_station_index % 2 == 0:
                crrt_color = "b"
            else:
                crrt_color = 'k'

            crrt_min_time = self.dict_metadata[crrt_station_id]["datetime_start"]
            crrt_max_time = self.dict_metadata[crrt_station_id]["datetime_end"]

            logger.info("{} to {}".format(crrt_min_time, crrt_max_time))

            plt.plot([crrt_min_time, crrt_max_time], [crrt_station_index, crrt_station_index], linewidth=3.5, color=crrt_color)

            plt.text(datetime.datetime(1980, 1, 1), crrt_station_index, "#{:02}{} {}.{:02}-{}.{:02}".format(crrt_station_index, crrt_station_id, crrt_min_time.year, crrt_min_time.month, crrt_max_time.year, crrt_max_time.month), color=crrt_color)

            if date_start is not None and date_end is not None:
                if (date_start > crrt_min_time and date_end < crrt_max_time):
                    plt.text(datetime.datetime(1985, 6, 1), crrt_station_index, "Y", color="g")
                else:
                    plt.text(datetime.datetime(1985, 6, 1), crrt_station_index, "N", color="r")

        if date_start is not None and date_end is not None:
            plt.axvline(date_start, linewidth=2.5, color="orange")
            plt.axvline(date_end, linewidth=2.5, color="orange")

        mpl_min_time = mdates.date2num(datetime.datetime(1980, 1, 1))
        mpl_max_time = mdates.date2num(datetime.datetime(2020, 12, 1))

        plt.xlim([mpl_min_time, mpl_max_time])
        plt.ylabel("station number")

        plt.show()

    def visualize_station_positions(self):
        """Visualize the position of the stations."""

        # The data to plot are defined in lat/lon coordinate system, so PlateCarree()
        # is the appropriate choice of coordinate reference system:
        _ = ccrs.PlateCarree()

        # the map projection properties.
        proj = ccrs.LambertConformal(central_latitude=65.0,
                                    central_longitude=15.0,
                                    standard_parallels=(52.5, 75.0))

        plt.figure(figsize=(15, 18))
        ax = plt.axes(projection=proj)

        ax.add_feature(cfeature.LAND) #If I comment this => all ok, but I need
        ax.add_feature(cfeature.LAKES)
        ax.add_feature(cfeature.RIVERS)
        ax.set_global()
        ax.coastlines()

        list_lats = []
        list_lons = []
        list_names = []

        for crrt_station_id in self.station_ids:
            list_lats.append(self.dict_metadata[crrt_station_id]["latitude"])
            list_lons.append(self.dict_metadata[crrt_station_id]["longitude"])
            list_names.append(crrt_station_id)

        ax.scatter(list_lons, list_lats, transform=ccrs.PlateCarree(), color="red")

        transform = ccrs.PlateCarree()._as_mpl_transform(ax)
        for crrt_station_index in range(self.number_of_stations):
            ax.annotate("#{}{}".format(crrt_station_index, list_names[crrt_station_index]), xy=(list_lons[crrt_station_index], list_lats[crrt_station_index]),
                        xycoords=transform,
                        xytext=(5, 5), textcoords="offset points", color="red"
                        )

        ax.set_extent([-3.5, 32.5, 50.5, 82.5])

        plt.show()

    def visualize_single_station(self, station_id, datetime_start, datetime_end):
        """Show the data for both observation and prediction for a specific station over
        a specific time interval.

        Input:
            - station_id: the station to look at
            - datetime_start: the start of the plot
            - datetime_end: the end of the plot
        """

        timestamps, observation, prediction = self.get_data(station_id, datetime_start, datetime_end)
        datetime_timestamps = [datetime.datetime.fromtimestamp(crrt_datetime) for crrt_datetime in timestamps]

        plt.figure()

        plt.plot(datetime_timestamps, observation)
        plt.plot(datetime_timestamps, prediction)

        plt.ylim([-1000.0, 1000.0])

        plt.show()

    def get_data(self, station_id, datetime_start, datetime_end):
        """Get the data contained in the netcdf4 dump about stations_id, that
        is between times datetime_start and datetime_end.

        Input:
            - sation_id: the station ID, for example 'OSL'
            - datetime_start, datetime_end: the limits of the extracted data.

        Output:
            - data_timestamps: the timestamps of the data.
            - data_observation: the observation.
            - data_prediction: the prediction.
        """

        if not station_id in self.station_ids:
            raise ValueError("{} is not a known station (list: {})".format(station_id, self.stations_ids))

        if not isinstance(datetime_start, datetime.datetime):
            raise ValueError("datetime_start should be a datetime.datetime, got {}".format(type(datetime_start)))

        if not isinstance(datetime_end, datetime.datetime):
            raise ValueError("datetime_end should be a datetime.datetime, got {}".format(type(datetime_end)))

        if not datetime_start < datetime_end:
            raise ValueError("need datetime_start < datetime_end, but got {} and {}".format(datetime_start, datetime_end))

        if not (datetime_start > datetime.datetime.fromtimestamp(self.first_timestamp) and datetime_end < datetime.datetime.fromtimestamp(self.last_timestamp)):
            raise ValueError("requested data in range {} to {}, but nc4 file covers only {} to {}".format(datetime_start, datetime_end, datetime.datetime.fromtimestamp(self.first_timestamp), datetime.datetime.fromtimestamp(self.last_timestamp)))

        nc4_index = self.dict_metadata[station_id]["station_index"]

        timestamp_start = datetime_start.timestamp()
        timestamp_end = datetime_end.timestamp()

        with nc4.Dataset(self.path_to_NetCDF, "r", format="NETCDF4") as nc4_fh:
            data_timestamp_full = nc4_fh["timestamp"][:]
            data_observation_full = nc4_fh["observation"][nc4_index][:]
            data_prediction_full = nc4_fh["prediction"][nc4_index][:]

        first_index = moa.find_index_first_greater_or_equal(data_timestamp_full, timestamp_start)
        last_index = moa.find_index_first_greater_or_equal(data_timestamp_full, timestamp_end) + 1

        data_timestamp = data_timestamp_full[first_index:last_index]
        data_observation = data_observation_full[first_index:last_index]
        data_prediction = data_prediction_full[first_index:last_index]

        return(data_timestamp, data_observation, data_prediction)


if __name__ == "__main__":
    logger.setLevel(logging.INFO)

    date_start = datetime.date(2006, 12, 12)
    date_end = datetime.date(2014, 1, 3)

    if False:
        kartverket_api = KartverketAPI(short_test=False)

        kartverket_api.generate_netcdf_dataset(date_start, date_end)

    if True:
        kartverket_nc4 = AccessStormSurgeNetCDF("./data_kartverket_stormsurge.nc4")
        kartverket_nc4.visualize_available_times(date_to_datetime(date_start, False), date_to_datetime(date_end, False))
        # kartverket_nc4.visualize_station_positions()

        datetime_start_data = datetime.datetime(2006, 12, 15, 0, 0, 0)
        datetime_end_data = datetime.datetime(2006, 12, 30, 0, 0, 0)

        # timestamps, observation, prediction = kartverket_nc4.get_data("OSL", datetime_start_data, datetime_end_data)
        kartverket_nc4.visualize_single_station("OSL", datetime_start_data, datetime_end_data)


class NetCDFTester():
    """A class to perform checks of the correctness of the netCDF data dump a posteriori."""

    def __init__(self, path_to_ordered_cache, path_to_netCDF, limit_datetimes):
        """
        Input:
            - path_to_ordered_cache: the path to the ordered cache with the html requests that
                were used to build the netCDF database. For example: /home/jrmet/.NicedUrlRequest/cache/
            - path_to_netCDF: the path to the netCDF database.
            - limit_dates: the limit dates over which testing is allowed.
        """

        self.path_to_ordered_cache = path_to_ordered_cache
        self.path_to_netCDF = path_to_netCDF
        self.limit_datetimes = limit_datetimes

        if not os.path.exists(self.path_to_ordered_cache):
            raise ValueError("path to ordered cache {} does not exist".format(self.path_to_ordered_cache))

        if not self.path_to_ordered_cache[-1] == "/":
            raise ValueError("please terminate path_to_ordered_cache with /")

        if not Path(self.path_to_netCDF).is_file():
            raise ValueError("path to netCDF dump {} does not exist".format(self.path_to_netCDF))

        self.kartverket_nc4 = AccessStormSurgeNetCDF(self.path_to_netCDF)

    def perform_random_tests(self, n_tests=1000):
        """
        Input:
            - n_tests: the number of tests to perform at random.
        """

        list_data_folders = [x[0] for x in os.walk(self.path_to_ordered_cache) if len(x[0]) == 11 + len(self.path_to_ordered_cache)]

        for test_index in tqdm(range(n_tests)):
            crrt_folder = random.choice(list_data_folders)
            crrt_station = crrt_folder[-11:-8]

            logger.info(crrt_station)

            list_files = os.listdir(crrt_folder)
            crrt_path = "{}/{}".format(crrt_folder, random.choice(list_files))

            crrt_start_datetime_pathname = datetime.datetime.fromisoformat(crrt_path[-139:-120])
            logger.info(crrt_start_datetime_pathname)

            # parse the current file
            with open(crrt_path, 'rb') as fh:
                html_string = fh.read()
            soup = bfls(html_string, features="lxml")

            dict_segment = {}

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
                        value = float(crrt_entry["value"])
                        data_tuple = (datetime.datetime.fromisoformat(time), value)
                        dict_segment[crrt_key].append(data_tuple)

            if list(dict_segment.keys()) != []:
                crrt_key = random.choice(list(dict_segment.keys()))

                crrt_data_tuple = random.choice(dict_segment[crrt_key])

                crrt_datetime = crrt_data_tuple[0]

                if crrt_datetime.replace(tzinfo=None) > self.limit_datetimes[0] and crrt_datetime.replace(tzinfo=None) < self.limit_datetimes[1]:
                    crrt_timestamp = crrt_datetime.timestamp()
                    crrt_data = crrt_data_tuple[1]

                    logger.info("chose crrt tuple {} with key {} from station {} from file {}".format(crrt_data_tuple, crrt_key, crrt_station, crrt_path))
                    logger.info("corresponding to timestamp: {}".format(crrt_timestamp))

                    datetime_start_data = crrt_datetime.replace(tzinfo=None)
                    datetime_end_data = (crrt_datetime + datetime.timedelta(minutes=1)).replace(tzinfo=None)

                    timestamps, observation, prediction = self.kartverket_nc4.get_data(crrt_station, datetime_start_data, datetime_end_data)

                    timestamp_nc4 = timestamps[0]
                    observation_nc4 = observation[0]
                    prediction_nc4 = prediction[0]

                    logger.info("the netcdf4 datafile provided station {} timestamp {} observation {} prediction {}:".format(crrt_station, timestamp_nc4, observation_nc4, prediction_nc4))

                    assert timestamp_nc4 == crrt_timestamp

                    if "observation" in crrt_key:
                        logger.info("compare obs {} from request to {} from nc4".format(crrt_data, observation_nc4))
                        assert abs(observation_nc4 - crrt_data) < 1e-2
                    elif "prediction" in crrt_key:
                        logger.info("compare pred {} from request to {} from nc4".format(crrt_data, prediction_nc4))
                        logger.info(prediction_nc4)
                        logger.info(crrt_data)
                        assert abs(prediction_nc4 - crrt_data) < 1e-2
                    else:
                        raise ValueError("unknown key {}".format(crrt_key))

                else:
                    logger.warning("skipping test with datetime {} outside range".format(crrt_datetime))
            else:
                datetime_start_data = crrt_start_datetime_pathname.replace(tzinfo=None)
                datetime_end_data = (datetime_start_data + datetime.timedelta(minutes=1)).replace(tzinfo=None)

                if datetime_start_data > self.limit_datetimes[0] and datetime_start_data < self.limit_datetimes[1]:

                    timestamps, observation, prediction = self.kartverket_nc4.get_data(crrt_station, datetime_start_data, datetime_end_data)

                    timestamp_nc4 = timestamps[0]
                    observation_nc4 = observation[0]
                    prediction_nc4 = prediction[0]

                    logger.info("this entry is from a missing html message")
                    logger.info("the netcdf4 datafile provided station {} timestamp {} observation {} prediction {}:".format(crrt_station, timestamp_nc4, observation_nc4, prediction_nc4))

                    assert observation_nc4 > 1e8, ("observation: {}".format(observation_nc4))
                    assert prediction_nc4 > 1e8, ("predictino: {}".format(prediction_nc4))

                else:
                    logger.warning("skipping test with datetime {} outside range".format(datetime_start_data))
