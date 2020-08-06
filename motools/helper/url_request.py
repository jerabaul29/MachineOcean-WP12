"""A few helper functions to work with urls."""

import os.path
import urllib.request
import time
import datetime
from pathlib import Path
from motools import logger

# NOTE: for now caching is done by hand in the class; consider using established packages such as:
# http://www.grantjenks.com/docs/diskcache/tutorial.html
# https://fcache.readthedocs.io/en/stable/


class NicedUrlRequest():
    """A simple wrapper to nice url requests.
    Make sure that the caller has to wait for a minimum amount of time between requests."""

    def __init__(self, min_wait_time_s=1, cache_folder="default"):
        """
        - min_wait_time_s: minimum time interval between requests.
        - cache_folder: properties for caching the data. Can be: None (no caching),
            "default" (use ./NicedUrlRequest/cache folder in home dir), or any custom
            valid path.
        """

        self.min_wait_time_s = min_wait_time_s
        self.time_last = None

        # initialize with the start time -min_wait_time_s, so that immediately ready to use
        self.update_time()
        self.time_last -= self.min_wait_time_s

        # use the right cache folder, make sure valid / terminated both if default
        # and user specified
        self.cache_folder = cache_folder
        if cache_folder == "default":
            self.cache_folder = str(Path.home()) + "/.NicedUrlRequest/cache"

        if cache_folder is not None:
            self.cache_folder += "/"

        if self.cache_folder is not None and not os.path.exists(cache_folder):
            Path(self.cache_folder).mkdir(parents=True)

        logger.info("the cache folder is set to {}".format(self.cache_folder))

        self.cache_warning()

        # TODO: add functions for cleaning the cache: all, size_max, age_max, nbr_max

    def cache_warning(self, cache_warning_size=50*(2.0**30), age_warning_days=90, nbr_files_warning=100):
        """warns if cache reaches some given metrics.

        - cache_warning_size: the size above which we get a cache warning due to the size. Default
            is 50 GB.
        - age_warning_days: the age above which we get a cache warning due to old age file. Default
            is 90 days.
        """

        cache_warning_met = False

        if self.cache_folder is not None:
            root_of_cache = Path(self.cache_folder)
            size_cache_content = sum(f.stat().st_size for f in root_of_cache.glob('**/*') if f.is_file())

            if size_cache_content > cache_warning_size:
                logger.warning("large NicedUrlRequest cache size of {}GB at location {}".format(size_cache_content / 2.0**30, self.cache_folder))
                cache_warning_met = True

            sorted_files_time = sorted(root_of_cache.glob('**/*'), key = lambda x: x.stat().st_ctime)

            for crrt_file in sorted_files_time:
                crrt_age_in_days = (datetime.datetime.fromtimestamp(crrt_file.stat().st_ctime)-datetime.datetime.now()).days
                if crrt_age_in_days < -age_warning_days:
                    logger.warning("NicedUrlRequest cache file {} is old: {} days".format(crrt_file, crrt_age_in_days))
                    cache_warning_met = True

            if len(sorted_files_time) > nbr_files_warning:
                logger.warning("large NicedUrlRequest cache number of files: {} in total".format(len(sorted_files_time)))
                cache_warning_met = True

            if cache_warning_met:
                logger.warning("you should clean your cache at: {}".format(str(self.cache_folder)))


    def update_time(self):
        """Update the time corresponding to the last request."""
        self.time_last = time.time()

    def elapsed_since_last_request(self):
        """Time elapsed since the last request."""
        return time.time() - self.time_last

    def path_in_cache(self, request):
        if self.cache_folder is None:
            return(None)
        else:
            return(self.cache_folder + request.replace("/", ""))

    def perform_request(self, request, ignore_cache=False, allow_caching=True):
        """Perform the request request, after checking if the data are available in cache,
        and making sure we are not too hard on the server.

        If necessary, sleep a bit to avoid overwhelming the server with too many requests.

        Input:
            - request: the request to perform
            - ignore_cache: boolean, if True ignore the cache entry and perform the request
                anyways, if False uses cached value if available.
            - allow_caching: boolean, if True allow caching, if False not, default True.

        Output:
            - status: the status code
            - html_string: the answer html
        """

        if not isinstance(request, str):
            raise ValueError("request should be a string, but got {}".format(request))

        if not isinstance(ignore_cache, bool):
            raise ValueError("ignore_cache should be a bool, but got {}".format(ignore_cache))

        if not isinstance(allow_caching, bool):
            raise ValueError("allow_caching should be a bool, but got {}".format(allow_caching))

        logger.info("go through request {}".format(request))

        if self.path_in_cache(request) is not None and not ignore_cache and Path(self.path_in_cache(request)).is_file():
            logger.info("the request is already available in cache, and we are allowed to use it")

            with open(self.path_in_cache(request), 'rb') as fh:
                html_string = fh.read()

        else:
            remaining_sleep = self.min_wait_time_s - self.elapsed_since_last_request()
            logger.info("remaining_sleep (negative is none needed): {}".format(remaining_sleep))

            if remaining_sleep > 0:
                logger.info("sleeping")
                time.sleep(remaining_sleep)

            logger.info("perform request")
            self.update_time()

            with urllib.request.urlopen(request) as response:
                status = response.status

                if not status == 200:
                    raise ValueError("got status {} on request {}".format(response.status, request))

                html_string = response.read()

            logger.info("successful request")

            if self.path_in_cache(request) is not None and allow_caching:
                logger.info("we are allowed to cache this request; caching")

                with open(self.path_in_cache(request), "wb") as fh:
                    fh.write(html_string)

        return html_string
