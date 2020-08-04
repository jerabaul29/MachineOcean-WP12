"""A few helper functions to work with urls."""

import logging
import urllib.request
import time

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)


class NicedUrlRequest():
    """A simple wrapper to nice url requests.
    Make sure that the caller has to wait for a minimum amount of time between requests."""

    def __init__(self, min_wait_time_s=1):
        """min_wait_time_s: minimum time interval between requests."""
        self.min_wait_time_s = min_wait_time_s
        self.time_last = None

        # initialize with the start time -min_wait_time_s, so that immediately ready to use
        self.update_time()
        self.time_last -= self.min_wait_time_s

    def update_time(self):
        """Update the time corresponding to the last request."""
        self.time_last = time.time()

    def elapsed_since_last_request(self):
        """Time elapsed since the last request."""
        return time.time() - self.time_last

    def perform_request(self, request):
        """Perform the request request, after making sure we are not too hard on the server.

        If necessary, sleep a bit to avoid overwhelming the server with too many requests.

        Input:
            - request: the request to perform

        Output:
            - status: the status code
            - html_string: the answer html
        """

        log.info("go through request {}".format(request))

        remaining_sleep = self.min_wait_time_s - self.elapsed_since_last_request()
        log.info("remaining_sleep (negative is none needed): {}".format(remaining_sleep))

        if remaining_sleep > 0:
            log.info("sleeping")
            time.sleep(remaining_sleep)

        log.info("perform request")
        self.update_time()

        with urllib.request.urlopen(request) as response:
            status = response.status

            if not status == 200:
                raise ValueError("got status {} on request {}".format(response.status, request))

            html_string = response.read()

        log.info("successful request")

        return html_string
