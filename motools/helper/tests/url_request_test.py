"""tests"""

import time
import tempfile
from motools.helper.url_request import NicedUrlRequest
from bs4 import BeautifulSoup as bfls4
import json


def double_request(cache_folder=None, expected_time_min=1.0, expected_time_max=2.0):
    """a double request using a publicly available url, as recommended on:
        https://stackoverflow.com/questions/5725430/http-test-server-accepting-get-post-requests

    Input:
        - cache_folder: the cache_folder option to be transmitted down.
        - expected_time_min: the min time expected
        - expected_time_max: the max time expected
    """

    niced_requester = NicedUrlRequest(cache_folder=cache_folder)
    time_start = time.time()

    html_string = niced_requester.perform_request("http://httpbin.org/get?bla=blabla")
    soup = bfls4(html_string, features="lxml")
    dict_data_html = json.loads(soup.findAll("p")[0].text)
    assert dict_data_html["args"]["bla"] == "blabla"

    html_string = niced_requester.perform_request("http://httpbin.org/get?bla2=blabla2")
    soup = bfls4(html_string, features="lxml")
    dict_data_html = json.loads(soup.findAll("p")[0].text)
    assert dict_data_html["args"]["bla2"] == "blabla2"

    time_end = time.time()

    assert time_end - time_start > expected_time_min
    assert time_end - time_start < expected_time_max

def test_url_request_timing():
    """test that the timing is right and we do not cause DoS. No cache
    used."""

    double_request()


def test_caching():
    """test basic caching functionality."""

    # using as cache any entry
    with tempfile.TemporaryDirectory() as tmpdirname:
        double_request(cache_folder=tmpdirname, expected_time_min=1.0, expected_time_max=2.0)
        double_request(cache_folder=tmpdirname, expected_time_min=0.0, expected_time_max=0.5)
