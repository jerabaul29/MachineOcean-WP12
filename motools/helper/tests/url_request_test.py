"""tests"""

import time
from motools.helper.url_request import NicedUrlRequest
from bs4 import BeautifulSoup as bfls4
import json

def test_url_request_timing():
    """test that the timing is right and we do not cause DoS. The url
    used was recommended here:
        https://stackoverflow.com/questions/5725430/http-test-server-accepting-get-post-requests
    """

    niced_requester = NicedUrlRequest()
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

    assert time_end - time_start > 1.0  # make sure forces to wait
    assert time_end - time_start < 2.0  # check not too slow either...

