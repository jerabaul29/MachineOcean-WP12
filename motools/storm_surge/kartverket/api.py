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
        pass

    def get_stations_info(self):
        request = "http://api.sehavniva.no/tideapi.php?tide_request=stationlist&type=perm"
        html_string = perform_request(request)
        soup = bfls(html_string, features="lxml")
        list_tags = soup.find('stationinfo').select('location')

        dict_all_stations_info = {}

        for crrt_tag in list_tags:
            dict_tag = crrt_tag.attrs
            dict_all_stations_info[dict_tag["code"]] = dict_tag

        return(dict_all_stations_info)

