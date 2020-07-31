This folder contains tools for accessing the API from kartverket.no about storm surge observations.

The main document documenting the API is attached, and available at:
http://api.sehavniva.no/tideapi_protocol.pdf

In addition, for an example of something that has been used in ROMS query of data:
wget --tries=5 -O /lustre/storeB/project/metproduction/products/util/vannstand/sehavniva.xml "https://api.sehavniva.no/tideapi.php?stationcode=&fromtime=2020-07-01T11:00:00&totime=2020-08-20T11:00:00&datatype=all&refcode=msl&place=&file=&lang=nn&interval=60&dst=2&tzone=utc&tide_request=stationdata"

As a side note, there is also a web-based html interface to the API:
http://api.sehavniva.no/tideapi_en.html

And the entry point for finding this documentation was:
https://www.kartverket.no/en/sehavniva/api-for-water-level-data-and-widget/
