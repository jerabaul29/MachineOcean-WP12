import datetime
import tempfile
from motools.storm_surge.kartverket.api import KartverketAPI, AccessStormSurgeNetCDF

temp_dir = tempfile.TemporaryDirectory()
tmpdirname = temp_dir.name
with tempfile.TemporaryDirectory() as tmpdirname:
    kartverket_api = KartverketAPI(short_test=False)

    path_to_NetCDF = "{}/netcdf_test.nc4".format(tmpdirname)

    date_start = datetime.date(2009, 12, 8)
    date_end = datetime.date(2009, 12, 10)

    kartverket_api.generate_netcdf_dataset(date_start, date_end, path=path_to_NetCDF)

    kartverket_nc4 = AccessStormSurgeNetCDF(path_to_NetCDF=path_to_NetCDF)
    kartverket_nc4.visualize_available_times()
    kartverket_nc4.visualize_station_positions()
