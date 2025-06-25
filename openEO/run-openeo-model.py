# Import required packages
import openeo
from openeo.processes import process

# Connect to the back-end

#connection = openeo.connect("https://openeo.dataspace.copernicus.eu")

connection = openeo.connect("https://openeo.cloud")
connection.authenticate_oidc()

datacube1 = connection.datacube_from_process("load_url", format = "PARQUET", url = "https://zenodo.org/records/14513586/files/airquality.no2.o3.so2.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet?download=1")
datacube2 = connection.load_url(format = "PARQUET", url = "https://zenodo.org/records/14513586/files/airquality.no2.o3.so2.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet?download=1")



load1 = connection.datacube_from_process("load_stac", url = "https://github.com/GeoScripting-WUR/VectorRaster/releases/download/exercise-data/cams_o3_2020-stac-item-gtiff.json")

# Define the bounding box for the area of interest in WGS84
bbox = {"west": 5.563765056326457, "east": 7.819215368253948, "south": 51.821315223506765, "north": 52.18968462236922}

# Load covariate data for a specific extent to avoid an 'ExtentTooLarge' error.
# The vector data will be filtered to this extent inside the UDF.
load6 = connection.load_collection(
    collection_id="AGERA5",
    spatial_extent=bbox,
    temporal_extent=["2020-01-01T00:00:00Z", "2020-12-31T00:00:00Z"],
    bands=["solar-radiation-flux", "wind-speed"]
)

def reducer1(data, context = None):
    first1 = process("first", data = data)
    return first1

aggregate4 = load6.aggregate_temporal(intervals = [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]], reducer = reducer1)
merge8 = aggregate4.process("merge_cubes", cube2 = aggregate4, cube1 = load1)

# udf = openeo.UDF.from_file(
#    "openeo-udf.py",
#    context=None,  # No context needed for this example
# )

with open('openeo-udf.py', 'r') as file:
    udfcontent = file.read()

def process2(data, context = None):
    udf = process("run_udf", data = data, context = context, runtime = "Python", udf = udfcontent)
    return udf


apply3 = merge8.apply_dimension(process = process2, dimension = "t", context=datacube2)
#save9 = apply3.save_result(format = "GTIFF")

# The process can be executed synchronously (see below), as batch job or as web service now
#result = connection.execute(save9)

apply3.execute_batch("openeo.tif", out_format="GTiff")
