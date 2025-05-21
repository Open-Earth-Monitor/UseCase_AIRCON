# Import required packages
import openeo
from openeo.processes import process

# Connect to the back-end
connection = openeo.connect("https://openeo.cloud")
connection.authenticate_oidc()

datacube1 = connection.datacube_from_process("load_url", format = "PARQUET", url = "https://zenodo.org/records/14513586/files/airquality.no2.o3.so2.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet?download=1")
load1 = connection.datacube_from_process("load_stac", url = "https://github.com/GeoScripting-WUR/VectorRaster/releases/download/exercise-data/cams_o3_2020-stac-item-gtiff.json")
load6 = connection.load_collection(collection_id = "AGERA5", spatial_extent = {"west": 5.563765056326457, "east": 7.819215368253948, "south": 51.821315223506765, "north": 52.18968462236922}, temporal_extent = ["2020-01-01T00:00:00Z", "2020-12-31T00:00:00Z"], bands = ["solar-radiation-flux", "wind-speed"])

def reducer1(data, context = None):
    first1 = process("first", data = data)
    return first1

aggregate4 = load6.aggregate_temporal(intervals = [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]], reducer = reducer1)
merge8 = aggregate4.process("merge_cubes", cube2 = aggregate4, cube1 = load1)

def process2(data, context = None):
    run1 = process("run_udf", data = data, context = context, runtime = "Python", udf = "# Filter station geoParquet file\nimport geopandas\nimport logging\nfrom openeo.udf import XarrayDataCube\n\n# Configure logging\nlogging.basicConfig(\n    level=logging.INFO,\n    format='%(message)s'  # Simple format showing only the message\n)\nlogger = logging.getLogger(__name__)\n\ndef lnp(message):\n    \"\"\"Log and print the message\"\"\"\n    logger.info(message)\n    #print(message)\n\ndef apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:\n    # Print context for debugging\n    lnp(f\"UDF CONTEXT: {context}\")\n    lnp(f\"Vector Cube: {context._cube}\")\n    lnp(f\"Vector Geometry: {context._geometries}\")\n    \n    # Get the DataArray and ensure the correct dimension order\n    original_array = cube.get_array()\n    lnp(f\"Input cube shape: {original_array.shape}\")\n    lnp(f\"Input cube dimensions: {original_array.dims}\")\n    lnp(f\"Input cube coordinates: {list(original_array.coords.keys())}\")\n\n    return cube\n")
    return run1

apply3 = datacube1.apply_dimension(data = merge8, process = process2, dimension = "t")
save9 = apply3.save_result(format = "GTIFF")

# The process can be executed synchronously (see below), as batch job or as web service now
result = connection.execute(save9)
