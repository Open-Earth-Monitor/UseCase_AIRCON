# Import required packages
import openeo
from openeo.processes import process

# Connect to the back-end
connection = openeo.connect("https://openeo.cloud")
connection.authenticate_oidc()

spatial_extent = {"west": 5.563765056326457, "east": 7.819215368253948, "south": 51.821315223506765, "north": 52.18968462236922}
temporal_extent = ["2020-01-01T00:00:00Z", "2020-12-31T00:00:00Z"]

reference_points = connection.load_url(format = "PARQUET", url = "https://zenodo.org/records/14513586/files/airquality.no2.o3.so2.pm10.pm2p5_4.annual_pnt_20150101_20231231_eu_epsg.3035_v20240718.parquet?download=1")
cams_o3 = connection.datacube_from_process("load_stac", url = "https://github.com/GeoScripting-WUR/VectorRaster/releases/download/exercise-data/cams_o3_2020-stac-item-gtiff.json")
era5 = connection.load_collection(collection_id = "AGERA5", spatial_extent = spatial_extent, temporal_extent = temporal_extent, bands = ["solar-radiation-flux", "wind-speed"])

## EODC only - try to automatically get a STAC item and then run the rest
#clc = connection.load_collection(collection_id = "corine_land_cover", spatial_extent = spatial_extent, temporal_extent = ["2018-01-01T00:00:00Z", "2018-12-15T00:00:00Z"])

#def clc_natural_areas(x, context = None):
#    return process("and", x = process("gt", x = x, y = 22), y = process("lt", x = x, y = 35))

#clc_nat = clc.apply(process = clc_natural_areas)
clc_nat = connection.datacube_from_process("load_stac", url = "https://openeo.eodc.eu/openeo/1.1.0/jobs/d4d446fa-5e66-4c2a-b567-e906a4cdc431/results?Expires=1748440233&KeyName=SIGN_KEY_1&UserId=7c1932a6-9479-49c1-9114-8bcaaa4a65dd&Signature=ZLM88NoFKvWMn8X9nYt80NSHFLU=")






def first(data, context = None):
    first1 = process("first", data = data)
    return first1

era5_2020 = era5.aggregate_temporal(intervals = [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]], reducer = first)
era5_cams = era5_2020.process("merge_cubes", cube2 = era5_2020, cube1 = cams_o3)
covariates = era5_cams
#covariates = era5_cams.process("merge_cubes", cube2 = era5_cams, cube1 = clc_nat)

with open("openeo-udf.py", 'r') as file:
    udfcontent = file.read()

def udf_process(data, context = None):
    udf = process("run_udf", data = data, context = context, runtime = "Python", udf = udfcontent)
    return udf

result = covariates.apply_dimension(process = udf_process, dimension = "t", context=reference_points)

result.execute_batch("openeo.tif", out_format="GTiff")
