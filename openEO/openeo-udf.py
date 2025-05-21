# Filter station geoParquet file
import geopandas
import logging
from openeo.udf import XarrayDataCube

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'  # Simple format showing only the message
)
logger = logging.getLogger(__name__)

def lnp(message):
    """Log and print the message"""
    logger.info(message)
    #print(message)

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    # Print context for debugging
    lnp(f"UDF CONTEXT: {context}")
    lnp(f"Vector Cube: {context._cube}")
    lnp(f"Vector Geometry: {context._geometries}")
    
    # Get the DataArray and ensure the correct dimension order
    original_array = cube.get_array()
    lnp(f"Input cube shape: {original_array.shape}")
    lnp(f"Input cube dimensions: {original_array.dims}")
    lnp(f"Input cube coordinates: {list(original_array.coords.keys())}")

    return cube
