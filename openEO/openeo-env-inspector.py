# /// script
# ///

import logging
import sys
from openeo.udf import XarrayDataCube

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    versions = []
    versions.append(f"Python version: {sys.version}")

    try:
        import pandas
        versions.append(f"pandas version: {pandas.__version__}")
    except ImportError:
        versions.append("pandas is not installed.")

    try:
        import xarray
        versions.append(f"xarray version: {xarray.__version__}")
    except ImportError:
        versions.append("xarray is not installed.")

    try:
        import geopandas
        versions.append(f"geopandas version: {geopandas.__version__}")
    except ImportError:
        versions.append("geopandas is not installed.")

    try:
        import sklearn
        versions.append(f"scikit-learn version: {sklearn.__version__}")
    except ImportError:
        versions.append("scikit-learn is not installed.")

    try:
        import pykrige
        versions.append(f"pykrige version: {pykrige.__version__}")
    except ImportError:
        versions.append("pykrige is not installed.")

    try:
        import numpy
        versions.append(f"numpy version: {numpy.__version__}")
    except ImportError:
        versions.append("numpy is not installed.")

    # Combine all version info into a single string and raise an error
    # to ensure it gets logged by the backend.
    error_message = "\n".join(versions)
    raise RuntimeError(f"--- Environment Inspection ---\n{error_message}")

    # This part is now unreachable
    return cube
