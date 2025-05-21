"""Python analogue of the original `AQ_Demo.R` workflow.

It demonstrates how to load measurement data, build a linear model,
interpolate residuals with kriging, merge predictions, and plot results
using the helper functions defined in `functions.py`.

Run with:
    python AQ_Demo.py

The script assumes the same folder structure as the R version and that you
have activated the conda env from `environment.yml`.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import matplotlib.pyplot as plt
import rioxarray as rxr

from functions import (
    combine_results,
    krige_aq_residuals,
    linear_aq_model,
    load_aq,
    load_covariates,
    predict_linear_model,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Parameters (mirror the R script)
# -----------------------------------------------------------------------------

year = 2015
month = 1
pollutant = "PM10"
stat = "perc"
station_area_type = "RB"  # placeholder – filter not implemented yet

# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------

LOG.info("Loading AQ measurements …")
aq = load_aq(pollutant, stat, year, m=month)

# DEM (replace with actual file available to you)
DEM_PATH = Path("Data/aq_cov_o3_2020_dem.tiff")
if not DEM_PATH.exists():
    raise FileNotFoundError(
        f"Cannot find DEM raster: {DEM_PATH}. Update `DEM_PATH` to point to a valid raster file."
    )

dem = rxr.open_rasterio(DEM_PATH, masked=True).sel(band=1)

LOG.info("Loading covariates …")
t0 = time.time()
aq_cov = load_covariates(aq, dem)
LOG.info("Covariate load finished in %.1fs", time.time() - t0)

LOG.info(f"--- Covariate Inspection (Pollutant: {pollutant}, Year: {year}, Month: {month if month > 0 else 'Annual'}) ---")
LOG.info(f"Loaded covariate layers: {list(aq_cov.data_vars.keys())}")
LOG.info(f"Covariate dataset attributes: {aq_cov.attrs}")
for var_name in aq_cov.data_vars:
    LOG.info(f"  Layer '{var_name}': shape {aq_cov[var_name].shape}, coords {list(aq_cov[var_name].coords.keys())}")
LOG.info("--- End Covariate Inspection ---")

# -----------------------------------------------------------------------------
# Modelling
# -----------------------------------------------------------------------------

LOG.info("Fitting linear model …")
linmod = linear_aq_model(aq, aq_cov)

# Predict over raster grid
LOG.info("Predicting linear model over covariate grid …")
aq_cov["lm_pred"] = predict_linear_model(linmod, aq_cov)

# -----------------------------------------------------------------------------
# Residual kriging
# -----------------------------------------------------------------------------

LOG.info("Kriging residuals … this may take a while …")
t0 = time.time()
krige_res = krige_aq_residuals(aq, aq_cov, linmod, n_max=10)
LOG.info("Kriging finished in %.1fs", time.time() - t0)

# -----------------------------------------------------------------------------
# Combine & plot
# -----------------------------------------------------------------------------

result = combine_results(aq_cov, krige_res)

fig, ax = plt.subplots(1, 2, figsize=(12, 5))
_ = ax  # type: ignore

from functions import plot_aq_prediction  # lazy import to avoid circular @ top-level

plot_aq_prediction(result, ax=ax[0])
ax[0].set_title("Predicted AQ")

try:
    from functions import plot_aq_se

    plot_aq_se(result, ax=ax[1])
    ax[1].set_title("Prediction Std. Error")
except KeyError:
    ax[1].text(0.5, 0.5, "`pred_se` layer missing", ha="center", va="center")
    ax[1].axis("off")

plt.tight_layout()
plt.show()

LOG.info("Workflow finished successfully.")
