import rioxarray as rxr, geopandas as gpd
from functions_py import load_aq, load_covariates, linear_aq_model, predict_linear_model, krige_aq_residuals, combine_results

aq = load_aq("PM10", "perc", 2015, m=1)
dem = rxr.open_rasterio("supplementary/static/COP-DEM/... .tif").sel(band=1)
cov = load_covariates(aq, dem)
model = linear_aq_model(aq, cov)
cov["lm_pred"] = predict_linear_model(model, cov)
kr = krige_aq_residuals(aq, cov, model)
result = combine_results(cov, kr)