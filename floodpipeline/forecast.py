from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    PipelineDataSets,
    ForecastDataUnit,
    FloodForecast,
    ForecastStationDataUnit,
)
from floodpipeline.load import Load
from datetime import datetime
from typing import List
from shapely import Polygon
from shapely.geometry import shape
import pandas as pd
from rasterstats import zonal_stats
import os
import numpy as np
import xarray as xr
import rasterio
import rioxarray
import math
from rasterio.transform import from_origin
from rasterio.warp import reproject, Resampling, transform_bounds
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.features import shapes
import shutil


def merge_rasters(raster_filepaths: list) -> tuple:
    """Merge rasters into a single one, return the merged raster and its metadata"""
    if len(raster_filepaths) > 0:
        with rasterio.open(raster_filepaths[0]) as src:
            out_meta = src.meta.copy()
    mosaic, out_trans = merge(raster_filepaths)
    out_meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
        }
    )
    return mosaic, out_meta


def clip_raster(
    raster_filepath: str, shapes: List[Polygon], invert: bool = False
) -> tuple:
    """Clip raster with a list of polygons, return the clipped raster and its metadata"""
    crop = True if not invert else False
    with rasterio.open(raster_filepath) as src:
        outImage, out_transform = mask(src, shapes, crop=crop, invert=invert)
        outMeta = src.meta.copy()
    outMeta.update(
        {
            "driver": "GTiff",
            "height": outImage.shape[1],
            "width": outImage.shape[2],
            "transform": out_transform,
            "compress": "lzw",
        }
    )
    return outImage, outMeta


def classify_alert(
    triggered: str,
    likelihood_per_return_period: dict,
    classify_alert_on: str,
    alert_on_return_period,
    alert_on_minimum_probability,
) -> str:
    """
    Classify alert based as specified in config file
    """
    alert_class = "no"
    if classify_alert_on == "return-period":
        if (
            type(alert_on_return_period) != dict
            and type(alert_on_minimum_probability) != float
        ):
            raise ValueError(
                "to classify alerts on return period, alert-on-return-period should be a dictionary "
                "and alert-on-minimum-probability should be a float"
            )
        alert_on_return_period = {
            k: v
            for k, v in sorted(alert_on_return_period.items(), key=lambda item: item[1])
        }  # order by return period, from smallest to largest
        for class_, return_period in alert_on_return_period.items():
            if (
                likelihood_per_return_period[return_period]
                >= alert_on_minimum_probability
            ):
                alert_class = class_
    elif classify_alert_on == "probability":
        if (
            type(alert_on_minimum_probability) != dict
            and type(alert_on_return_period) != float
        ):
            raise ValueError(
                "to classify alerts on minimum probability, alert-on-minimum-probability should be a dictionary "
                "and alert-on-return-period should be a float"
            )
        alert_on_minimum_probability = {
            k: v
            for k, v in sorted(
                alert_on_minimum_probability.items(), key=lambda item: item[1]
            )
        }  # order by probability, from smallest to largest
        for class_, minimum_probability in alert_on_minimum_probability.items():
            if (
                likelihood_per_return_period[alert_on_return_period]
                >= minimum_probability
            ):
                alert_class = class_
    elif classify_alert_on == "disable":
        if triggered and type(alert_on_return_period) == dict:
            alert_class = max(alert_on_return_period, key=alert_on_return_period.get)
        elif triggered and type(alert_on_minimum_probability) == dict:
            alert_class = max(
                alert_on_minimum_probability, key=alert_on_minimum_probability.get
            )
    else:
        raise ValueError(
            "classify-alert-on should be either 'return-period' or 'probability' or 'disable"
        )
    return alert_class


class Forecast:
    """
    Forecast flood events based on river discharge data
    """

    def __init__(
        self,
        settings: Settings = None,
        secrets: Secrets = None,
        data: PipelineDataSets = None,
    ):
        self.secrets = None
        self.settings = None
        self.set_settings(settings)
        self.set_secrets(secrets)
        self.load = Load(settings=self.settings, secrets=self.secrets)
        self.input_data_path: str = "data/input"
        self.output_data_path: str = "data/output"
        self.flood_extent_raster: str = self.output_data_path + "/flood_extent.tif"
        self.pop_raster: str = self.input_data_path + "/population_density.tif"
        self.aff_pop_raster: str = self.output_data_path + "/affected_population.tif"
        self.data = data

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        settings.check_settings(["global_flood_maps_url"])
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets based on the data source"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets([])
        self.secrets = secrets

    def compute_forecast(self):
        """
        Forecast floods based on river discharge data
        """
        os.makedirs(self.input_data_path, exist_ok=True)
        os.makedirs(self.output_data_path, exist_ok=True)
        self.compute_forecast_admin()
        self.compute_forecast_station()

    def compute_forecast_admin(self):
        """
        Forecast floods per admin division based on river discharge data
        1. determine if trigger level is reached, with which probability, and alert class
        2. compute flood extent
        3. compute people affected
        """
        self.__compute_triggers()
        self.__compute_flood_extent()
        if self.data.forecast_admin.is_any_triggered():
            self.__compute_affected_pop()

    def __compute_triggers(self):
        """Determine if trigger level is reached, its probability, and the alert class"""

        country = self.data.discharge_admin.country
        trigger_on_lead_time = self.settings.get_country_setting(
            country, "trigger-on-lead-time"
        )
        trigger_on_return_period = self.settings.get_country_setting(
            country, "trigger-on-return-period"
        )
        trigger_on_minimum_probability = self.settings.get_country_setting(
            country, "trigger-on-minimum-probability"
        )
        classify_alert_on = self.settings.get_country_setting(
            country, "classify-alert-on"
        )
        alert_on_return_period = self.settings.get_country_setting(
            country, "alert-on-return-period"
        )
        alert_on_minimum_probability = self.settings.get_country_setting(
            country, "alert-on-minimum-probability"
        )

        pcode_not_found = []
        for pcode in self.data.discharge_admin.get_pcodes():
            try:
                threshold_data_unit = self.data.threshold_admin.get_data_unit(pcode)
            except (KeyError, ValueError, TypeError):
                pcode_not_found.append(pcode)
                continue
            for lead_time in self.data.discharge_admin.get_lead_times():
                discharge_data_unit = self.data.discharge_admin.get_data_unit(
                    pcode, lead_time
                )
                adm_level = discharge_data_unit.adm_level

                # calculate likelihood per return period
                likelihood_per_return_period, forecasts = {}, []
                for threshold in threshold_data_unit.thresholds:
                    threshold_checks = map(
                        lambda x: 1 if x > threshold["threshold_value"] else 0,
                        discharge_data_unit.discharge_ensemble,
                    )
                    likelihood = sum(threshold_checks) / len(
                        discharge_data_unit.discharge_ensemble
                    )
                    return_period = threshold["return_period"]
                    likelihood_per_return_period[return_period] = likelihood
                    forecasts.append(
                        FloodForecast(
                            return_period=return_period, likelihood=likelihood
                        )
                    )

                # determine if triggered and the corresponding return period
                triggered = (
                    True
                    if likelihood_per_return_period[trigger_on_return_period]
                    >= trigger_on_minimum_probability
                    and lead_time <= trigger_on_lead_time
                    else False
                )
                return_period = next(
                    (
                        key
                        for key, value in reversed(likelihood_per_return_period.items())
                        if value >= trigger_on_minimum_probability
                    ),
                    0.0,
                )

                # determine the alert class
                alert_class = classify_alert(
                    triggered,
                    likelihood_per_return_period,
                    classify_alert_on,
                    alert_on_return_period,
                    alert_on_minimum_probability,
                )

                forecast_data_unit = ForecastDataUnit(
                    adm_level=adm_level,
                    pcode=pcode,
                    lead_time=lead_time,
                    forecasts=forecasts,
                    triggered=triggered,
                    return_period=return_period,
                    alert_class=alert_class,
                )
                self.data.forecast_admin.upsert_data_unit(forecast_data_unit)

        # check if any lower admin division is triggered but the upper one isn't, if so, trigger the upper one as well
        gdf_adms = {}
        for adm_level in self.data.forecast_admin.adm_levels:
            gdf_adm = self.load.get_adm_boundaries(
                self.data.forecast_admin.country, adm_level
            )
            gdf_adms[adm_level] = gdf_adm
        for adm_level in self.data.forecast_admin.adm_levels:
            for forecast_data_unit in self.data.forecast_admin.get_data_units(
                adm_level=adm_level
            ):
                if not forecast_data_unit.triggered:
                    lower_adm_level = adm_level + 1
                    if lower_adm_level in self.data.forecast_admin.adm_levels:
                        # get lower admin divisions contained in the current admin division
                        pcodes_in_current_adm = gdf_adms[lower_adm_level][
                            gdf_adms[lower_adm_level][f"adm{adm_level}_pcode"]
                            == forecast_data_unit.pcode
                        ][f"adm{lower_adm_level}_pcode"].tolist()
                        lower_forecast_data_units = (
                            self.data.forecast_admin.get_data_units(
                                adm_level=lower_adm_level,
                                lead_time=forecast_data_unit.lead_time,
                            )
                        )
                        for lower_forecast_data_unit in lower_forecast_data_units:
                            if (
                                lower_forecast_data_unit.pcode in pcodes_in_current_adm
                                and lower_forecast_data_unit.triggered
                            ):
                                forecast_data_unit.triggered = True
                                forecast_data_unit.alert_class = (
                                    lower_forecast_data_unit.alert_class
                                )

    def __compute_flood_extent(self):
        """Compute flood extent raster"""
        # get country-wide flood extent rasters
        country = self.data.forecast_admin.country
        if os.path.exists(self.flood_extent_raster):
            os.remove(self.flood_extent_raster)
        flood_rasters = {}
        for rp in [10, 20, 50, 75, 100, 200, 500]:
            flood_raster_filepath = (
                self.input_data_path + f"/flood_map_{country.upper()}_RP{rp}.tif"
            )
            if not os.path.exists(flood_raster_filepath):
                self.load.get_from_blob(
                    flood_raster_filepath,
                    f"{self.settings.get_setting('blob_storage_path')}"
                    f"/flood-maps/{country.upper()}/flood_map_{country.upper()}_RP{rp}.tif",
                )
            flood_rasters[rp] = flood_raster_filepath

        # create empty raster
        empty_raster = self.flood_extent_raster.replace(".tif", "_empty.tif")
        with rasterio.open(list(flood_rasters.values())[0]) as src:
            flood_raster_data = src.read()
            flood_raster_data = np.empty(flood_raster_data.shape)
            flood_raster_meta = src.meta.copy()
            flood_raster_meta["compress"] = "lzw"
            with rasterio.open(empty_raster, "w", **flood_raster_meta) as dest:
                dest.write(flood_raster_data)

        # PHL specific: get additional local flood extent
        if country.upper() == "PHL":
            # Get Delft-FEWS flood extent raster file
            local_flood_extent_dir = (
                self.input_data_path + f"/delft-fews"
            )
            if not os.path.exists(local_flood_extent_dir):
                self.load.get_all_from_blob(
                    local_flood_extent_dir,
                    f"{self.settings.get_setting('blob_storage_path')}"
                    f"/flood-maps/{country.upper()}/delft-fews"
                )

        adm_lvl = self.data.forecast_admin.adm_levels[-1]

        # get adm boundaries
        gdf_adm = self.load.get_adm_boundaries(
            self.data.forecast_admin.country, adm_lvl
        )
        gdf_adm.index = gdf_adm[f"adm{adm_lvl}_pcode"]

        for lead_time in self.data.forecast_admin.get_lead_times():

            raster_lead_time = self.flood_extent_raster.replace(
                ".tif", f"_{lead_time}.tif"
            )

            # calculate flood extent for each triggered admin division
            flood_rasters_admin_div = []
            for forecast_data_unit in self.data.forecast_admin.get_data_units(
                lead_time=lead_time, adm_level=adm_lvl
            ):
                if forecast_data_unit.triggered:
                    adm_bounds = gdf_adm.loc[forecast_data_unit.pcode, "geometry"]
                    rp = forecast_data_unit.return_period

                    # if return period is not available, use the smallest available
                    if rp not in flood_rasters.keys():
                        rp = min(flood_rasters.keys())

                    # PHL specific: merge global flood extent raster and local Delft-FEWS flood extent raster
                    # other countries: directly clip global flood extent raster
                    if country.upper() == "PHL":
                        # Extract Delft-FEWS flood extent based on lead time
                        flood_rasters_delft_fews = self.__filter_delft_fews_lead_time(
                            local_flood_extent_dir,
                            lead_time
                        )
                        flood_raster = self.__merge_all_flood_extents(
                            flood_rasters[rp],
                            flood_rasters_delft_fews,
                        )
                    else:
                        flood_raster = flood_rasters[rp]

                    # clip flood extent raster with admin division boundaries
                    flood_raster_data, flood_raster_meta = clip_raster(
                        flood_raster, [adm_bounds]
                    )
                    # save the clipped raster
                    flood_raster_admin_div = (
                        f"data/output/flood_extent_{forecast_data_unit.pcode}.tif"
                    )
                    with rasterio.open(
                        flood_raster_admin_div, "w", **flood_raster_meta
                    ) as dest:
                        dest.write(flood_raster_data)
                    flood_rasters_admin_div.append(flood_raster_admin_div)

            # merge flood extents of each triggered admin division
            if len(flood_rasters_admin_div) > 0:
                flood_rasters_admin_div.append(empty_raster)
                flood_raster_data, flood_raster_meta = merge_rasters(
                    flood_rasters_admin_div
                )
                flood_raster_meta["compress"] = "lzw"
                with rasterio.open(raster_lead_time, "w", **flood_raster_meta) as dest:
                    dest.write(flood_raster_data)
                for file in flood_rasters_admin_div:
                    if file != empty_raster:
                        try:
                            os.remove(file)
                        except FileNotFoundError:
                            pass
            else:
                shutil.copy(empty_raster, raster_lead_time)

    def __compute_affected_pop_raster(self):
        """Compute affected population raster given a flood extent"""
        country = self.data.forecast_admin.country
        # get population density raster
        self.load.get_population_density(country, self.pop_raster)

        flood_shapes = []
        for lead_time in self.data.forecast_admin.get_lead_times():
            flood_raster_lead_time = self.flood_extent_raster.replace(
                ".tif", f"_{lead_time}.tif"
            )
            aff_pop_raster_lead_time = self.aff_pop_raster.replace(
                ".tif", f"_{lead_time}.tif"
            )
            if os.path.exists(aff_pop_raster_lead_time):
                os.remove(aff_pop_raster_lead_time)
            with rasterio.open(flood_raster_lead_time) as dataset:
                # Read the dataset's valid data mask as a ndarray.
                image = dataset.read(1).astype(np.float32)
                image[image >= self.settings.get_setting("minimum_flood_depth")] = 1
                rasterio_shapes = shapes(
                    image, transform=dataset.transform
                )  # convert flood extent raster to vector (list of shapes)
                for geom, val in rasterio_shapes:
                    if val >= self.settings.get_setting("minimum_flood_depth"):
                        flood_shapes.append(shape(geom))
            # clip population density raster with flood shapes and save the result
            if len(flood_shapes) > 0:
                affected_pop_raster, affected_pop_meta = clip_raster(
                    self.pop_raster, flood_shapes
                )
                with rasterio.open(
                    aff_pop_raster_lead_time, "w", **affected_pop_meta
                ) as dest:
                    dest.write(affected_pop_raster)

    def __compute_affected_pop(self):
        """Compute affected population given a flood extent"""

        # calculate affected population raster
        self.__compute_affected_pop_raster()

        # calculate affected population per admin division
        for adm_lvl in self.data.forecast_admin.adm_levels:
            # get adm boundaries
            gdf_adm = self.load.get_adm_boundaries(
                self.data.forecast_admin.country, adm_lvl
            )
            gdf_aff_pop, gdf_pop = pd.DataFrame(), pd.DataFrame()

            for lead_time in self.data.forecast_admin.get_lead_times():
                aff_pop_raster_lead_time = self.aff_pop_raster.replace(
                    ".tif", f"_{lead_time}.tif"
                )
                if os.path.exists(aff_pop_raster_lead_time):
                    # perform zonal statistics on affected population raster
                    with rasterio.open(aff_pop_raster_lead_time) as src:
                        raster_array = src.read(1)
                        raster_array[raster_array < 0.0] = 0.0
                        transform = src.transform

                    stats = zonal_stats(
                        gdf_adm,
                        raster_array,
                        affine=transform,
                        stats=["sum"],
                        all_touched=True,
                        nodata=0.0,
                    )
                    gdf_aff_pop = pd.concat([gdf_adm, pd.DataFrame(stats)], axis=1)
                    gdf_aff_pop.index = gdf_aff_pop[f"adm{adm_lvl}_pcode"]

                    # perform zonal statistics on population density raster (to compute % aff pop)
                    with rasterio.open(self.pop_raster) as src:
                        raster_array = src.read(1)
                        raster_array[raster_array < 0.0] = 0.0
                        transform = src.transform
                    stats = zonal_stats(
                        gdf_adm,
                        raster_array,
                        affine=transform,
                        stats=["sum"],
                        all_touched=True,
                        nodata=0.0,
                    )
                    gdf_pop = pd.concat([gdf_adm, pd.DataFrame(stats)], axis=1)
                    gdf_pop.index = gdf_pop[f"adm{adm_lvl}_pcode"]

                # add affected population to forecast data units
                for forecast_data_unit in self.data.forecast_admin.get_data_units(
                    adm_level=adm_lvl, lead_time=lead_time
                ):
                    if forecast_data_unit.triggered:
                        try:
                            pop_affected = int(
                                gdf_aff_pop.loc[forecast_data_unit.pcode, "sum"]
                            )
                        except (ValueError, TypeError, KeyError):
                            pop_affected = 0
                        forecast_data_unit.pop_affected = pop_affected
                        try:
                            forecast_data_unit.pop_affected_perc = (
                                float(
                                    pop_affected
                                    / gdf_pop.loc[forecast_data_unit.pcode, "sum"]
                                )
                                * 100.0
                            )
                        except (ValueError, TypeError, KeyError):
                            forecast_data_unit.pop_affected_perc = 0.0

    def compute_forecast_station(self):
        """
        Forecast floods per GloFAS station based on river discharge data
        1. determine if trigger level is reached, with which probability, and alert class
        """
        self.__compute_triggers_station()

    def __compute_triggers_station(self):
        """Determine if trigger level is reached, its probability, and the alert class"""

        os.makedirs(self.input_data_path, exist_ok=True)
        os.makedirs(self.output_data_path, exist_ok=True)
        country = self.data.forecast_station.country

        trigger_on_lead_time = self.settings.get_country_setting(
            country, "trigger-on-lead-time"
        )
        for discharge_station in self.data.discharge_station.data_units:
            station_code = discharge_station.station_code
            lead_time = discharge_station.lead_time
            threshold_station = self.data.threshold_station.get_data_unit(station_code)

            likelihood_per_return_period, forecasts = {}, []
            for threshold in threshold_station.thresholds:
                threshold_checks = map(
                    lambda x: 1 if x > threshold["threshold_value"] else 0,
                    discharge_station.discharge_ensemble,
                )
                likelihood = sum(threshold_checks) / len(
                    discharge_station.discharge_ensemble
                )
                return_period = threshold["return_period"]
                likelihood_per_return_period[return_period] = likelihood
                forecasts.append(
                    FloodForecast(return_period=return_period, likelihood=likelihood)
                )

            trigger_on_return_period = self.settings.get_country_setting(
                country, "trigger-on-return-period"
            )
            trigger_on_minimum_probability = self.settings.get_country_setting(
                country, "trigger-on-minimum-probability"
            )
            classify_alert_on = self.settings.get_country_setting(
                country, "classify-alert-on"
            )
            alert_on_return_period = self.settings.get_country_setting(
                country, "alert-on-return-period"
            )
            alert_on_minimum_probability = self.settings.get_country_setting(
                country, "alert-on-minimum-probability"
            )
            if trigger_on_return_period not in likelihood_per_return_period.keys():
                raise ValueError(
                    f"No threshold found for return period {trigger_on_return_period}, "
                    f"which defines trigger in config file (trigger-on-return-period). "
                    f"Thresholds found: {likelihood_per_return_period.keys()}"
                )

            # determine if triggered and the corresponding return period
            triggered = (
                True
                if likelihood_per_return_period[trigger_on_return_period]
                >= trigger_on_minimum_probability
                and lead_time <= trigger_on_lead_time
                else False
            )
            return_period = next(
                (
                    key
                    for key, value in reversed(likelihood_per_return_period.items())
                    if value >= trigger_on_minimum_probability
                ),
                0.0,
            )

            # determine the alert class
            alert_class = classify_alert(
                triggered,
                likelihood_per_return_period,
                classify_alert_on,
                alert_on_return_period,
                alert_on_minimum_probability,
            )

            forecast_data_unit = ForecastStationDataUnit(
                station_code=discharge_station.station_code,
                station_name=discharge_station.station_name,
                lat=discharge_station.lat,
                lon=discharge_station.lon,
                pcodes=discharge_station.pcodes,
                lead_time=lead_time,
                forecasts=forecasts,
                triggered=triggered,
                return_period=return_period,
                alert_class=alert_class,
            )
            self.data.forecast_station.upsert_data_unit(forecast_data_unit)

    def __filter_delft_fews_lead_time(
            self, 
            local_flood_extent_dir: str,
            lead_time: int
        ) -> list[str]:
        """
        - Filter Delft-FEWS netCDF file with given lead time
        - Reproject from projected EPSG:32651 (WGS 84 / UTM zone 51N) specified 
        by Delft-FEWS model to the same generic coordinate EPSG:3857 with other 
        data.

        local_flood_extent_dir: directory path to Delft-FEWS netCDF files
        lead_time: lead time to filter the netCDF files, in hours
        """

        paths_to_nc_files = [
            os.path.join(local_flood_extent_dir, f)
            for f in os.listdir(local_flood_extent_dir)
            if f.endswith(".nc")
        ]

        paths_to_tif = []
        for nc_filepath in paths_to_nc_files:
            ds = xr.open_dataset(nc_filepath)
            ds = ds.rio.write_crs("EPSG:32651")  # Deltares model CRS

            # select lead time
            ds_lead_time = ds["H"].isel(time=lead_time)

            # reproject to generic CRS
            ds_lead_time_proj = ds_lead_time.rio.reproject("EPSG:4326")
            
            output_filepath = nc_filepath.replace(".nc", f"_{lead_time}.tif")
            ds_lead_time_proj.rio.to_raster(output_filepath)
            paths_to_tif.append(output_filepath)

        return paths_to_tif

    def __merge_all_flood_extents(
            self, 
            global_flood_extent: str,
            local_flood_extents: list[str],
            resolution="global",  # "global" or "local"
        ) -> str:
        """
        Merge global flood extent raster and local Delft-FEWS flood extent rasters
        into a single one:
        - If resolution is "global", use the global flood extent raster's resolution as final resolution
        - If resolution is "local", use local flood extent raster instead
        Then save the merged raster.

        global_flood_extent: file path to global flood extent raster
        local_flood_extents: list of file paths to local flood extent rasters
        """

        if resolution not in ["global", "local"]:
            raise ValueError("resolution must be 'global' or 'local'")

        all_flood_extents = [global_flood_extent] + local_flood_extents

        # Determine target CRS and resolution
        with rasterio.open(global_flood_extent) as src_global:
            global_crs = src_global.crs
            global_res = src_global.res
            global_nodata = src_global.nodata

        if resolution == "global":  # use resolution of global raster
            target_crs = global_crs
            target_res_x, target_res_y = global_res
        elif resolution == "local":  # use resolution of Deltares raster
            with rasterio.open(local_flood_extents[0]) as src_local:
                target_crs = src_local.crs
                target_res_x, target_res_y = src_local.res

        # Compute UNION extent of flood raster
        all_bounds = []

        for file in all_flood_extents:
            with rasterio.open(file) as src:
                bounds = transform_bounds(
                    src.crs, target_crs, *src.bounds
                )
                all_bounds.append(bounds)

        xmin = min(b[0] for b in all_bounds)
        ymin = min(b[1] for b in all_bounds)
        xmax = max(b[2] for b in all_bounds)
        ymax = max(b[3] for b in all_bounds)

        # Build output raster grid
        width = math.ceil((xmax - xmin) / target_res_x)
        height = math.ceil((ymax - ymin) / abs(target_res_y))

        transform = from_origin(xmin, ymax, target_res_x, abs(target_res_y))

        result = np.full((height, width), np.nan, dtype=np.float32)

        # Fill flood depth values over no-data
        for file in all_flood_extents:
            with rasterio.open(file) as src:
                temp = np.full((height, width), np.nan, dtype=np.float32)

                reproject(
                    source=rasterio.band(src, 1),
                    destination=temp,
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    dst_width=width,
                    dst_height=height,
                    resampling=Resampling.nearest
                )

                if src.nodata is not None:
                    temp[temp == src.nodata] = np.nan

                result = np.where(np.isnan(result), temp, result)

        result[np.isnan(result)] = global_nodata

        profile = {
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": 1,
            "dtype": "float32",
            "crs": target_crs,
            "transform": transform,
            "nodata": global_nodata
        }

        output_path = global_flood_extent.replace(".tif", f"_delft_fews.tif")
        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(result, 1)

        return output_path