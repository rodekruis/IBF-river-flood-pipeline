from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, FloodForecastDataUnit, FloodForecast
from floodpipeline.load import Load
import time
from typing import List
from shapely import Polygon
import pandas as pd
from rasterstats import zonal_stats
import os
import numpy as np
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.features import shapes
import itertools


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


def clip_raster(raster_filepath: str, shapes: List[Polygon]) -> tuple:
    """Clip raster with a list of polygons, return the clipped raster and its metadata"""
    with rasterio.open(raster_filepath) as src:
        outImage, out_transform = mask(src, shapes, crop=True)
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


class Forecast:
    """
    Forecast flood events based on river discharge data
    1. determine if trigger level is reached, with which probability, and the 'EAP Alert Class'
    2. compute exposure (people affected)
    3. compute flood extent
    """

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        self.set_settings(settings)
        self.set_secrets(secrets)
        self.flood_data = BaseDataSet()
        self.load = Load(settings=self.settings, secrets=self.secrets)
        self.input_data_path: str = "data/input"
        self.output_data_path: str = "data/output"
        self.flood_extent_filepath: str = self.output_data_path + "/flood_extent.tif"
        self.pop_filepath: str = self.input_data_path + "/population_density.tif"
        self.aff_pop_filepath: str = self.output_data_path + "/affected_population.tif"

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

    def forecast(
        self, river_discharges: BaseDataSet, trigger_thresholds: BaseDataSet
    ) -> BaseDataSet:
        os.makedirs(self.input_data_path, exist_ok=True)
        os.makedirs(self.output_data_path, exist_ok=True)
        self.flood_data = BaseDataSet(
            country=river_discharges.country, adm_levels=river_discharges.adm_levels
        )
        self.__compute_triggers(river_discharges, trigger_thresholds)
        self.__compute_flood_extent()
        self.__compute_affected_pop()
        return self.flood_data

    def __compute_triggers(
        self,
        river_discharges: BaseDataSet,
        trigger_thresholds: BaseDataSet,
    ):
        """Determine if trigger level is reached, its probability, and the 'EAP Alert Class'"""
        country = self.flood_data.country
        for lead_time, pcode in itertools.product(
            river_discharges.get_lead_times(), river_discharges.get_pcodes()
        ):
            river_discharge_data_unit = river_discharges.get_data_unit(pcode, lead_time)
            trigger_threshold_du_data_unit = trigger_thresholds.get_data_unit(pcode)
            adm_level = river_discharge_data_unit.adm_level

            likelihood_per_return_period, flood_forecasts = {}, []
            for trigger_threshold in trigger_threshold_du_data_unit.trigger_thresholds:
                threshold_checks = map(
                    lambda x: 1 if x > trigger_threshold["threshold"] else 0,
                    river_discharge_data_unit.river_discharge_ensemble,
                )
                likelihood = sum(threshold_checks) / len(
                    river_discharge_data_unit.river_discharge_ensemble
                )
                return_period = trigger_threshold["return_period"]
                likelihood_per_return_period[return_period] = likelihood
                flood_forecasts.append(
                    FloodForecast(return_period=return_period, likelihood=likelihood)
                )

            # START: TO BE DEPRECATED
            trigger_on_return_period = self.settings.get_country_setting(
                country, "trigger-on-return-period"
            )
            trigger_on_minimum_probability = self.settings.get_country_setting(
                country, "trigger-on-minimum-probability"
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
            triggered = (
                True
                if likelihood_per_return_period[trigger_on_return_period]
                >= trigger_on_minimum_probability
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
            if any(
                rp not in likelihood_per_return_period.keys()
                for rp in alert_on_return_period.values()
            ):
                missing_rps = [
                    rp
                    for rp in alert_on_return_period.values()
                    if rp not in likelihood_per_return_period.keys()
                ]
                raise ValueError(
                    f"No threshold found for return periods {missing_rps}, "
                    f"which define alert classes in config file (alert-on-return-period). "
                    f"Thresholds found: {likelihood_per_return_period.keys()}"
                )
            alert_class = self.__classify_eap_alert(
                likelihood_per_return_period,
                alert_on_return_period,
                alert_on_minimum_probability,
            )
            # END: TO BE DEPRECATED
            # threshold_reached = True if 'max' in alert_class else False
            forecast_data_unit = FloodForecastDataUnit(
                adm_level=adm_level,
                pcode=pcode,
                lead_time=lead_time,
                flood_forecasts=flood_forecasts,
                # START: TO BE DEPRECATED
                triggered=triggered,
                return_period=return_period,
                alert_class=alert_class,
                # END: TO BE DEPRECATED
            )
            self.flood_data.upsert_data_unit(forecast_data_unit)

    def __compute_flood_extent(self):
        """Compute flood extent raster"""
        # get country-wide flood extent rasters
        country = self.flood_data.country
        flood_rasters = {}
        for rp in [10, 20, 50, 75, 100, 200, 500]:
            flood_raster_filepath = (
                self.input_data_path + f"/flood_map_{country.upper()}_RP{rp}.tif"
            )
            if not os.path.exists(flood_raster_filepath):
                self.load.get_from_blob(
                    flood_raster_filepath,
                    f"flood/pipeline-input/flood-maps/{country.upper()}/flood_map_{country.upper()}_RP{rp}.tif",
                )
            flood_rasters[rp] = flood_raster_filepath
        flood_rasters_admin_div = []
        for adm_lvl in self.flood_data.adm_levels:
            # get adm boundaries
            gdf_adm = self.load.get_adm_boundaries(self.flood_data.country, adm_lvl)
            gdf_adm.index = gdf_adm[f"adm{adm_lvl}_pcode"]

            # calculate flood extent for each triggered admin division
            for forecast_data_unit in self.flood_data.data_units:
                if (
                    forecast_data_unit.adm_level == adm_lvl
                    and forecast_data_unit.triggered
                ):
                    adm_bounds = gdf_adm.loc[forecast_data_unit.pcode, "geometry"]
                    rp = forecast_data_unit.return_period

                    # if return period is not available, use the smallest available
                    if rp not in flood_rasters.keys():
                        rp = min(flood_rasters.keys())

                    # clip flood extent raster with admin division boundaries
                    flood_raster_data, flood_raster_meta = clip_raster(
                        flood_rasters[rp], [adm_bounds]
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
            flood_raster_data, flood_raster_meta = merge_rasters(
                flood_rasters_admin_div
            )
            if os.path.exists(self.flood_extent_filepath):
                os.remove(self.flood_extent_filepath)
            with rasterio.open(
                self.flood_extent_filepath, "w", **flood_raster_meta
            ) as dest:
                dest.write(flood_raster_data)
            # delete intermediate files
            for file in flood_rasters_admin_div:
                os.remove(file)

    def __compute_affected_pop_raster(self):
        """Compute affected population raster given a flood extent"""
        if os.path.exists(self.flood_extent_filepath):
            # get population density raster
            self.load.get_population_density(self.flood_data.country, self.pop_filepath)
            # convert flood extent raster to vector (list of shapes)
            flood_shapes = []
            with rasterio.open(self.flood_extent_filepath) as dataset:
                # Read the dataset's valid data mask as a ndarray.
                image = dataset.read(1).astype(np.float32)
                image[image >= 0] = 1
                mask = dataset.dataset_mask()
                rasterio_shapes = shapes(image, mask=mask, transform=dataset.transform)
                for geom, val in rasterio_shapes:
                    if val >= self.settings.get_setting("minimum_flood_depth"):
                        flood_shapes.append(geom)
            # clip population density raster with flood shapes and save the result
            affected_pop_raster, affected_pop_meta = clip_raster(
                self.pop_filepath, flood_shapes
            )
            if os.path.exists(self.aff_pop_filepath):
                os.remove(self.aff_pop_filepath)
            with rasterio.open(self.aff_pop_filepath, "w", **affected_pop_meta) as dest:
                dest.write(affected_pop_raster)

    def __compute_affected_pop(self):
        """Compute affected population given a flood extent"""

        # calculate affected population raster
        self.__compute_affected_pop_raster()

        if os.path.exists(self.aff_pop_filepath):
            # calculate affected population per admin division
            for adm_lvl in self.flood_data.adm_levels:

                # get adm boundaries
                gdf_adm = self.load.get_adm_boundaries(self.flood_data.country, adm_lvl)
                gdf_adm = gdf_adm.to_crs("EPSG:4326")

                # perform zonal statistics on affected population raster
                with rasterio.open(self.aff_pop_filepath) as src:
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
                with rasterio.open(self.pop_filepath) as src:
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
                for forecast_data_unit in self.flood_data.data_units:
                    if (
                        forecast_data_unit.adm_level == adm_lvl
                        and forecast_data_unit.triggered
                    ):
                        forecast_data_unit.pop_affected = int(
                            gdf_aff_pop.loc[forecast_data_unit.pcode, "sum"]
                        )
                        forecast_data_unit.pop_affected_perc = (
                            float(
                                forecast_data_unit.pop_affected
                                / gdf_pop.loc[forecast_data_unit.pcode, "sum"]
                            )
                            * 100.0
                        )

    # START: TO BE DEPRECATED
    def __compute_triggers_stations(
        self,
        river_discharges: BaseDataSet,
        trigger_thresholds: BaseDataSet,
    ):
        """Determine if trigger level is reached, its probability, and the 'EAP Alert Class'"""
        pass

    # END: TO BE DEPRECATED

    def __classify_eap_alert(
        self,
        likelihood_per_return_period: dict,
        alert_on_return_period: dict,
        alert_on_minimum_probability: float,
    ) -> str:
        """
        Classify EAP Alert based on flood forecast return period specified in settings.py
        """
        alert_on_return_period = {
            k: v
            for k, v in sorted(alert_on_return_period.items(), key=lambda item: item[1])
        }  # order by return period, from smallest to largest
        alert_class = "no"
        for class_, return_period in alert_on_return_period.items():
            if (
                likelihood_per_return_period[return_period]
                >= alert_on_minimum_probability
            ):
                alert_class = class_
        return alert_class

    def __calculate_probability(self, river_discharges):
        """Calculate probability of river discharge"""
        pass
