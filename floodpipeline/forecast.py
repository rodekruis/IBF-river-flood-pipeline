from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    AdminDataSet,
    ForecastDataUnit,
    FloodForecast,
    ForecastStationDataUnit,
    StationDataSet,
)
from floodpipeline.load import Load
from datetime import datetime
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


def classify_alert(
    likelihood_per_return_period: dict,
    alert_on_return_period: dict,
    alert_on_minimum_probability: float,
) -> str:
    """
    Classify alert based on flood forecast return period specified in settings.py
    """
    alert_on_return_period = {
        k: v
        for k, v in sorted(alert_on_return_period.items(), key=lambda item: item[1])
    }  # order by return period, from smallest to largest
    alert_class = "no"
    for class_, return_period in alert_on_return_period.items():
        if likelihood_per_return_period[return_period] >= alert_on_minimum_probability:
            alert_class = class_
    return alert_class


class Forecast:
    """
    Forecast flood events based on river discharge data
    1. determine if trigger level is reached, with which probability, and alert class'
    2. compute exposure (people affected)
    3. compute flood extent
    """

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        self.set_settings(settings)
        self.set_secrets(secrets)
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
        self, discharge_dataset: AdminDataSet, threshold_dataset: AdminDataSet
    ) -> AdminDataSet:
        os.makedirs(self.input_data_path, exist_ok=True)
        os.makedirs(self.output_data_path, exist_ok=True)
        country = discharge_dataset.country

        forecast_dataset = self.__compute_triggers(discharge_dataset, threshold_dataset)
        flooded = self.__compute_flood_extent(country, forecast_dataset)
        if flooded:
            forecast_dataset = self.__compute_affected_pop(country, forecast_dataset)
        return forecast_dataset

    def forecast_station(
        self, discharge_dataset: StationDataSet, threshold_dataset: StationDataSet
    ) -> StationDataSet:
        os.makedirs(self.input_data_path, exist_ok=True)
        os.makedirs(self.output_data_path, exist_ok=True)
        country = discharge_dataset.country
        forecast_dataset = StationDataSet(country=country, timestamp=datetime.today())

        trigger_on_lead_time = self.settings.get_country_setting(
            country, "trigger-on-lead-time"
        )
        for discharge_station in discharge_dataset.data_units:
            station_code = discharge_station.station_code
            lead_time = discharge_station.lead_time
            threshold_station = threshold_dataset.get_data_unit(station_code)

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
                and lead_time == trigger_on_lead_time
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
            alert_class = classify_alert(
                likelihood_per_return_period,
                alert_on_return_period,
                alert_on_minimum_probability,
            )
            # END: TO BE DEPRECATED
            forecast_data_unit = ForecastStationDataUnit(
                station_code=discharge_station.station_code,
                station_name=discharge_station.station_name,
                lat=discharge_station.lat,
                lon=discharge_station.lon,
                pcodes=discharge_station.pcodes,
                lead_time=lead_time,
                forecasts=forecasts,
                # START: TO BE DEPRECATED
                triggered=triggered,
                return_period=return_period,
                alert_class=alert_class,
                # END: TO BE DEPRECATED
            )
            forecast_dataset.upsert_data_unit(forecast_data_unit)
        return forecast_dataset

    def __compute_triggers(
        self,
        discharges: AdminDataSet,
        thresholds: AdminDataSet,
    ) -> AdminDataSet:
        """Determine if trigger level is reached, its probability, and the alert class"""

        country = discharges.country
        adm_levels = discharges.adm_levels
        forecast_dataset = AdminDataSet(
            country=country, adm_levels=adm_levels, timestamp=datetime.today()
        )
        trigger_on_lead_time = self.settings.get_country_setting(
            country, "trigger-on-lead-time"
        )
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

        for pcode in discharges.get_pcodes():
            threshold_data_unit = thresholds.get_data_unit(pcode)
            for lead_time in discharges.get_lead_times():
                discharge_data_unit = discharges.get_data_unit(pcode, lead_time)
                adm_level = discharge_data_unit.adm_level

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

                # START: TO BE DEPRECATED
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
                    and lead_time == trigger_on_lead_time
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
                alert_class = classify_alert(
                    likelihood_per_return_period,
                    alert_on_return_period,
                    alert_on_minimum_probability,
                )
                # END: TO BE DEPRECATED
                forecast_data_unit = ForecastDataUnit(
                    adm_level=adm_level,
                    pcode=pcode,
                    lead_time=lead_time,
                    forecasts=forecasts,
                    # START: TO BE DEPRECATED
                    triggered=triggered,
                    return_period=return_period,
                    alert_class=alert_class,
                    # END: TO BE DEPRECATED
                )
                forecast_dataset.upsert_data_unit(forecast_data_unit)
        return forecast_dataset

    def __compute_flood_extent(
        self, country: str, forecast_dataset: AdminDataSet
    ) -> bool:
        """Compute flood extent raster, return True if flooded, False otherwise"""
        # get country-wide flood extent rasters
        if country != forecast_dataset.country:
            raise ValueError(
                f"Country {country} does not match flood forecast dataset country {forecast_dataset.country}"
            )
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
        for adm_lvl in forecast_dataset.adm_levels:

            # get adm boundaries
            gdf_adm = self.load.get_adm_boundaries(forecast_dataset.country, adm_lvl)
            gdf_adm.index = gdf_adm[f"adm{adm_lvl}_pcode"]

            # calculate flood extent for each triggered admin division
            for forecast_data_unit in forecast_dataset.data_units:
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
            flooded = True
        else:
            # create empty raster
            with rasterio.open(list(flood_rasters.values())[0]) as src:
                flood_raster_data = src.read()
                flood_raster_data = np.where(
                    flood_raster_data > -9999999999999999.0, 0.0, 0.0
                )
                flood_raster_meta = src.meta.copy()
            if os.path.exists(self.flood_extent_filepath):
                os.remove(self.flood_extent_filepath)
            with rasterio.open(
                self.flood_extent_filepath, "w", **flood_raster_meta
            ) as dest:
                dest.write(flood_raster_data)
            flooded = False
        return flooded

    def __compute_affected_pop_raster(self, country: str):
        """Compute affected population raster given a flood extent"""
        # get population density raster
        self.load.get_population_density(country, self.pop_filepath)
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

    def __compute_affected_pop(
        self, country: str, forecast_dataset: AdminDataSet
    ) -> AdminDataSet:
        """Compute affected population given a flood extent"""

        # calculate affected population raster
        self.__compute_affected_pop_raster(country=country)

        if os.path.exists(self.aff_pop_filepath):
            # calculate affected population per admin division
            for adm_lvl in forecast_dataset.adm_levels:

                # get adm boundaries
                gdf_adm = self.load.get_adm_boundaries(country, adm_lvl)
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
                for forecast_data_unit in forecast_dataset.data_units:
                    if (
                        forecast_data_unit.adm_level == adm_lvl
                        and forecast_data_unit.triggered
                    ):
                        try:
                            pop_affected = int(
                                gdf_aff_pop.loc[forecast_data_unit.pcode, "sum"]
                            )
                        except (ValueError, TypeError):
                            pop_affected = 0
                        forecast_data_unit.pop_affected = pop_affected
                        forecast_data_unit.pop_affected_perc = (
                            float(
                                pop_affected
                                / gdf_pop.loc[forecast_data_unit.pcode, "sum"]
                            )
                            * 100.0
                        )

        return forecast_dataset

    # START: TO BE DEPRECATED
    def __compute_triggers_stations(
        self,
        discharges: AdminDataSet,
        thresholds: AdminDataSet,
    ):
        """Determine if trigger level is reached, its probability, and alert class'"""
        pass

    # END: TO BE DEPRECATED
