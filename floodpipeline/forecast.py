from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, FloodForecastDataUnit
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
        country_settings = next(item for item in self.settings.settings['countries'] if river_discharges.country in item)
        for lead_time, pcode in itertools.product(river_discharges.get_lead_times(), river_discharges.get_pcodes()):
            river_discharge_data_unit = river_discharges.get_data_unit(pcode, lead_time)
            trigger_thresholds_du = trigger_thresholds.get_data_unit(pcode, lead_time)

            if pcode in trigger_thresholds_du.pcode:
                trigger_threshold = country_settings['trigger-on-return-period']
                threshold = next(item for item in trigger_thresholds_du.trigger_thresholds 
                                 if item['return_period'] == trigger_threshold)
                threshold_checks = map(lambda x: 1 if x > threshold['threshold'] else 0, 
                                       river_discharge_data_unit.river_discharge_ensemble)
                ensemble_options = len(river_discharge_data_unit.river_discharge_ensemble)
                dis_avg = sum(river_discharge_data_unit.river_discharge_ensemble)/ensemble_options
                likelihood = sum(threshold_checks)/ensemble_options
                severity = threshold['return_period']
                # # START: TO BE DEPRECATED
                triggered = 1 if severity > trigger_threshold else 0
                alert_class = self.__classify_eap_alert(severity, country_settings['alert-on-return-period'])
                # threshold_reached = True if 'max' in alert_class else False
                # # END: TO BE DEPRECATED
                forecast_data_unit = FloodForecastDataUnit(
                    lead_time=lead_time,
                    likelihood=likelihood,
                    severity=severity,
                    # START: TO BE DEPRECATED
                    triggered=triggered,
                    alert_class=alert_class,
                    return_period=severity
                    # END: TO BE DEPRECATED
                )
                print('forecast_data_unit: ', forecast_data_unit)
                self.flood_data.upsert_data_unit(forecast_data_unit)
                print('flood_data: ', vars(self.flood_data))

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
        flood_raster_data, flood_raster_meta = merge_rasters(flood_rasters_admin_div)
        with rasterio.open(
            self.flood_extent_filepath, "w", **flood_raster_meta
        ) as dest:
            dest.write(flood_raster_data)
        # delete intermediate files
        for file in flood_rasters_admin_div:
            os.remove(file)

    def __compute_affected_pop_raster(self):
        """Compute affected population raster given a flood extent"""
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
        with rasterio.open(self.aff_pop_filepath, "w", **affected_pop_meta) as dest:
            dest.write(affected_pop_raster)

    def __compute_affected_pop(self):
        """Compute affected population given a flood extent"""

        # calculate affected population raster
        self.__compute_affected_pop_raster()

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

    def __classify_eap_alert(self, severity, alert_levels):
        ''' 
        Classify EAP Alert based on flood forecast return period specified in settings.py
        Applicable only for Uganda
        '''
        if not severity:
            return "no"
        elif severity >= alert_levels['max']:
            return "max"
        elif severity >= alert_levels['med']:
            return "med"
        elif severity >= alert_levels['min']:
            return "min"
        else:
            return "no"

    def __calculate_probability(self, river_discharges):
        '''Calculate probability of river discharge'''
        pass