from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, FloodForecastDataUnit
from floodpipeline.load import Load
import requests
import urllib.request
from shapely import Polygon
import re
import os
import geopandas as gpd
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask


def merge_rasters(raster_filepaths: list):
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


def clip_raster(raster_filepath: str, shapes: Polygon):
    with rasterio.open(raster_filepath) as src:
        outImage, out_transform = mask(src, [shapes], crop=True)
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
        if settings is not None:
            self.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
        self.flood_data = BaseDataSet()

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
        self,
        river_discharges: BaseDataSet,
        trigger_thresholds: BaseDataSet,
    ) -> BaseDataSet:
        self.__compute_triggers(river_discharges, trigger_thresholds)
        self.__compute_flood_extent()
        self.__compute_exposure()
        return self.flood_data

    def __compute_triggers(
        self,
        river_discharges: BaseDataSet,
        trigger_thresholds: BaseDataSet,
    ):
        """Determine if trigger level is reached, its probability, and the 'EAP Alert Class'"""
        pass

    def __compute_flood_extent(self):
        """Compute flood extent"""
        pass

    def __compute_exposure(self):
        """Compute exposure (people affected)"""
        pass

    # START: TO BE DEPRECATED
    def __compute_triggers_stations(
        self,
        river_discharges: BaseDataSet,
        trigger_thresholds: BaseDataSet,
    ):
        """Determine if trigger level is reached, its probability, and the 'EAP Alert Class'"""
        pass

    # END: TO BE DEPRECATED
