from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, FloodForecastDataUnit
from floodpipeline.load import Load
import requests
import urllib.request
import re
import os
from shapely.geometry import box
import geopandas as gpd
import rasterio


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

    def get_flood_map(self, country: str, rp: int):
        """Get flood map for a given country and return period"""
        if rp not in [10, 20, 50, 75, 100, 200, 500]:
            raise ValueError("Return Period must be in 10, 20, 50, 75, 100, 200 or 500")
        flood_map_html = requests.get(
            f"{self.settings.get_setting('global_flood_maps_url')}/RP{rp}/"
        ).text
        flood_map_files = re.findall(r"^ID(.*?).tif$", flood_map_html)
        gdf_flood_map = gpd.GeoDataFrame(columns=["filename", "geometry"])
        for file in flood_map_files:
            if "N" in file:
                max_lat = int(re.search(r"N\d{2}", file)[0][1:])
            else:
                max_lat = -int(re.search(r"S\d{2}", file)[0][1:])
            min_lat = max_lat - 10
            if "E" in file:
                min_lon = int(re.search(r"E\d{2}", file)[0][1:])
            else:
                min_lon = -int(re.search(r"W\d{2}", file)[0][1:])
            max_lon = min_lon + 10
            geom = box(min_lon, min_lat, max_lon, max_lat)
            gdf_flood_map.loc[len(gdf_flood_map)] = {"filename": file, "geometry": geom}

        country_gdf = Load(secrets=self.secrets).get_adm_boundaries(
            country=country, adm_level=1
        )
        gdf_flood_map = gdf_flood_map[
            gdf_flood_map["geometry"].touches(country_gdf["geometry"])
        ]
        flood_map_files = gdf_flood_map["filename"].tolist()
        for flood_map_file in flood_map_files:
            url = f"{self.settings.get_setting('global_flood_maps_url')}/RP{rp}/{flood_map_file}"
            urllib.request.urlretrieve(url, flood_map_file)
        mosaic, out_meta = rasterio.merge(flood_map_files)
        out_meta.update(
            {
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": out_meta,
            }
        )
        with rasterio.open(f"data/input/flood_map_RP{rp}.tif", "w", **out_meta) as dest:
            dest.write(mosaic)
        for flood_map_file in flood_map_files:
            os.remove(flood_map_file)

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
