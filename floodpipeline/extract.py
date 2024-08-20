from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    AdminDataSet,
    DischargeDataUnit,
    StationDataSet,
    DischargeStationDataUnit,
)
from floodpipeline.load import Load
import os
from datetime import datetime, timedelta
import time
import pandas as pd
import xarray as xr
from rasterstats import zonal_stats
import rasterio
import logging
import itertools
from typing import List
import urllib.request
import ftplib
import copy

supported_sources = ["GloFAS"]


def slice_netcdf_file(nc_file: xr.Dataset, country_bounds: list):
    """Slice the netcdf file to the bounding box"""
    min_lon = country_bounds[0]  # Minimum longitude
    max_lon = country_bounds[2]  # Maximum longitude
    min_lat = country_bounds[1]  # Minimum latitude
    max_lat = country_bounds[3]  # Maximum latitude
    var_data = nc_file.sel(lon=slice(min_lon, max_lon), lat=slice(max_lat, min_lat))
    return var_data


class Extract:
    """Extract river discharge data from external sources"""

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.source = None
        self.country = None
        self.secrets = None
        self.settings = None
        self.inputPathGrid = "./data/input"
        self.load = Load()
        if not os.path.exists(self.inputPathGrid):
            os.makedirs(self.inputPathGrid)
        if settings is not None:
            self.set_settings(settings)
            self.load.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
            self.load.set_secrets(secrets)

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        if self.source == "GloFAS":
            settings.check_settings(["glofas_ftp_server", "no_ensemble_members"])
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets based on the data source"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        if self.source == "GloFAS":
            secrets.check_secrets(["GLOFAS_USER", "GLOFAS_PASSWORD"])
        self.secrets = secrets

    def set_source(self, source_name, secrets: Secrets = None):
        """Set the data source"""
        if source_name is not None:
            if source_name not in supported_sources:
                raise ValueError(
                    f"Source {source_name} is not supported."
                    f"Supported sources are {', '.join(supported_sources)}"
                )
            else:
                self.source = source_name
                self.inputPathGrid = os.path.join(self.inputPathGrid, self.source)
        else:
            raise ValueError(
                f"Source not specified; provide one of {', '.join(supported_sources)}"
            )
        if secrets is not None:
            self.set_secrets(secrets)
        elif self.secrets is not None:
            self.set_secrets(self.secrets)
        else:
            raise ValueError(f"Set secrets before setting source")
        return self

    def get_data(self, country: str, source: str = None) -> AdminDataSet:
        """Get river discharge data from source and return AdminDataSet"""
        if source is None and self.source is None:
            raise RuntimeError("Source not specified, use set_source()")
        elif self.source is None and source is not None:
            self.source = source
        self.country = country
        discharge_dataset, discharge_station_dataset = None, None
        if self.source == "GloFAS":
            logging.info("get GloFAS data")
            self.prepare_glofas_data(countries=[self.country])
            discharge_dataset, discharge_station_dataset = self.extract_glofas_data(
                self.country
            )
        return discharge_dataset, discharge_station_dataset

    def extract_glofas_data(self, country: str):
        """Download GloFAS data for each ensemble member and map to AdminDataSet"""
        self.country = country
        discharge_dataset = AdminDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=self.settings.get_country_setting(self.country, "admin-levels"),
        )

        discharge_station_dataset = self.load.get_stations(country=self.country)

        # Download pre-processed NetCDF files for each ensemble member
        nofEns = self.settings.get_setting("no_ensemble_members")
        blob_path = self.settings.get_setting("blob_storage_path")
        date = datetime.today().strftime("%Y%m%d")
        filenames = [
            f"GloFAS_{date}_{self.country}_{ensemble}.nc"
            for ensemble in range(0, nofEns)
        ]
        netcdf_files_local_path = []
        for filename in filenames:
            local_path = os.path.join(self.inputPathGrid, filename)
            self.load.get_from_blob(
                local_path,
                f"{blob_path}/glofas-data/{date}/{self.country}/{filename}",
            )
            netcdf_files_local_path.append(local_path)

        # Extract data from NetCDF files
        logging.info("Extract admin-level river discharge from GloFAS data")
        discharges = {}
        for adm_level in discharge_dataset.adm_levels:
            country_gdf = self.load.get_adm_boundaries(
                country=self.country, adm_level=adm_level
            )
            for filename in netcdf_files_local_path:
                for lead_time in range(1, 8):
                    with rasterio.open(filename) as src:
                        raster_array = src.read(lead_time)
                        transform = src.transform
                    # Perform zonal statistics for admin divisions
                    stats = zonal_stats(
                        country_gdf,
                        raster_array,
                        affine=transform,
                        stats=["max", "median"],
                        all_touched=True,
                        nodata=0.0,
                    )
                    dis = pd.concat([country_gdf, pd.DataFrame(stats)], axis=1)
                    for ix, row in dis.iterrows():
                        key = f'{row[f"adm{adm_level}_pcode"]}_{lead_time}'
                        if key not in discharges.keys():
                            discharges[key] = []
                        discharges[key].append(row["max"])

            for lead_time, pcode in itertools.product(
                range(1, 8), list(country_gdf[f"adm{adm_level}_pcode"].unique())
            ):
                key = f"{pcode}_{lead_time}"
                discharge_dataset.upsert_data_unit(
                    DischargeDataUnit(
                        adm_level=adm_level,
                        pcode=pcode,
                        lead_time=lead_time,
                        discharge_ensemble=discharges[key],
                    )
                )

        logging.info("Extract station-level river discharge from GloFAS data")
        discharges_stations = {}
        for filename in netcdf_files_local_path:
            with rasterio.open(filename) as src:
                for station in discharge_station_dataset.data_units:
                    lead_time = int(station.lead_time)
                    # Extract data for stations
                    discharges = []
                    for shiftx in [-0.01, 0.01]:
                        for shifty in [-0.01, 0.01]:
                            coords = [
                                (
                                    float(station.lon) + shiftx,
                                    float(station.lat) + shifty,
                                )
                            ]
                            discharge = float(
                                [x[0] for x in src.sample(coords, indexes=lead_time)][0]
                            )
                            discharges.append(discharge)
                    key = f"{station.station_code}_{lead_time}"
                    if key not in discharges_stations.keys():
                        discharges_stations[key] = []
                    discharges_stations[key].append(max(discharges))

        for station in discharge_station_dataset.data_units:
            key = f"{station.station_code}_{station.lead_time}"
            discharge_station_dataset.upsert_data_unit(
                DischargeStationDataUnit(
                    station_code=station.station_code,
                    station_name=station.station_name,
                    lat=station.lat,
                    lon=station.lon,
                    pcodes=station.pcodes,
                    lead_time=station.lead_time,
                    discharge_ensemble=discharges_stations[key],
                )
            )

        return discharge_dataset, discharge_station_dataset

    def prepare_glofas_data(self, countries: List[str] = None):
        """
        Download one netcdf file per ensemble member;
        for each country, slice the data to the extent of country and save it to storage
        """
        if countries is None:
            countries = [c["name"] for c in self.settings.get_setting("countries")]
        logging.info(f"start preparing GloFAS data for countries {countries}")
        country_gdfs = {}
        for country in countries:
            country_gdfs[country] = self.load.get_adm_boundaries(
                country=country, adm_level=1
            )
        nofEns = self.settings.get_setting("no_ensemble_members")
        blob_path = self.settings.get_setting("blob_storage_path")
        date = datetime.today().strftime("%Y%m%d")
        for ensemble in range(0, nofEns):
            # Download netcdf file
            logging.info(f"start ensemble {ensemble}")
            filename_local = os.path.join(self.inputPathGrid, f"GloFAS_{ensemble}.nc")
            self.load.get_from_blob(
                filename_local,
                f"{blob_path}/glofas-data/{date}/dis_{'{:02d}'.format(ensemble)}_{date}00.nc",
            )
            nc_file = xr.open_dataset(filename_local)

            # Slice netcdf file to country boundaries
            for country in countries:
                country_gdf = country_gdfs[country]
                country_bounds = country_gdf.total_bounds
                nc_file_sliced = slice_netcdf_file(nc_file, country_bounds)
                filename_local_sliced = os.path.join(
                    self.inputPathGrid,
                    f"GloFAS_{date}_{country}_{ensemble}.nc",
                )
                nc_file_sliced.to_netcdf(filename_local_sliced)
                self.load.save_to_blob(
                    filename_local_sliced,
                    f"{blob_path}/glofas-data/{date}/{country}/GloFAS_{date}_{country}_{ensemble}.nc",
                )

            nc_file.close()
            os.remove(filename_local)
            logging.info(f"finished ensemble {ensemble}")
        logging.info("finished preparing GloFAS data")
