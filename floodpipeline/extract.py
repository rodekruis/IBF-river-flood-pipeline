from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    PipelineDataSets,
    DischargeDataUnit,
    DischargeStationDataUnit,
)
from floodpipeline.load import Load
import os
from datetime import datetime, timedelta
import pandas as pd
import xarray as xr
from rasterstats import zonal_stats
import rasterio
import logging
import itertools

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

    def __init__(
        self,
        country: str = None,
        settings: Settings = None,
        secrets: Secrets = None,
        data: PipelineDataSets = None,
        load: Load = None,
    ):
        self.country = country
        self.source = None
        self.set_secrets(secrets)
        self.set_settings(settings)
        self.load = Load(
            country=self.country, settings=self.settings, secrets=self.secrets
        )
        self.inputPathGrid = "./data/input"
        self.load = load
        if not os.path.exists(self.inputPathGrid):
            os.makedirs(self.inputPathGrid)
        self.data = data

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

    def get_data(self, country: str, source: str = None):
        """Get river discharge data from source and return AdminDataSet"""
        if source is None and self.source is None:
            raise RuntimeError("Source not specified, use set_source()")
        elif self.source is None and source is not None:
            self.source = source
        self.country = country
        if self.source == "GloFAS":
            self.prepare_glofas_data()
            self.extract_glofas_data()

    def prepare_glofas_data(self, country: str = None, debug: bool = False):
        """
        For each ensemble member, download the global NetCDF file and slice it to the extent of the country
        """
        if country is None:
            country = self.country
        logging.info(f"start preparing GloFAS data for country {country}")
        country_gdf = self.load.get_adm_boundaries(adm_level=1)
        no_ens = self.settings.get_setting("no_ensemble_members")
        date = datetime.today().strftime("%Y%m%d")
        if debug:
            no_ens = 1
            date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

        for ensemble in range(0, no_ens):

            filename_local_sliced = os.path.join(
                self.inputPathGrid,
                f"GloFAS_{date}_{country}_{ensemble}.nc",
            )
            if os.path.exists(filename_local_sliced):
                continue

            # Download netcdf file
            logging.info(f"downloading GloFAS data for ensemble {ensemble}")
            filename_local = os.path.join(self.inputPathGrid, f"GloFAS_{ensemble}.nc")
            try:
                self.load.get_from_blob(
                    filename_local,
                    f"{self.settings.get_setting('blob_storage_path')}"
                    f"/glofas-data/{date}/dis_{'{:02d}'.format(ensemble)}_{date}00.nc",
                )
            except FileNotFoundError:
                logging.warning(
                    f"NetCDF file of ensemble {ensemble} not found, skipping"
                )
                continue

            logging.info(f"slicing GloFAS data for ensemble {ensemble}")
            try:
                nc_file = xr.open_dataset(filename_local)
            except ValueError:
                logging.warning(
                    f"Something is wrong with this file, trying to download again"
                )
                self.load.get_from_blob(
                    filename_local,
                    f"{self.settings.get_setting('blob_storage_path')}"
                    f"/glofas-data/{date}/dis_{'{:02d}'.format(ensemble)}_{date}00.nc",
                )
                try:
                    nc_file = xr.open_dataset(filename_local)
                except ValueError:
                    logging.warning(
                        f"Something is definitely wrong with this file, skipping"
                    )
                    continue

            # Slice netcdf file to country boundaries
            country_bounds = country_gdf.total_bounds
            nc_file_sliced = slice_netcdf_file(nc_file, country_bounds)
            nc_file_sliced.to_netcdf(filename_local_sliced)

            nc_file.close()
            os.remove(filename_local)
        logging.info("finished preparing GloFAS data")

    def extract_glofas_data(self, country: str = None, debug: bool = False):
        """
        Download GloFAS data for each ensemble member
        and extract river discharge data per admin division and station
        """
        if country is None:
            country = self.country

        # Download pre-processed NetCDF files for each ensemble member
        no_ens = self.settings.get_setting("no_ensemble_members")
        date = datetime.today().strftime("%Y%m%d")
        if debug:
            no_ens = 1
            date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

        # # Download permanent water bodies and crop around country
        # country_gdf = self.load.get_adm_boundaries(country=country, adm_level=1)
        # country_gdf = country_gdf.to_crs("EPSG:4326")
        # lake_filepath = "data/updates/HydroLAKES_polys_v10.gdb"
        # if not os.path.exists(lake_filepath):
        #     self.load.get_from_blob(
        #         lake_filepath,
        #         f"{self.settings.get_setting('blob_storage_path')}/lakes/HydroLAKES_polys_v10.gdb",
        #     )
        # lake_gdf = gpd.read_file(lake_filepath)
        # lake_country_gdf = gpd.clip(
        #         lake_gdf, country_gdf.total_bounds, keep_geom_type=True
        #     )

        # Extract data from NetCDF files
        logging.info("Extract admin-level river discharge from GloFAS data")

        discharges = {}
        for adm_level in self.data.discharge_admin.adm_levels:
            try:
                country_gdf = self.load.get_adm_boundaries(adm_level=adm_level)
            except AttributeError:
                logging.error(
                    f"Country {country} does not have admin level {adm_level}, skipping"
                )
                continue
            for ensemble in range(0, no_ens):
                filename = os.path.join(
                    self.inputPathGrid,
                    f"GloFAS_{date}_{country}_{ensemble}.nc",
                )
                if not os.path.exists(filename):
                    logging.warning(
                        f"Country-specific NetCDF file of ensemble {ensemble} not found, skipping"
                    )
                    continue
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
                        discharges[key].append(
                            row["max"] if not pd.isna(row["max"]) else 0.0
                        )

            for lead_time, pcode in itertools.product(
                range(1, 8), list(country_gdf[f"adm{adm_level}_pcode"].unique())
            ):
                key = f"{pcode}_{lead_time}"
                self.data.discharge_admin.upsert_data_unit(
                    DischargeDataUnit(
                        adm_level=adm_level,
                        pcode=pcode,
                        lead_time=lead_time,
                        discharge_ensemble=discharges[key],
                    )
                )

        logging.info("Extract station-level river discharge from GloFAS data")

        discharges_stations = {}
        for ensemble in range(0, no_ens):
            filename = os.path.join(
                self.inputPathGrid,
                f"GloFAS_{date}_{country}_{ensemble}.nc",
            )
            if not os.path.exists(filename):
                logging.warning(
                    f"Country-specific NetCDF file of ensemble {ensemble} not found, skipping"
                )
                continue
            with rasterio.open(filename) as src:
                for station_code in self.data.threshold_station.get_station_codes():
                    station = self.data.threshold_station.get_data_unit(
                        station_code=station_code
                    )  # get station information from preloaded thresholds
                    coords = [(float(station.lon), float(station.lat))]
                    for lead_time in range(1, 8):
                        # Extract data for stations
                        discharge = float(
                            [x[0] for x in src.sample(coords, indexes=lead_time)][0]
                        )
                        key = f"{station.station_code}_{lead_time}"
                        if key not in discharges_stations.keys():
                            discharges_stations[key] = []
                        discharges_stations[key].append(
                            discharge if not pd.isna(discharge) else 0.0
                        )

        for station_code in self.data.threshold_station.get_station_codes():
            station = self.data.threshold_station.get_data_unit(
                station_code=station_code
            )
            for lead_time in range(1, 8):
                key = f"{station_code}_{lead_time}"
                self.data.discharge_station.upsert_data_unit(
                    DischargeStationDataUnit(
                        station_code=station_code,
                        station_name=station.station_name,
                        lat=station.lat,
                        lon=station.lon,
                        pcodes=station.pcodes,
                        lead_time=lead_time,
                        discharge_ensemble=discharges_stations[key],
                    )
                )
