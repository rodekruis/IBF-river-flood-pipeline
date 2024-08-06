from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, RiverDischargeDataUnit
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

    def get_data(self, country: str, source: str = None) -> BaseDataSet:
        """Get river discharge data from source and return BaseDataSet"""
        if source is None and self.source is None:
            raise RuntimeError("Source not specified, use set_source()")
        elif self.source is None and source is not None:
            self.source = source
        self.country = country
        river_discharge_dataset = None
        if self.source == "GloFAS":
            logging.info("get GloFAS data")
            river_discharge_dataset = self.__download_extract_glofas_data()
        return river_discharge_dataset

    def __download_extract_glofas_data(self) -> BaseDataSet:
        """Download GloFAS data for each ensemble member and map to BaseDataSet"""

        river_discharge_dataset = BaseDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=self.settings.get_country_setting(self.country, "admin-levels"),
        )

        # Download NetCDF files for each ensemble member
        downloadDone = False
        timeToTryDownload = 43200
        timeToRetry = 6
        start = time.time()
        end = start + timeToTryDownload
        netcdf_files = []
        while not downloadDone and time.time() < end:
            try:
                netcdf_files = self.__download_and_clip_glofas_data()
                downloadDone = True
            except Exception as e:
                error = (
                    f"Download data failed: {e}. Will be trying again in "
                    + str(timeToRetry / 60)
                    + " minutes."
                )
                logging.error(error)
                time.sleep(timeToRetry)
        if not downloadDone:
            raise ValueError(
                "GLofas download failed for "
                + str(timeToTryDownload / 3600)
                + " hours, no new dataset was found"
            )

        # Extract data from NetCDF files
        logging.info("Extract zonal statistics from GloFAS data")
        for adm_level in river_discharge_dataset.adm_levels:
            country_gdf = self.load.get_adm_boundaries(
                country=self.country, adm_level=adm_level
            )
            for filename in netcdf_files:
                for lead_time in range(0, 8):
                    with rasterio.open(filename) as src:
                        raster_array = src.read(lead_time)
                        transform = src.transform
                    # Perform zonal statistics
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
                        try:
                            rddu = river_discharge_dataset.get_data_unit(
                                row[f"adm{adm_level}_pcode"], lead_time
                            )
                            rddu.river_discharge_ensemble.append(row["max"])
                            river_discharge_dataset.upsert_data_unit(rddu)
                        except ValueError:
                            river_discharge_dataset.upsert_data_unit(
                                RiverDischargeDataUnit(
                                    adm_level=adm_level,
                                    pcode=row[f"adm{adm_level}_pcode"],
                                    lead_time=lead_time,
                                    river_discharge_ensemble=[row["max"]],
                                )
                            )

            # Calculate average river discharge for each lead time
            for lead_time, pcode in itertools.product(
                river_discharge_dataset.get_lead_times(),
                river_discharge_dataset.get_pcodes(),
            ):
                river_discharge_dataset.get_data_unit(pcode, lead_time).compute_mean()

        return river_discharge_dataset

    def __get_ftp_path(self, date: str) -> str:
        """Get the FTP path for GloFAS data"""
        glofas_ftp_dir = f'{self.settings.get_setting("glofas_ftp_server")}/{date}/'
        ftp_path = (
            "ftp://"
            + self.secrets.get_secret("GLOFAS_USER")
            + ":"
            + self.secrets.get_secret("GLOFAS_PASSWORD")
            + "@"
            + glofas_ftp_dir
        )
        return ftp_path

    def __download_and_clip_glofas_data(self) -> List[str]:
        """
        Download one netcdf file per ensemble member and save it locally;
        slice the data to the extent of country and save it locally;
        return list of clipped files
        """
        logging.info(f"start downloading GloFAS data")
        country_gdf = self.load.get_adm_boundaries(country=self.country, adm_level=1)
        country_bounds = country_gdf.total_bounds
        nofEns = self.settings.get_setting("no_ensemble_members")
        ntecdf_files = []
        # date = datetime.today().strftime("%Y%m%d")
        date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

        for ensemble in range(0, nofEns):
            # Download netcdf file
            logging.info(f"start downloading GloFAS data for ensemble {ensemble}")
            filename_local = os.path.join(self.inputPathGrid, f"GloFAS_{ensemble}.nc")
            filename_remote = f'dis_{"{:02d}".format(ensemble)}_{date}00.nc'
            ftp_path = self.__get_ftp_path(date)
            if not os.path.exists(filename_local):
                max_retries, retries = 5, 0
                while retries < max_retries:
                    try:
                        urllib.request.urlretrieve(
                            ftp_path + filename_remote, filename_local
                        )
                        break  # Connection successful, exit the loop
                    except ftplib.error_temp as e:
                        if "421 Maximum number of connections exceeded" in str(e):
                            retries += 1
                            logging.info("Retrying FTP connection...")
                            time.sleep(5)  # Wait for 5 seconds before retrying
                        else:
                            raise  # Reraise other FTP errors
                else:
                    logging.info(
                        "Max retries reached. Unable to establish FTP connection."
                    )
            ntecdf_files.append(filename_local)
            # Slice netcdf file to country boundaries
            nc_file = xr.open_dataset(filename_local)
            nc_file_sliced = slice_netcdf_file(nc_file, country_bounds)
            filename_local_sliced = os.path.join(
                self.inputPathGrid,
                f"GloFAS_{ensemble}_{self.country}.nc",
            )
            nc_file_sliced.to_netcdf(filename_local_sliced)
            nc_file.close()
            logging.info(f"finished downloading GloFAS data for ensemble {ensemble}")
        logging.info("finished downloading GloFAS data")
        return ntecdf_files
