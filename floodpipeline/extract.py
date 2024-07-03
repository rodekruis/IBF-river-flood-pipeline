from floodpipeline.secrets import Secrets
from floodpipeline.data import RiverDischargeDataSet
import os
from datetime import datetime, timedelta
import time
import pandas as pd
import xarray as xr
from rasterstats import zonal_stats
import rasterio
import logging
import geopandas as gpd
from typing import List
import urllib
import ftplib
logger = logging.getLogger(__name__)
supported_sources = ["GloFAS"]


class Extract:
    """ Extract river discharge data from external sources """
    
    def __init__(self, secrets: Secrets = None):
        self.source = None
        self.secrets = None
        self.inputPathGrid = './data/input'
        self.flood_dataset = None
        if not os.path.exists(self.inputPathGrid):
            os.makedirs(self.inputPathGrid)
        if secrets is not None:
            self.set_secrets(secrets)
    
    def set_secrets(self, secrets):
        """ Set secrets based on the data source """
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        missing_secrets = []
        if self.source == "GloFAS":
            missing_secrets = secrets.check_secrets(
                [
                    "GLOFAS_USER",
                    "GLOFAS_PASSWORD"
                ]
            )
        if missing_secrets:
            raise Exception(f"Missing secret(s) {', '.join(missing_secrets)} for source {self.source}")
        else:
            self.secrets = secrets
            return self
    
    def set_source(self, source_name, secrets: Secrets = None):
        """ Set the data source """
        if source_name is not None:
            if source_name not in supported_sources:
                raise ValueError(f"Source {source_name} is not supported."
                                 f"Supported sources are {', '.join(supported_sources)}")
            else:
                self.source = source_name
                self.inputPathGrid = os.path.join(self.inputPathGrid, self.source)
        else:
            raise ValueError(f"Source not specified; provide one of {', '.join(supported_sources)}")
        if secrets is not None:
            self.set_secrets(secrets)
        elif self.secrets is not None:
            self.set_secrets(self.secrets)
        else:
            raise ValueError(f"Set secrets before setting source")
        return self
    
    def get_data(self, country: str, adm_levels: List[int], source: str = None) -> RiverDischargeDataSet:
        """ Get river discharge data from source and return RiverDischargeDataSet """
        if source is None and self.source is None:
            raise RuntimeError("Source not specified, use set_source()")
        elif self.source is None and source is not None:
            self.source = source
        self.flood_dataset = RiverDischargeDataSet(country, datetime.today(), adm_levels)
        if self.source == "GloFAS":
            logging.info('Getting GloFAS data')
            self.flood_dataset = RiverDischargeDataSet(country, datetime.today(), adm_levels)
            netcdf_files = self._download_glofas_data_loop()
            self._extract_glofas_data(netcdf_files)
        return self.flood_dataset
    
    def _extract_glofas_data(self, netcdf_files: List[str]) -> RiverDischargeDataSet:
        for adm_level in self.flood_dataset.admin_levels:
            country_gdf = gpd.read_file(r"C:\Users\JMargutti\OneDrive - Rode Kruis\Rode Kruis\shapefiles\Uganda\uga_admbnda_adm1_UBOS_v2.shp")
            pcode_label = 'ADM1_PCODE'
            # TODO: GET COUNTRY ADMIN BOUNDARIES
            for filename in netcdf_files:
                for lead_time in range(0, 7):
                    with rasterio.open(filename) as src:
                        raster_array = src.read(lead_time + 1)
                        transform = src.transform
                    # Perform zonal statistics
                    stats = zonal_stats(country_gdf, raster_array, affine=transform, stats=['max', 'median'], all_touched=True)
                    dis = pd.DataFrame(stats)
                    for ix, row in dis.iterrows():
                        self.flood_dataset.add_ensemble_member(
                            adm_level=adm_level,
                            pcode=row[pcode_label],
                            lead_time=lead_time,
                            river_discharge=row['max']
                        )
    
    def _download_glofas_data_loop(self) -> List[str]:
        downloadDone = False
        timeToTryDownload = 43200
        timeToRetry = 6
        start = time.time()
        end = start + timeToTryDownload
        ntecdf_files = []
        while not downloadDone and time.time() < end:
            try:
                ntecdf_files = self._download_and_clip_glofas_data()
                downloadDone = True
            except:
                error = 'Download data failed. Will be trying again in ' + str(timeToRetry / 60) + ' minutes.'
                logger.error(error)
                time.sleep(timeToRetry)
        if not downloadDone:
            logger.error('GLofas download failed for ' +
                         str(timeToTryDownload / 3600) + ' hours, no new dataset was found')
            raise ValueError('GLofas download failed for ' +
                             str(timeToTryDownload / 3600) + ' hours, no new dataset was found')
        return ntecdf_files
    
    def _download_and_clip_glofas_data(self) -> List[str]:
        """
        Download one netcdf file per ensemble member and save it locally;
        clip the data to the extent of country and save it locally;
        return list of clipped files
        """
        
        logger.info(f'start downloading glofas data for ensemble')
        # The following extent will download data for the extent of Uganda
        min_lon = 29.5794661801
        max_lon = 35.03599
        min_lat = -1.44332244223
        max_lat = 4.24988494736
        # TODO: GET COUNTRY BOUNDARIES
        nofEns = 51  # number of ensemble members
        ntecdf_files = []
        for ensemble in range(0, nofEns):
            logger.info(f'start downloading data for ensemble {ensemble}')
            filename_local = os.path.join(self.inputPathGrid, f'GloFAS_{ensemble}.nc')
            filename_remote = f'dis_{"{:02d}".format(ensemble)}_{datetime.today().strftime("%Y%m%d")}00.nc'
            GLOFAS_FTP_GRID = f'aux.ecmwf.int/fc_netcdf/{datetime.today().strftime("%Y%m%d")}/'
            ftp_path = ('ftp://' + self.secrets.get_secret("GLOFAS_USER") + ':'
                        + self.secrets.get_secret("GLOFAS_PASSWORD") + '@' + GLOFAS_FTP_GRID)
            if not os.path.exists(filename_local):
                max_retries, retries = 5, 0
                while retries < max_retries:
                    try:
                        logger.info('accessing GloFAS data')
                        print(ftp_path + filename_remote)
                        urllib.request.urlretrieve(ftp_path + filename_remote, filename_local)
                        logger.info('downloaded GloFAS data')
                        break  # Connection successful, exit the loop
                    except ftplib.error_temp as e:
                        if "421 Maximum number of connections exceeded" in str(e):
                            retries += 1
                            logger.info("Retrying FTP connection...")
                            time.sleep(5)  # Wait for 5 seconds before retrying
                        else:
                            raise  # Reraise other FTP errors
                else:
                    logger.info("Max retries reached. Unable to establish FTP connection.")
            ntecdf_files.append(filename_local)
            
            nc_file = xr.open_dataset(filename_local)
            var_data = nc_file.sel(
                lon=slice(min_lon, max_lon),
                lat=slice(max_lat, min_lat)
            )
            filename_local = os.path.join(self.inputPathGrid, f'GloFAS_{ensemble}_{self.flood_dataset.country}.nc')
            var_data.to_netcdf(filename_local)
            nc_file.close()
            logger.info(f'finished downloading data for ensemble {ensemble}')
        logger.info('finished downloading data ')
        return ntecdf_files
