from __future__ import annotations

import os.path
import time

from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    AdminDataSet,
    AdminDataUnit,
    DischargeDataUnit,
    ForecastDataUnit,
    ThresholdDataUnit,
    StationDataUnit,
    StationDataSet,
    ThresholdStationDataUnit,
    ForecastStationDataUnit,
    DischargeStationDataUnit,
    PipelineDataSets,
)
from urllib.error import HTTPError
import json
from datetime import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import geopandas as gpd
from typing import List
import shutil
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError


def alert_class_to_severity(alert_class: str, triggered: bool) -> float:
    """Convert alert class to 'forecast_severity'"""
    if alert_class == "no":
        return 0.0
    elif alert_class == "min":
        return 0.3
    elif alert_class == "med":
        return 0.7
    elif alert_class == "max":
        if triggered:
            return 1.0
        else:
            return 0.7
    else:
        raise ValueError(f"Invalid alert class {alert_class}")


class Load:
    """Download/upload data from/to a data storage"""

    def __init__(
        self, country: str = None, settings: Settings = None, secrets: Secrets = None
    ):
        self.country = country
        self.secrets = None
        self.settings = None
        if settings is not None:
            self.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)
        self.rasters_sent = []

    def set_settings(self, settings):
        """Set settings"""
        if not isinstance(settings, Settings):
            raise TypeError(f"invalid format of settings, use settings.Settings")
        settings.check_settings(
            ["postgresql_server", "postgresql_port", "postgresql_database"]
        )
        self.settings = settings

    def set_secrets(self, secrets):
        """Set secrets for storage"""
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        secrets.check_secrets(
            [
                "COSMOS_URL",
                "COSMOS_KEY",
                "BLOB_ACCOUNT_NAME",
                "BLOB_ACCOUNT_KEY",
                "IBF_API_URL",
                "IBF_API_USER",
                "IBF_API_PASSWORD",
            ]
        )
        self.secrets = secrets

    def get_population_density(self, file_path: str):
        """Get population density data from worldpop and save to file_path"""
        r = requests.get(
            f"{self.settings.get_setting('worldpop_url')}/{self.country}/{self.country.lower()}_ppp_2022_1km_UNadj_constrained.tif"
        )
        if "404 Not Found" in str(r.content):
            raise FileNotFoundError(
                f"Population density data not found for country {self.country}"
            )
        with open(file_path, "wb") as file:
            file.write(r.content)

    def get_adm_boundaries(self, adm_level: int) -> gpd.GeoDataFrame:
        """Get admin areas from IBF API"""
        try:
            adm_boundaries = self.ibf_api_get_request(
                f"admin-areas/{self.country}/{adm_level}",
            )
        except HTTPError:
            raise FileNotFoundError(
                f"Admin areas for country {self.country}"
                f" and admin level {adm_level} not found"
            )
        gdf_adm_boundaries = gpd.GeoDataFrame.from_features(adm_boundaries["features"])
        gdf_adm_boundaries.set_crs(epsg=4326, inplace=True)
        return gdf_adm_boundaries

    def __ibf_api_authenticate(self):
        no_attempts, attempt, login_response = 5, 0, None
        while attempt < no_attempts:
            try:
                login_response = requests.post(
                    self.secrets.get_secret("IBF_API_URL") + "user/login",
                    data=[
                        ("email", self.secrets.get_secret("IBF_API_USER")),
                        ("password", self.secrets.get_secret("IBF_API_PASSWORD")),
                    ],
                )
                break
            except requests.exceptions.ConnectionError:
                attempt += 1
                logging.warning(
                    "IBF API currently not available, trying again in 1 minute"
                )
                time.sleep(60)
        if not login_response:
            raise ConnectionError("IBF API not available")
        return login_response.json()["user"]["token"]

    def ibf_api_post_request(self, path, body=None, files=None):
        token = self.__ibf_api_authenticate()
        if body is not None:
            headers = {
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        elif files is not None:
            headers = {"Authorization": "Bearer " + token}
        else:
            raise ValueError("No body or files provided")
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        r = session.post(
            self.secrets.get_secret("IBF_API_URL") + path,
            json=body,
            files=files,
            headers=headers,
        )
        if r.status_code >= 400:
            raise ValueError(
                f"Error in IBF API POST request: {r.status_code}, {r.text}"
            )

    def ibf_api_get_request(self, path, parameters=None):
        token = self.__ibf_api_authenticate()
        headers = {
            "Authorization": "Bearer " + token,
            "Accept": "*/*",
        }
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        r = session.get(
            self.secrets.get_secret("IBF_API_URL") + path,
            headers=headers,
            params=parameters,
        )
        if r.status_code >= 400:
            raise ValueError(f"Error in IBF API GET request: {r.status_code}, {r.text}")
        return r.json()

    def get_stations(self) -> list[dict]:
        """Get GloFAS stations from IBF app"""
        stations = self.ibf_api_get_request(
            f"point-data/glofas_stations/{self.country}",
            parameters={
                "disasterType": "flood",
                "pointDataCategory": "glofas_stations",
                "countryCodeISO3": self.country,
            },
        )
        gdf_stations = gpd.GeoDataFrame.from_features(stations["features"])
        stations = []
        for ix, row in gdf_stations.iterrows():
            station = {
                "stationCode": row["stationCode"],
                "stationName": row["stationName"],
                "lat": row["geometry"].y,
                "lon": row["geometry"].x,
            }
            stations.append(station)

        return stations

    def send_to_ibf_api(
        self,
        forecast_data: AdminDataSet,
        forecast_station_data: StationDataSet,
        discharge_station_data: StationDataSet,
        flood_extent: str = None,
        upload_time: str = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
    ):
        """Send flood forecast data to IBF API"""

        trigger_on_lead_time = self.settings.get_country_setting(
            self.country, "trigger-on-lead-time"
        )
        trigger_on_return_period = self.settings.get_country_setting(
            self.country, "trigger-on-return-period"
        )
        threshold_station_data = self.get_thresholds_station()

        processed_stations, processed_pcodes, triggered_lead_times = [], [], []

        # START EVENT LOOP
        for station_code in forecast_station_data.get_station_codes():

            # determine events
            events = {}
            for lead_time in range(1, 8):
                if (
                    forecast_station_data.get_data_unit(
                        station_code, lead_time
                    ).alert_class
                    != "no"
                ):
                    events[lead_time] = "alert"
            for lead_time in range(1, 8):
                if forecast_station_data.get_data_unit(
                    station_code, lead_time
                ).triggered:
                    events[lead_time] = "trigger"
                    triggered_lead_times.append(lead_time)
            if not events:
                continue
            events = dict(sorted(events.items()))

            for lead_time_event, event_type in events.items():

                # set as alert if lead time is greater than trigger_on_lead_time
                if lead_time_event > trigger_on_lead_time and event_type == "trigger":
                    event_type = "alert"
                station_name = forecast_station_data.get_data_unit(
                    station_code, trigger_on_lead_time
                ).station_name
                event_name = str(station_name) if station_name else str(station_code)
                if event_name == "" or event_name == "None" or event_name == "Na":
                    event_name = str(station_code)

                logging.info(
                    f"event {event_name}, type '{event_type}', lead time {lead_time_event}"
                )
                forecast_station = forecast_station_data.get_data_unit(
                    station_code, lead_time_event
                )
                threshold_station = threshold_station_data.get_data_unit(station_code)

                # send exposure data: admin-area-dynamic-data/exposure
                indicators = [
                    "population_affected",
                    "population_affected_percentage",
                    "forecast_severity",
                    "forecast_trigger",
                ]
                for indicator in indicators:
                    for adm_level in forecast_station.pcodes.keys():
                        exposure_pcodes = []
                        for pcode in forecast_station.pcodes[adm_level]:
                            forecast_admin = forecast_data.get_data_unit(
                                pcode, lead_time_event
                            )
                            amount = None
                            if indicator == "population_affected":
                                amount = forecast_admin.pop_affected
                            elif indicator == "population_affected_percentage":
                                amount = forecast_admin.pop_affected_perc
                            elif indicator == "forecast_severity":
                                amount = alert_class_to_severity(
                                    alert_class=forecast_admin.alert_class,
                                    triggered=(
                                        True if event_type == "trigger" else False
                                    ),
                                )
                            elif indicator == "forecast_trigger":
                                forecast_severity = alert_class_to_severity(
                                    alert_class=forecast_admin.alert_class,
                                    triggered=(
                                        True if event_type == "trigger" else False
                                    ),
                                )
                                # Currently (with high-warning not facilitated yet): set forecast_trigger to 1 exactly for those case where forecast_severity is 1
                                if event_type == "trigger" and forecast_severity == 1.0:
                                    amount = 1
                                else:
                                    amount = 0
                            exposure_pcodes.append(
                                {"placeCode": pcode, "amount": amount}
                            )
                            processed_pcodes.append(pcode)
                        body = {
                            "countryCodeISO3": self.country,
                            "leadTime": f"{lead_time_event}-day",
                            "dynamicIndicator": indicator,
                            "adminLevel": int(adm_level),
                            "exposurePlaceCodes": exposure_pcodes,
                            "disasterType": "floods",
                            "eventName": event_name,
                            "date": upload_time,
                        }
                        self.ibf_api_post_request(
                            "admin-area-dynamic-data/exposure", body=body
                        )
                processed_pcodes = list(set(processed_pcodes))

                # GloFAS station data: point-data/dynamic
                # 1 call per alert/triggered station, and 1 overall (to same endpoint) for all other stations
                if event_type != "none":
                    station_forecasts = {
                        "forecastLevel": [],
                        "eapAlertClass": [],
                        "forecastReturnPeriod": [],
                        "triggerLevel": [],
                    }
                    discharge_station = discharge_station_data.get_data_unit(
                        station_code, lead_time_event
                    )
                    for indicator in station_forecasts.keys():
                        value = None
                        if indicator == "forecastLevel":
                            value = int(discharge_station.discharge_mean or 0)
                        elif indicator == "eapAlertClass":
                            value = forecast_station.alert_class
                            if event_type == "alert" and value == "max":
                                value = "med"
                        elif indicator == "forecastReturnPeriod":
                            value = forecast_station.return_period
                        elif indicator == "triggerLevel":
                            value = int(
                                threshold_station.get_threshold(
                                    trigger_on_return_period
                                )
                            )
                        station_data = {"fid": station_code, "value": value}
                        station_forecasts[indicator].append(station_data)
                        body = {
                            "leadTime": f"{lead_time_event}-day",
                            "key": indicator,
                            "dynamicPointData": station_forecasts[indicator],
                            "pointDataCategory": "glofas_stations",
                            "disasterType": "floods",
                            "countryCodeISO3": self.country,
                            "date": upload_time,
                        }
                        self.ibf_api_post_request("point-data/dynamic", body=body)
                    processed_stations.append(station_code)

            # send alerts per lead time: event/alerts-per-lead-time
            alerts_per_lead_time = []
            for lead_time in range(0, 8):
                is_trigger, is_trigger_or_alert = False, False
                for lead_time_event, event_type in events.items():
                    if event_type == "trigger" and lead_time >= lead_time_event:
                        is_trigger = True
                    if (
                        event_type == "trigger" or event_type == "alert"
                    ) and lead_time >= lead_time_event:
                        is_trigger_or_alert = True
                alerts_per_lead_time.append(
                    {
                        "leadTime": f"{lead_time}-day",
                        "forecastAlert": is_trigger_or_alert,
                        "forecastTrigger": is_trigger,
                    }
                )
            body = {
                "countryCodeISO3": self.country,
                "alertsPerLeadTime": alerts_per_lead_time,
                "disasterType": "floods",
                "eventName": event_name,
                "date": upload_time,
            }
            self.ibf_api_post_request("event/alerts-per-lead-time", body=body)

        # END OF EVENT LOOP
        ###############################################################################################################

        # flood extent raster: admin-area-dynamic-data/raster/floods
        self.rasters_sent = []
        for lead_time in range(0, 8):
            flood_extent_new = flood_extent.replace(
                ".tif", f"_{lead_time}-day_{self.country}.tif"
            )
            if lead_time in triggered_lead_times:
                shutil.copy(
                    flood_extent.replace(".tif", f"_{lead_time}.tif"), flood_extent_new
                )
            else:
                shutil.copy(
                    flood_extent.replace(".tif", f"_empty.tif"),
                    flood_extent_new,
                )
            self.rasters_sent.append(flood_extent_new)
            files = {"file": open(flood_extent_new, "rb")}
            self.ibf_api_post_request(
                "admin-area-dynamic-data/raster/floods", files=files
            )

        # send empty exposure data
        if len(processed_pcodes) == 0:
            indicators = [
                "population_affected",
                "population_affected_percentage",
                "forecast_severity",
                "forecast_trigger",
            ]
            for indicator in indicators:
                for adm_level in forecast_data.adm_levels:
                    exposure_pcodes = []
                    for pcode in forecast_data.get_pcodes(adm_level=adm_level):
                        if pcode not in processed_pcodes:
                            amount = None
                            if indicator == "population_affected":
                                amount = 0
                            elif indicator == "population_affected_percentage":
                                amount = 0.0
                            elif indicator == "forecast_severity":
                                amount = 0.0
                            elif indicator == "forecast_trigger":
                                amount = 0.0
                            exposure_pcodes.append(
                                {"placeCode": pcode, "amount": amount}
                            )
                    body = {
                        "countryCodeISO3": self.country,
                        "leadTime": "1-day",  # this is a specific check IBF uses to establish no-trigger
                        "dynamicIndicator": indicator,
                        "adminLevel": adm_level,
                        "exposurePlaceCodes": exposure_pcodes,
                        "disasterType": "floods",
                        "eventName": None,  # this is a specific check IBF uses to establish no-trigger
                        "date": upload_time,
                    }
                    self.ibf_api_post_request(
                        "admin-area-dynamic-data/exposure", body=body
                    )

        # send GloFAS station data for all other stations
        station_forecasts = {
            "forecastLevel": [],
            "eapAlertClass": [],
            "forecastReturnPeriod": [],
            "triggerLevel": [],
        }
        for indicator in station_forecasts.keys():
            for station_code in forecast_station_data.get_station_codes():
                if station_code not in processed_stations:
                    discharge_station = discharge_station_data.get_data_unit(
                        station_code, trigger_on_lead_time
                    )
                    forecast_station = forecast_station_data.get_data_unit(
                        station_code, trigger_on_lead_time
                    )
                    threshold_station = threshold_station_data.get_data_unit(
                        station_code
                    )
                    value = None
                    if indicator == "forecastLevel":
                        value = int(discharge_station.discharge_mean or 0)
                    elif indicator == "eapAlertClass":
                        value = forecast_station.alert_class
                    elif indicator == "forecastReturnPeriod":
                        value = forecast_station.return_period
                    elif indicator == "triggerLevel":
                        value = int(
                            threshold_station.get_threshold(trigger_on_return_period)
                        )
                    station_data = {"fid": station_code, "value": value}
                    station_forecasts[indicator].append(station_data)

            body = {
                "leadTime": f"7-day",
                "key": indicator,
                "dynamicPointData": station_forecasts[indicator],
                "pointDataCategory": "glofas_stations",
                "disasterType": "floods",
                "countryCodeISO3": self.country,
                "date": upload_time,
            }
            self.ibf_api_post_request("point-data/dynamic", body=body)

        # process events: events/process
        body = {
            "countryCodeISO3": self.country,
            "disasterType": "floods",
            "date": upload_time,
        }
        self.ibf_api_post_request("events/process", body=body)

    def get_thresholds_station(self):
        """Get GloFAS station thresholds from config file"""
        data_units = []
        if not os.path.exists(rf"config/{self.country}_station_thresholds.json"):
            raise FileNotFoundError(
                f"No station thresholds config file found for country {self.country}"
            )
        with open(rf"config/{self.country}_station_thresholds.json", "r") as read_file:
            station_thresholds = json.load(read_file)
            for station in station_thresholds:
                data_units.append(
                    ThresholdStationDataUnit(
                        station_code=station["station_code"],
                        station_name=station["station_name"],
                        lat=station["lat"],
                        lon=station["lon"],
                        pcodes=station["pcodes"],
                        thresholds=station["thresholds"],
                    )
                )
        dataset = StationDataSet(
            country=self.country,
            data_units=data_units,
        )
        return dataset

    def save_thresholds_station(self, data: List[ThresholdStationDataUnit]):
        """Save GloFAS station thresholds to config file"""
        # TBI validate before save
        with open(rf"config/{self.country}_station_thresholds.json", "w") as file:
            json.dump([record.__dict__ for record in data], file)

    def get_thresholds_admin(self):
        """Get GloFAS admin area thresholds from config file"""
        data_units = []
        if not os.path.exists(rf"config/{self.country}_admin_thresholds.json"):
            raise FileNotFoundError(
                f"No admin thresholds config file found for country {self.country}"
            )
        with open(rf"config/{self.country}_admin_thresholds.json", "r") as read_file:
            admin_thresholds = json.load(read_file)
            for record in admin_thresholds:
                data_units.append(
                    ThresholdDataUnit(
                        adm_level=record["adm_level"],
                        pcode=record["pcode"],
                        thresholds=record["thresholds"],
                    )
                )
        dataset = AdminDataSet(
            country=self.country,
            timestamp=datetime.now(),
            data_units=data_units,
        )
        return dataset

    def save_thresholds_admin(self, data: List[ThresholdDataUnit]):
        """Save GloFAS admin area thresholds to config file"""
        # TBI validate before save
        with open(rf"config/{self.country}_admin_thresholds.json", "w") as file:
            json.dump([record.__dict__ for record in data], file)

    def __get_blob_service_client(self, blob_path: str):
        """Get service client for Azure Blob Storage"""
        blob_service_client = BlobServiceClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;"
            f'AccountName={self.secrets.get_secret("BLOB_ACCOUNT_NAME")};'
            f'AccountKey={self.secrets.get_secret("BLOB_ACCOUNT_KEY")};'
            f"EndpointSuffix=core.windows.net"
        )
        container = self.settings.get_setting("blob_container")
        return blob_service_client.get_blob_client(container=container, blob=blob_path)

    def save_to_blob(self, local_path: str, file_dir_blob: str):
        """Save file to Azure Blob Storage"""
        # upload to Azure Blob Storage
        blob_client = self.__get_blob_service_client(file_dir_blob)
        with open(local_path, "rb") as upload_file:
            blob_client.upload_blob(upload_file, overwrite=True)

    def get_from_blob(self, local_path: str, blob_path: str):
        """Get file from Azure Blob Storage"""
        blob_client = self.__get_blob_service_client(blob_path)

        with open(local_path, "wb") as download_file:
            try:
                download_file.write(blob_client.download_blob(timeout=120).readall())
            except ResourceNotFoundError:
                raise FileNotFoundError(
                    f"File {blob_path} not found in Azure Blob Storage"
                )
