import os.path
import copy
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
)
from urllib.error import HTTPError
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timedelta, date
import azure.cosmos.cosmos_client as cosmos_client
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import geopandas as gpd
from shapely import Point
from typing import List
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

COSMOS_DATA_TYPES = [
    "discharge",
    "forecast",
    "threshold",
    "discharge-station",
    "forecast-station",
    "threshold-station",
]


def get_cosmos_query(
    start_date=None,
    end_date=None,
    country=None,
    adm_level=None,
    pcode=None,
    lead_time=None,
):
    query = "SELECT * FROM c WHERE "
    if start_date is not None:
        query += f'c.timestamp >= "{start_date.strftime("%Y-%m-%dT%H:%M:%S")}" '
    if end_date is not None:
        query += f'AND c.timestamp <= "{end_date.strftime("%Y-%m-%dT%H:%M:%S")}" '
    if country is not None:
        query += f'AND c.country = "{country}" '
    if adm_level is not None:
        query += f'AND c.adm_level = "{adm_level}" '
    if pcode is not None:
        query += f'AND c.adm_level = "{pcode}" '
    if lead_time is not None:
        query += f'AND c.adm_level = "{lead_time}" '
    if query.endswith("WHERE "):
        query = query.replace("WHERE ", "")
    query = query.replace("WHERE AND", "WHERE")
    return query


def get_data_unit_id(data_unit: AdminDataUnit, dataset: AdminDataSet):
    """Get data unit ID"""
    if hasattr(data_unit, "pcode"):
        if hasattr(data_unit, "lead_time"):
            id_ = f"{data_unit.pcode}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}_{data_unit.lead_time}"
        else:
            id_ = f"{data_unit.pcode}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}"
    elif hasattr(data_unit, "station_code"):
        if hasattr(data_unit, "lead_time"):
            id_ = f"{data_unit.station_code}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}_{data_unit.lead_time}"
        else:
            id_ = f"{data_unit.station_code}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}"
    else:
        id_ = f"{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}"
    return id_


def alert_class_to_threshold(alert_class: str, triggered: bool) -> float:
    """Convert alert class to 'alert_threshold'"""
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

    def __init__(self, settings: Settings = None, secrets: Secrets = None):
        self.secrets = None
        self.settings = None
        if settings is not None:
            self.set_settings(settings)
        if secrets is not None:
            self.set_secrets(secrets)

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
                "BLOB_CONTAINER_NAME",
                "IBF_API_URL",
                "IBF_API_USER",
                "IBF_API_PASSWORD",
            ]
        )
        self.secrets = secrets

    def get_population_density(self, country: str, file_path: str):
        """Get population density data from worldpop and save to file_path"""
        r = requests.get(
            f"{self.settings.get_setting('worldpop_url')}/{country.upper()}/{country.lower()}_ppp_2022_1km_UNadj_constrained.tif"
        )
        if "404 Not Found" in str(r.content):
            raise FileNotFoundError(
                f"Population density data not found for country {country}"
            )
        with open(file_path, "wb") as file:
            file.write(r.content)

    def get_adm_boundaries(self, country: str, adm_level: int) -> gpd.GeoDataFrame:
        """Get administrative boundaries from IBF API"""
        try:
            gdf = gpd.read_file(
                f"https://raw.githubusercontent.com/rodekruis/IBF-system/master/services/API-service/src/scripts/git-lfs/admin-boundaries/{country}_adm{adm_level}.json"
            )
            gdf = gdf.rename(columns={f"ADM{adm_level}_PCODE": f"adm{adm_level}_pcode"})
        except HTTPError:
            raise FileNotFoundError(
                f"Administrative boundaries for country {country} "
                f"and admin level {adm_level} not found"
            )
        # """Get administrative boundaries from PostgreSQL database"""
        # engine = create_engine(
        #     f"postgresql://{self.secrets.get_secret('SQL_USER')}:"
        #     f"{self.secrets.get_secret('SQL_PASSWORD')}"
        #     f"@{self.settings.get_setting('postgresql_server')}:"
        #     f"{self.settings.get_setting('postgresql_port')}/"
        #     f"{self.settings.get_setting('postgresql_database')}"
        # )
        # gdf = gpd.GeoDataFrame()
        # try:
        #     sql = f"SELECT geometry, adm{adm_level}_pcode FROM admin_boundaries_pcoded.{country.lower()}_adm{adm_level}"
        #     gdf = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col="geometry")
        # except ProgrammingError:
        #     logging.warning(
        #         f"WARNING: no administrative boundaries found for country {country} "
        #         f"and adm_level {adm_level}"
        #     )
        return gdf

    def __ibf_api_authenticate(self):
        login_response = requests.post(
            self.secrets.get_secret("IBF_API_URL") + "user/login",
            data=[
                ("email", self.secrets.get_secret("IBF_API_USER")),
                ("password", self.secrets.get_secret("IBF_API_PASSWORD")),
            ],
        )
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
        if os.path.exists("logs"):
            if body:
                filename = body["date"] + ".json"
                filename = "".join(x for x in filename if x.isalnum())
                filename = os.path.join("logs", filename)
                logs = {"endpoint": path, "payload": body}
                with open(filename, "a") as file:
                    file.write(str(logs) + "\n")
            elif files:
                filename = datetime.today().strftime("%Y%m%d") + ".json"
                filename = os.path.join("logs", filename)
                logs = {"endpoint": path, "payload": files}
                with open(filename, "a") as file:
                    file.write(str(logs) + "\n")

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

    def get_stations(
        self, country: str, pcodes_from: str = "threshold-station"
    ) -> StationDataSet:
        """Get GloFAS stations from IBF API"""
        stations = self.ibf_api_get_request(
            f"glofas-stations/{country}",
        )
        # get pcodes
        if pcodes_from == "threshold-station":
            station_data = self.get_pipeline_data(
                data_type="threshold-station", country=country
            )
        else:
            raise ValueError(f"Invalid pcodes_from {pcodes_from}")

        station_dataset = StationDataSet(
            country=country,
            timestamp=datetime.today(),
        )
        for station in stations:
            pcodes = station_data.get_data_unit(station["stationCode"]).pcodes
            for lead_time in range(1, 8):
                station_dataset.upsert_data_unit(
                    DischargeStationDataUnit(
                        station_code=station["stationCode"],
                        station_name=station["stationName"],
                        lat=station["lat"],
                        lon=station["lon"],
                        pcodes=pcodes,
                        lead_time=lead_time,
                    )
                )
        return station_dataset

    def send_to_ibf_api(
        self,
        forecast_data: AdminDataSet,
        discharge_data: AdminDataSet,
        forecast_station_data: StationDataSet,
        discharge_station_data: StationDataSet,
        flood_extent: str = None,
    ):
        """Send flood forecast data to IBF API"""
        upload_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        country = forecast_data.country
        trigger_on_lead_time = self.settings.get_country_setting(
            country, "trigger-on-lead-time"
        )
        trigger_on_return_period = self.settings.get_country_setting(
            country, "trigger-on-return-period"
        )
        threshold_station_data = self.get_pipeline_data(
            data_type="threshold-station", country=country
        )

        processed_stations, processed_pcodes = [], []

        # START EVENT LOOP
        for station_code in forecast_station_data.get_station_codes():

            # determine event lead time
            lead_time_event = None
            event_type = "none"
            for lead_time in range(1, 8):
                if forecast_station_data.get_data_unit(
                    station_code, lead_time
                ).triggered:
                    lead_time_event = lead_time
                    event_type = "trigger"
                    break
            if lead_time_event is None:
                for lead_time in range(1, 8):
                    if (
                        forecast_station_data.get_data_unit(
                            station_code, lead_time
                        ).alert_class
                        != "no"
                    ):
                        lead_time_event = lead_time
                        event_type = "alert"
                        break
            if lead_time_event is None:
                continue

            # set as alert if lead time is greater than trigger_on_lead_time
            if lead_time_event > trigger_on_lead_time and event_type == "trigger":
                event_type = "alert"
            station_name = forecast_station_data.get_data_unit(station_code, trigger_on_lead_time).station_name
            event_name = str(station_name) if station_name else str(station_code)
            if event_name == "":
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
                "alert_threshold",
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
                        elif indicator == "alert_threshold":
                            amount = alert_class_to_threshold(
                                alert_class=forecast_admin.alert_class,
                                triggered=True if event_type == "trigger" else False
                            )
                        exposure_pcodes.append({"placeCode": pcode, "amount": amount})
                        processed_pcodes.append(pcode)
                    body = {
                        "countryCodeISO3": forecast_data.country,
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

            # send trigger per lead time: event/triggers-per-leadtime
            triggers_per_lead_time = []
            for lead_time in range(1, 8):
                is_trigger, is_trigger_or_alert = False, False
                if event_type == "trigger" and lead_time >= lead_time_event:
                    is_trigger = True
                if (
                    event_type == "trigger" or event_type == "alert"
                ) and lead_time >= lead_time_event:
                    is_trigger_or_alert = True
                triggers_per_lead_time.append(
                    {
                        "leadTime": f"{lead_time}-day",
                        "triggered": is_trigger_or_alert,
                        "thresholdReached": is_trigger,
                    }
                )
            body = {
                "countryCodeISO3": forecast_data.country,
                "triggersPerLeadTime": triggers_per_lead_time,
                "disasterType": "floods",
                "eventName": event_name,
                "date": upload_time,
            }
            self.ibf_api_post_request("event/triggers-per-leadtime", body=body)

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
                        value = int(threshold_station.get_threshold(
                            trigger_on_return_period
                        ))
                    station_data = {"fid": station_code, "value": value}
                    station_forecasts[indicator].append(station_data)
                    body = {
                        "leadTime": f"{lead_time_event}-day",
                        "key": indicator,
                        "dynamicPointData": station_forecasts[indicator],
                        "pointDataCategory": "glofas_stations",
                        "disasterType": "floods",
                        "date": upload_time,
                    }
                    self.ibf_api_post_request("point-data/dynamic", body=body)
                    processed_stations.append(station_code)

        # END OF EVENT LOOP
        ###############################################################################################################

        # flood extent raster: admin-area-dynamic-data/raster/floods
        if flood_extent is not None:
            if not os.path.exists(flood_extent):
                raise FileNotFoundError(f"Flood extent raster {flood_extent} not found")
            files = {"file": open(flood_extent, "rb")}
            self.ibf_api_post_request(
                "admin-area-dynamic-data/raster/floods", files=files
            )

        # send empty exposure data
        if len(processed_pcodes) == 0:
            indicators = [
                "population_affected",
                "population_affected_percentage",
                "alert_threshold",
            ]
            for indicator in indicators:
                for adm_level in forecast_data.adm_levels:
                    exposure_pcodes = []
                    for pcode in forecast_data.get_pcodes(adm_level=adm_level):
                        if pcode not in processed_pcodes:
                            forecast_data_unit = forecast_data.get_data_unit(
                                pcode, trigger_on_lead_time
                            )
                            amount = None
                            if indicator == "population_affected":
                                amount = forecast_data_unit.pop_affected
                            elif indicator == "population_affected_percentage":
                                amount = forecast_data_unit.pop_affected_perc
                            elif indicator == "alert_threshold":
                                amount = alert_class_to_threshold(
                                    alert_class=forecast_data_unit.alert_class,
                                    triggered=False
                                )
                            exposure_pcodes.append(
                                {"placeCode": pcode, "amount": amount}
                            )
                    body = {
                        "countryCodeISO3": forecast_data.country,
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
                        value = int(threshold_station.get_threshold(
                            trigger_on_return_period
                        ))
                    station_data = {"fid": station_code, "value": value}
                    station_forecasts[indicator].append(station_data)

            body = {
                "leadTime": f"7-day",
                "key": indicator,
                "dynamicPointData": station_forecasts[indicator],
                "pointDataCategory": "glofas_stations",
                "disasterType": "floods",
                "date": upload_time,
            }
            self.ibf_api_post_request("point-data/dynamic", body=body)

        # close events: event/close-events
        body = {
            "countryCodeISO3": country,
            "disasterType": "floods",
            "date": upload_time,
        }
        self.ibf_api_post_request("event/close-events", body=body)

    def save_pipeline_data(
        self, data_type: str, dataset: AdminDataSet, replace_country: bool = False
    ):
        """Upload pipeline datasets to Cosmos DB"""
        if data_type not in COSMOS_DATA_TYPES:
            raise ValueError(
                f"Data type {data_type} is not supported."
                f"Supported storages are {', '.join(COSMOS_DATA_TYPES)}"
            )
        # check data types
        if data_type == "discharge":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, DischargeDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type DischargeDataUnit"
                    )
        elif data_type == "forecast":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, ForecastDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type ForecastDataUnit"
                    )
        elif data_type == "threshold":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, ThresholdDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type ThresholdDataUnit"
                    )
        elif data_type == "discharge-station":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, DischargeStationDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type DischargeStationDataUnit"
                    )
        elif data_type == "forecast-station":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, ForecastStationDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type ForecastStationDataUnit"
                    )
        elif data_type == "threshold-station":
            for data_unit in dataset.data_units:
                if not isinstance(data_unit, ThresholdStationDataUnit):
                    raise ValueError(
                        f"Data unit {data_unit} is not of type ThresholdStationDataUnit"
                    )

        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {"masterKey": self.secrets.get_secret("COSMOS_KEY")},
            user_agent="sml-api",
            user_agent_overwrite=True,
        )
        cosmos_db = client_.get_database_client("flood-pipeline")
        cosmos_container_client = cosmos_db.get_container_client(data_type)
        if replace_country:
            query = get_cosmos_query(country=dataset.country)
            old_records = cosmos_container_client.query_items(query)
            for old_record in old_records:
                cosmos_container_client.delete_item(
                    item=old_record.get("id"), partition_key=dataset.country
                )
        for data_unit in dataset.data_units:
            record = vars(data_unit)
            record["timestamp"] = dataset.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
            record["country"] = dataset.country
            record["id"] = get_data_unit_id(data_unit, dataset)
            cosmos_container_client.upsert_item(body=record)

    def get_pipeline_data(
        self,
        data_type,
        country,
        start_date=None,
        end_date=None,
        adm_level=None,
        pcode=None,
        lead_time=None,
    ) -> AdminDataSet:
        """Download pipeline datasets from Cosmos DB"""
        if data_type not in COSMOS_DATA_TYPES:
            raise ValueError(
                f"Data type {data_type} is not supported."
                f"Supported storages are {', '.join(COSMOS_DATA_TYPES)}"
            )
        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {"masterKey": self.secrets.get_secret("COSMOS_KEY")},
            user_agent="ibf-flood-pipeline",
            user_agent_overwrite=True,
        )
        cosmos_db = client_.get_database_client("flood-pipeline")
        cosmos_container_client = cosmos_db.get_container_client(data_type)
        query = get_cosmos_query(
            start_date, end_date, country, adm_level, pcode, lead_time
        )
        records_query = cosmos_container_client.query_items(
            query=query,
            enable_cross_partition_query=(
                True if country is None else None
            ),  # country must be the partition key
        )
        records = []
        for record in records_query:
            records.append(copy.deepcopy(record))
        datasets = []
        countries = list(set([record["country"] for record in records]))
        timestamps = list(set([record["timestamp"] for record in records]))
        for country in countries:
            for timestamp in timestamps:
                data_units = []
                for record in records:
                    if (
                        record["country"] == country
                        and record["timestamp"] == timestamp
                    ):
                        if data_type == "discharge":
                            data_unit = DischargeDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                lead_time=record["lead_time"],
                                discharge_mean=record["discharge_mean"],
                                discharge_ensemble=record["discharge_ensemble"],
                            )
                        elif data_type == "forecast":
                            data_unit = ForecastDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                lead_time=record["lead_time"],
                                forecasts=record["forecasts"],
                                pop_affected=record["pop_affected"],
                                pop_affected_perc=record["pop_affected_perc"],
                                triggered=record["triggered"],
                                return_period=record["return_period"],
                                alert_class=record["alert_class"],
                            )
                        elif data_type == "threshold":
                            data_unit = ThresholdDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                thresholds=record["thresholds"],
                            )
                        elif data_type == "discharge-station":
                            data_unit = DischargeStationDataUnit(
                                station_code=record["station_code"],
                                station_name=record["station_name"],
                                lat=record["lat"],
                                lon=record["lon"],
                                pcodes=record["pcodes"],
                                lead_time=record["lead_time"],
                                discharge_mean=record["discharge_mean"],
                                discharge_ensemble=record["discharge_ensemble"],
                            )
                        elif data_type == "forecast-station":
                            data_unit = ForecastStationDataUnit(
                                station_code=record["station_code"],
                                station_name=record["station_name"],
                                lat=record["lat"],
                                lon=record["lon"],
                                pcodes=record["pcodes"],
                                lead_time=record["lead_time"],
                                forecasts=record["forecasts"],
                                triggered=record["triggered"],
                                return_period=record["return_period"],
                                alert_class=record["alert_class"],
                            )
                        elif data_type == "threshold-station":
                            data_unit = ThresholdStationDataUnit(
                                station_code=record["station_code"],
                                station_name=record["station_name"],
                                lat=record["lat"],
                                lon=record["lon"],
                                pcodes=record["pcodes"],
                                thresholds=record["thresholds"],
                            )
                        else:
                            raise ValueError(f"Invalid data type {data_type}")
                        data_units.append(data_unit)
                if (
                    data_type == "discharge"
                    or data_type == "forecast"
                    or data_type == "threshold"
                ):
                    adm_levels = list(
                        set([data_unit.adm_level for data_unit in data_units])
                    )
                    dataset = AdminDataSet(
                        country=country,
                        timestamp=timestamp,
                        adm_levels=adm_levels,
                        data_units=data_units,
                    )
                    datasets.append(dataset)
                else:
                    dataset = StationDataSet(
                        country=country,
                        timestamp=timestamp,
                        data_units=data_units,
                    )
                    datasets.append(dataset)
        if len(datasets) == 0:
            raise KeyError(
                f"No datasets of type '{data_type}' found for country {country} in date range "
                f"{start_date}-{end_date}."
            )
        elif len(datasets) > 1:
            logging.warning(
                f"Multiple datasets of type '{data_type}' found for country {country} in date range "
                f"{start_date}-{end_date}; returning the latest (timestamp {datasets[-1].timestamp}). "
            )
        return datasets[-1]

    def __get_blob_service_client(self, blob_path: str):
        """Get service client for Azure Blob Storage"""
        blob_service_client = BlobServiceClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;"
            f'AccountName={self.secrets.get_secret("BLOB_ACCOUNT_NAME")};'
            f'AccountKey={self.secrets.get_secret("BLOB_ACCOUNT_KEY")};'
            f"EndpointSuffix=core.windows.net"
        )
        container = self.secrets.get_secret("BLOB_CONTAINER_NAME")
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
                download_file.write(blob_client.download_blob().readall())
            except ResourceNotFoundError:
                raise FileNotFoundError(f"File {blob_path} not found in Azure Blob Storage")
