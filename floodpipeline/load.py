import os.path
import copy
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import (
    BaseDataSet,
    BaseDataUnit,
    RiverDischargeDataUnit,
    FloodForecastDataUnit,
    TriggerThresholdDataUnit,
)
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timedelta, date
import azure.cosmos.cosmos_client as cosmos_client
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import geopandas as gpd
from typing import List
from azure.storage.blob import BlobServiceClient

COSMOS_DATA_TYPES = ["river-discharge", "flood-forecast", "trigger-threshold"]


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


def get_data_unit_id(data_unit: BaseDataUnit, dataset: BaseDataSet):
    """Get data unit ID"""
    if hasattr(data_unit, "lead_time"):
        id_ = f"{data_unit.pcode}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}_{data_unit.lead_time}"
    else:
        id_ = f"{data_unit.pcode}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}"
    return id_


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
                "SQL_USER",
                "SQL_PASSWORD",
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
        """Get administrative boundaries from PostgreSQL database"""
        engine = create_engine(
            f"postgresql://{self.secrets.get_secret('SQL_USER')}:"
            f"{self.secrets.get_secret('SQL_PASSWORD')}"
            f"@{self.settings.get_setting('postgresql_server')}:"
            f"{self.settings.get_setting('postgresql_port')}/"
            f"{self.settings.get_setting('postgresql_database')}"
        )
        gdf = gpd.GeoDataFrame()
        try:
            sql = f"SELECT geometry, adm{adm_level}_pcode FROM admin_boundaries_pcoded.{country.lower()}_adm{adm_level}"
            gdf = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col="geometry")
        except ProgrammingError:
            logging.warning(
                f"WARNING: no administrative boundaries found for country {country} "
                f"and adm_level {adm_level}"
            )
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

    def __ibf_api_post_request(self, path, body=None, files=None):
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
            # logger.info(r.text)
            # logger.error("PIPELINE ERROR")
            raise ValueError()

    def send_to_ibf_api(
        self, flood_forecast_data: BaseDataSet, flood_extent: str = None
    ):
        """Send flood forecast data to IBF API"""
        upload_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        # event/triggers-per-leadtime - trigger per lead time
        triggers_per_lead_time = []
        for lead_time in flood_forecast_data.get_lead_times():
            is_trigger, is_alert = False, False
            for pcode in flood_forecast_data.get_pcodes():
                du = flood_forecast_data.get_data_unit(pcode, lead_time)
                if du.triggered:
                    is_trigger = True
                if du.alert_class > 0:
                    is_alert = True
            triggers_per_lead_time.append(
                {
                    "leadTime": f"{lead_time}-day",
                    "triggered": is_trigger,
                    "thresholdReached": is_alert,
                }
            )
        body = {
            "countryCodeISO3": flood_forecast_data.country,
            "triggersPerLeadTime": triggers_per_lead_time,
            "disasterType": "floods",
            "date": upload_time,
        }
        self.__ibf_api_post_request("event/triggers-per-leadtime", body=body)

        # point-data/dynamic - trigger and threshold per GloFAS station
        # TBI

        # admin-area-dynamic-data/exposure - exposure data
        for indicator in [
            "population_affected",
            "population_affected_percentage",
            "alert_threshold",
        ]:
            for lead_time in flood_forecast_data.get_lead_times():
                for adm_lvl in flood_forecast_data.adm_levels:
                    exposure_pcodes = []
                    for pcode in flood_forecast_data.get_pcodes(adm_level=adm_lvl):
                        du = flood_forecast_data.get_data_unit(pcode, lead_time)
                        exposure_pcodes.append(
                            {
                                "placeCode": pcode,
                                "amount": getattr(du, indicator),
                            }
                        )
                    body = {
                        "countryCodeISO3": flood_forecast_data.country,
                        "leadTime": f"{lead_time}-day",
                        "dynamicIndicator": indicator,
                        "adminLevel": adm_lvl,
                        "exposurePlaceCodes": exposure_pcodes,
                        "disasterType": "floods",
                        "date": upload_time,
                    }
                    self.__ibf_api_post_request(
                        "event/triggers-per-leadtime", body=body
                    )

        # admin-area-dynamic-data/raster/floods - flood extent raster
        if flood_extent is not None:
            if not os.path.exists(flood_extent):
                raise FileNotFoundError(f"Flood extent raster {flood_extent} not found")
            files = {"file": open(flood_extent, "rb")}
            self.__ibf_api_post_request(
                "admin-area-dynamic-data/raster/floods", files=files
            )

    def save_pipeline_data(
        self, data_type: str, dataset: BaseDataSet, replace_country: bool = False
    ):
        """Upload pipeline datasets to Cosmos DB"""
        if data_type not in COSMOS_DATA_TYPES:
            raise ValueError(
                f"Data type {data_type} is not supported."
                f"Supported storages are {', '.join(COSMOS_DATA_TYPES)}"
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
    ) -> BaseDataSet:
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
                        if data_type == "river-discharge":
                            data_unit = RiverDischargeDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                lead_time=record["lead_time"],
                                river_discharge_ensemble=record[
                                    "river_discharge_ensemble"
                                ],
                            )
                        elif data_type == "flood-forecast":
                            data_unit = FloodForecastDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                lead_time=record["lead_time"],
                                flood_forecasts=record["flood_forecasts"],
                                pop_affected=record["pop_affected"],
                                pop_affected_perc=record["pop_affected_perc"],
                                triggered=record["triggered"],
                                return_period=record["return_period"],
                                alert_class=record["alert_class"],
                            )
                        elif data_type == "trigger-threshold":
                            data_unit = TriggerThresholdDataUnit(
                                adm_level=record["adm_level"],
                                pcode=record["pcode"],
                                trigger_thresholds=record["trigger_thresholds"],
                            )
                        else:
                            raise ValueError(f"Invalid data type {data_type}")
                        data_units.append(data_unit)
                adm_levels = list(
                    set([data_unit.adm_level for data_unit in data_units])
                )
                dataset = BaseDataSet(
                    country=country,
                    timestamp=timestamp,
                    adm_levels=adm_levels,
                    data_units=data_units,
                )
                datasets.append(dataset)
        if len(datasets) > 1:
            raise KeyError(
                f"Multiple datasets of type '{data_type}' found for country {country} in date range "
                f"{start_date}-{end_date}; restrict the query."
            )
        elif len(datasets) == 0:
            raise KeyError(
                f"No datasets of type '{data_type}' found for country {country} in date range "
                f"{start_date}-{end_date}."
            )
        else:
            return datasets[0]

    def __get_blob_service_client(self, blob_path: str):
        blob_service_client = BlobServiceClient.from_connection_string(
            self.secrets.get_secret("blob_connection_string")
        )
        container = self.secrets.get_secret("blob_container_name")
        return blob_service_client.get_blob_client(container=container, blob=blob_path)

    def save_to_blob(self, local_path: str, file_dir_blob: str):
        """Save file to Azure Blob Storage"""
        # upload to Azure Blob Storage
        blob_client = self.__get_blob_service_client(file_dir_blob)
        with open(local_path, "rb") as upload_file:
            blob_client.upload_blob(upload_file, overwrite=True)
        logging.info("Successfully uploaded to Azure Blob Storage")

    def get_from_blob(self, local_path: str, blob_path: str):
        """Get file from Azure Blob Storage"""
        blob_client = self.__get_blob_service_client(blob_path)

        with open(local_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        logging.info("Successfully downloaded from Azure Blob Storage")
