from floodpipeline.secrets import Secrets
from floodpipeline.data import (BaseDataSet, BaseDataUnit, RiverDischargeDataUnit, FloodForecastDataUnit,
                                TriggerThresholdDataUnit)
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timedelta
import azure.cosmos.cosmos_client as cosmos_client
import logging
import json
import os
import geopandas as gpd
from typing import List

COSMOS_DATA_TYPES = ["river-discharge", "flood-forecast", "trigger-threshold"]


class Load:
    """ Download/upload data from/to a data storage """
    
    def __init__(self, secrets: Secrets = None):
        self.secrets = None
        if secrets is not None:
            self.set_secrets(secrets)
    
    def set_secrets(self, secrets):
        """ Set secrets for storage """
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        missing_secrets = secrets.check_secrets(
            [
                "COSMOS_URL",
                "COSMOS_KEY",
                "SQL_SERVER",
                "SQL_USER",
                "SQL_PASSWORD"
            ]
        )
        if missing_secrets:
            raise Exception(f"Missing secret(s) {', '.join(missing_secrets)}")
        else:
            self.secrets = secrets
            return self
    
    @staticmethod
    def get_adm_boundaries(country: str, adm_level: int) -> gpd.GeoDataFrame:
        """ Get administrative boundaries from PostgreSQL database """
        engine = create_engine(f"postgresql://{os.getenv('SQL_USER')}:{os.getenv('SQL_PASSWORD')}"
                               f"@{os.getenv('SQL_SERVER')}.postgres.database.azure.com:5432/global510")
        gdf = gpd.GeoDataFrame()
        try:
            sql = f"SELECT geometry, adm1_pcode FROM admin_boundaries_pcoded.{country.lower()}_adm{adm_level}"
            gdf = gpd.GeoDataFrame.from_postgis(sql, engine, geom_col="geometry")
        except ProgrammingError:
            logging.warning(f"WARNING: no administrative boundaries found for country {country} "
                            f"and adm_level {adm_level}")
        return gdf
    
    def send_to_ibf(self, dataset):
        """ Send data to IBF """
        # TBI

    def save_pipeline_data(self, data_type: str, dataset: BaseDataSet):
        """ Upload pipeline datasets to Cosmos DB """
        if data_type not in COSMOS_DATA_TYPES:
            raise ValueError(f"Data type {data_type} is not supported."
                             f"Supported storages are {', '.join(COSMOS_DATA_TYPES)}")
        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {'masterKey': self.secrets.get_secret("COSMOS_KEY")},
            user_agent="sml-api",
            user_agent_overwrite=True
        )
        cosmos_db = client_.get_database_client("flood-pipeline")
        cosmos_container_client = cosmos_db.get_container_client(data_type)
        for data_unit in dataset.data_units:
            record = vars(data_unit)
            record['timestamp'] = dataset.timestamp
            record['country'] = dataset.country
            record['id'] = (f"{dataset.pcode}_{dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}_"
                            f"{data_unit.lead_time}")
            cosmos_container_client.upsert_item(body=record)

    def get_pipeline_data(self, data_type, start_date, end_date, country, adm_level, pcode, lead_time) -> List[BaseDataSet]:
        """ Download pipeline datasets from Cosmos DB """
        if data_type not in COSMOS_DATA_TYPES:
            raise ValueError(f"Data type {data_type} is not supported."
                             f"Supported storages are {', '.join(COSMOS_DATA_TYPES)}")
        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {'masterKey': self.secrets.get_secret("COSMOS_KEY")},
            user_agent="ibf-flood-pipeline",
            user_agent_overwrite=True
        )
        cosmos_db = client_.get_database_client("flood-pipeline")
        cosmos_container_client = cosmos_db.get_container_client(data_type)
        query = 'SELECT * FROM c WHERE '
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
        print(f"QUERY: {query}")
        records = cosmos_container_client.query_items(
            query=query,
            enable_cross_partition_query=True if country is None else None  # country must be the partition key
        )
        datasets = []
        countries = list(set([record['country'] for record in records]))
        timestamps = list(set([record['timestamp'] for record in records]))
        for country in countries:
            for timestamp in timestamps:
                data_units = []
                for record in records:
                    if record['country'] == country and record['timestamp'] == timestamp:
                        if data_type == "river-discharge":
                            data_unit = RiverDischargeDataUnit(
                                adm_level=record['adm_level'],
                                pcode=record['pcode'],
                                lead_time=record['lead_time'],
                                river_discharge_ensemble=record['river_discharge_ensemble']
                            )
                        elif data_type == "flood-forecast":
                            data_unit = FloodForecastDataUnit(
                                adm_level=record['adm_level'],
                                pcode=record['pcode'],
                                lead_time=record['lead_time'],
                                likelihood=record['likelihood'],
                                severity=record['severity'],
                                pop_affected=record['pop_affected'],
                                pop_affected_perc=record['pop_affected_perc']
                            )
                        elif data_type == "trigger-threshold":
                            data_unit = TriggerThresholdDataUnit(
                                adm_level=record['adm_level'],
                                pcode=record['pcode'],
                                lead_time=record['lead_time'],
                                trigger_thresholds=record['trigger_thresholds']
                            )
                        else:
                            raise ValueError(f"Invalid data type {data_type}")
                        data_units.append(data_unit)
                adm_levels = list(set([data_unit.adm_level for data_unit in data_units]))
                dataset = BaseDataSet(
                    country=country,
                    timestamp=timestamp,
                    adm_levels=adm_levels,
                    data_units=data_units
                )
                datasets.append(dataset)
        return datasets
