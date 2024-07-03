from floodpipeline.secrets import Secrets
from floodpipeline.data import BaseDataSet, BaseDataUnit, RiverDischargeDataUnit, FloodForecastDataUnit
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import azure.cosmos.cosmos_client as cosmos_client
import logging
import json
import os
import pandas as pd
from typing import List

supported_storages = ["local", "Azure Cosmos DB"]


class Load:
    """ Download/upload data from/to a data storage """
    
    def __init__(self, secrets: Secrets = None):
        self.storage = "local"
        self.secrets = None
        if secrets is not None:
            self.set_secrets(secrets)
    
    def set_secrets(self, secrets):
        """ Set secrets for storage """
        if not isinstance(secrets, Secrets):
            raise TypeError(f"invalid format of secrets, use secrets.Secrets")
        missing_secrets = []
        if self.storage == "Azure Cosmos DB":
            missing_secrets = secrets.check_secrets(
                [
                    "COSMOS_URL",
                    "COSMOS_KEY",
                    "COSMOS_DATABASE",
                    "COSMOS_CONTAINER"
                ]
            )
        if missing_secrets:
            raise Exception(f"Missing secret(s) {', '.join(missing_secrets)} for storage {self.storage}")
        else:
            self.secrets = secrets
            return self
    
    def set_storage(self, storage_name, secrets: Secrets = None):
        """ Set storage to save/load data """
        if storage_name is not None:
            if storage_name not in supported_storages:
                raise ValueError(f"Storage {storage_name} is not supported."
                                 f"Supported storages are {', '.join(supported_storages)}")
            
            if hasattr(self, "storage") and self.storage == storage_name:
                logging.info(f"Storage already set to {storage_name}")
                return
            self.storage = storage_name
        else:
            raise ValueError(f"Storage not specified; provide one of {', '.join(supported_storages)}")
        if secrets is not None:
            self.set_secrets(secrets)
        elif self.secrets is not None:
            self.set_secrets(self.secrets)
        return self
    
    def get_data(
            self,
            data_type: str,
            start_date: datetime = None,
            end_date: datetime = None,
            country: str = None,
            adm_level: int = None,
            pcode: str = None,
            lead_time: int = None
    ) -> BaseDataSet:
        """ Download flood data from storage """
        if self.storage is None:
            raise RuntimeError("Storage not specified, use set_storage()")
        flood_dataset = None
        
        if self.storage == "local":
            pass
            # TBI
        
        elif self.storage == "Azure Cosmos DB":
            try:
                flood_dataset = self._get_from_cosmos(data_type, start_date, end_date, country, adm_level, pcode, lead_time)
            except Exception as e:
                logging.error(f"Failed downloading from Azure Cosmos DB: {e}")
                
        return flood_dataset
    
    def save_data(self, flood_dataset):
        """ Upload flood data to storage """
        if self.storage is None:
            raise RuntimeError("Storage not specified, use set_storage()")
        
        if self.storage == "local":
            pass
            # TBI save locally
        
        elif self.storage == "Azure Cosmos DB":
            try:
                self._save_to_cosmos(flood_dataset)
            except Exception as e:
                logging.error(f"Failed uploading to Azure Cosmos DB: {e}")

    def _save_to_cosmos(self, flood_dataset: BaseDataSet):
        """ Upload flood data to Cosmos DB """
        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {'masterKey': self.secrets.get_secret("COSMOS_KEY")},
            user_agent="sml-api",
            user_agent_overwrite=True
        )
        cosmos_db = client_.get_database_client(self.secrets.get_secret("COSMOS_DATABASE"))
        cosmos_container_client = cosmos_db.get_container_client(self.secrets.get_secret("COSMOS_CONTAINER"))
        for flood_data_unit in flood_dataset.flood_data_units:
            record = vars(flood_data_unit)
            record['timestamp'] = flood_dataset.timestamp
            record['country'] = flood_dataset.country
            record['id'] = (f"{flood_dataset.pcode}_{flood_dataset.timestamp.strftime('%Y-%m-%dT%H:%M:%S')}_"
                            f"{flood_data_unit.lead_time}")
            cosmos_container_client.upsert_item(body=record)

    def _get_from_cosmos(self, data_type, start_date, end_date, country, adm_level, pcode, lead_time) -> List[BaseDataSet]:
        """ Download flood data from Cosmos DB """
        client_ = cosmos_client.CosmosClient(
            self.secrets.get_secret("COSMOS_URL"),
            {'masterKey': self.secrets.get_secret("COSMOS_KEY")},
            user_agent="sml-api",
            user_agent_overwrite=True
        )
        cosmos_db = client_.get_database_client(self.secrets.get_secret("COSMOS_DATABASE"))
        cosmos_container_client = cosmos_db.get_container_client(self.secrets.get_secret("COSMOS_CONTAINER"))
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
        flood_datasets = []
        countries = list(set([record['country'] for record in records]))
        timestamps = list(set([record['timestamp'] for record in records]))
        for country in countries:
            for timestamp in timestamps:
                flood_data_units = []
                for record in records:
                    if record['country'] == country and record['timestamp'] == timestamp:
                        if data_type == "river_discharge":
                            flood_data_unit = RiverDischargeDataUnit(
                                adm_level=record['adm_level'],
                                pcode=record['pcode'],
                                lead_time=record['lead_time'],
                                river_discharge_ensemble=record['river_discharge_ensemble']
                            )
                        elif data_type == "flood_forecast":
                            flood_data_unit = FloodForecastDataUnit(
                                adm_level=record['adm_level'],
                                pcode=record['pcode'],
                                lead_time=record['lead_time'],
                                likelihood=record['likelihood'],
                                severity=record['severity'],
                                pop_affected=record['pop_affected'],
                                pop_affected_perc=record['pop_affected_perc']
                            )
                        else:
                            raise ValueError(f"Invalid data type {data_type}")
                        flood_data_units.append(flood_data_unit)
                adm_levels = list(set([flood_data_unit.adm_level for flood_data_unit in flood_data_units]))
                flood_dataset = BaseDataSet(
                    country=country,
                    timestamp=timestamp,
                    adm_levels=adm_levels,
                    flood_data_units=flood_data_units
                )
                flood_datasets.append(flood_dataset)
        return flood_datasets
