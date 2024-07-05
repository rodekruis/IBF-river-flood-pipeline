from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
import os
import yaml


class Pipeline:
    """ Base class for flood data pipeline """

    def __init__(self, secrets: Secrets = None):
        self.extract = Extract(secrets=secrets)
        self.forecast = Forecast(secrets=secrets)
        self.load = Load(secrets=secrets)
        self.river_discharge_dataset = None
        self.flood_forecast_dataset = None
        self.trigger_thresholds_dataset = None

    def run_pipline(
            self,
            settings: dict = None,
            extract: bool = True,
            forecast: bool = True,
            upload: bool = True,
    ):
        if extract:
            self.river_discharge_dataset = self.extract.get_data()
        else:
            self.river_discharge_dataset = self.load.get_data()
        if forecast:
            self.trigger_thresholds_dataset = self.load.get_data()
            self.flood_forecast_dataset = self.forecast.forecast(
                river_discharges=self.river_discharge_dataset,
                trigger_thresholds=self.trigger_thresholds_dataset
            )
        if upload:
            self.load.send_to_ibf()
