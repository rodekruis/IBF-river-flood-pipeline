from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
from datetime import datetime, timedelta
from typing import List


class Pipeline:
    """ Base class for flood data pipeline """

    def __init__(self, secrets: Secrets = None):
        self.extract = Extract(secrets=secrets)
        self.forecast = Forecast(secrets=secrets)
        self.load = Load(secrets=secrets)
        self.messages = []

    def run_pipline(self,
                    extract=True,
                    forecast=True,
                    send=True):
        if extract:
            self.glofas_data = self.extract.get_glofas_data()
        else:
            self.glofas_data = self.load.get_glofas_data()
        if forecast:
            self.forecast = self.forecast.process_glofas()
        if send:
            self.load.send_to_ibf()
