from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings


class Pipeline:
    """Base class for flood data pipeline"""

    def __init__(self, settings: Settings, secrets: Secrets):
        self.extract = Extract(settings=settings, secrets=secrets)
        self.forecast = Forecast(settings=settings, secrets=secrets)
        self.load = Load(settings=settings, secrets=secrets)
        self.river_discharge_dataset = None
        self.flood_forecast_dataset = None
        self.trigger_thresholds_dataset = None

    def run_pipeline(
        self,
        extract: bool = True,
        forecast: bool = True,
        upload: bool = True,
    ):
        """Run the flood data pipeline"""
        if extract:
            # get GloFAS data
            self.extract.set_source("GloFAS")
            self.river_discharge_dataset = self.extract.get_data()
            # save river discharge data to storage
            self.load.save_pipeline_data(
                "river-discharge", self.river_discharge_dataset
            )
        else:
            # get river discharge data from storage
            self.river_discharge_dataset = self.load.get_pipeline_data(
                "river-discharge"
            )
        if forecast:
            # get trigger thresholds
            self.trigger_thresholds_dataset = self.load.get_pipeline_data(
                "trigger-threshold"
            )
            # forecast floods
            self.flood_forecast_dataset = self.forecast.forecast(
                river_discharges=self.river_discharge_dataset,
                trigger_thresholds=self.trigger_thresholds_dataset,
            )
            # save flood forecasts to storage
            self.load.save_pipeline_data("flood-forecast", self.flood_forecast_dataset)
        if upload:
            # upload to IBF
            self.load.send_to_ibf()
