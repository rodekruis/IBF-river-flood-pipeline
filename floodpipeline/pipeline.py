from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from datetime import date, timedelta
import logging

logger = logging.getLogger()
logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)


class Pipeline:
    """Base class for flood data pipeline"""

    def __init__(self, settings: Settings, secrets: Secrets):
        self.settings = settings
        self.extract = Extract(settings=settings, secrets=secrets)
        self.forecast = Forecast(settings=settings, secrets=secrets)
        self.load = Load(settings=settings, secrets=secrets)
        self.river_discharge_dataset = None
        self.flood_forecast_dataset = None
        self.trigger_thresholds_dataset = None

    def run_pipeline(
        self,
        country: str = None,
        extract: bool = True,
        forecast: bool = True,
        send: bool = True,
    ):
        countries = [c["name"] for c in self.settings.get_setting("countries")]
        if country is not None:
            if country not in countries:
                raise ValueError(f"Country {country} not found in settings")
            countries = [country]
        """Run the flood data pipeline per country"""
        for country in countries:
            if extract:
                logging.info("get river discharge data")
                self.river_discharge_dataset = self.extract.get_data(
                    country=country, source="GloFAS"
                )
                logging.info("save river discharge data to storage")
                self.load.save_pipeline_data(
                    data_type="river-discharge", dataset=self.river_discharge_dataset
                )
            else:
                logging.info("get river discharge data from storage")
                self.river_discharge_dataset = self.load.get_pipeline_data(
                    data_type="river-discharge",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
            if forecast:
                logging.info("get trigger thresholds from storage")
                self.trigger_thresholds_dataset = self.load.get_pipeline_data(
                    data_type="trigger-threshold", country=country
                )
                logging.info("forecast floods")
                self.flood_forecast_dataset = self.forecast.forecast(
                    river_discharges=self.river_discharge_dataset,
                    trigger_thresholds=self.trigger_thresholds_dataset,
                )
                logging.info("save flood forecasts to storage")
                self.load.save_pipeline_data(
                    data_type="flood-forecast", dataset=self.flood_forecast_dataset
                )
            if send:
                logging.info("send data to IBF API")
                self.load.send_to_ibf()
