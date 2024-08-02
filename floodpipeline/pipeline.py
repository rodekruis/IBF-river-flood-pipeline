from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from datetime import datetime, date, timedelta
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
                river_discharge_dataset = self.extract.get_data(
                    country=country, source="GloFAS"
                )
                logging.info("save river discharge data to storage")
                self.load.save_pipeline_data(
                    data_type="river-discharge",
                    dataset=river_discharge_dataset,
                    replace_country=True,
                )
            else:
                logging.info("get river discharge data from storage")
                river_discharge_dataset = self.load.get_pipeline_data(
                    data_type="river-discharge",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
            if forecast:
                logging.info("get trigger thresholds from storage")
                trigger_thresholds_dataset = self.load.get_pipeline_data(
                    data_type="trigger-threshold", country=country
                )
                logging.info("forecast floods")
                flood_forecast_dataset = self.forecast.forecast(
                    river_discharges=river_discharge_dataset,
                    trigger_thresholds=trigger_thresholds_dataset,
                )
                logging.info("save flood forecasts to storage")
                self.load.save_pipeline_data(
                    data_type="flood-forecast",
                    dataset=flood_forecast_dataset,
                    replace_country=True,
                )
            else:
                logging.info("get flood forecasts from storage")
                flood_forecast_dataset = self.load.get_pipeline_data(
                    data_type="flood-forecast",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )

            if send:
                logging.info("send data to IBF API")
                self.load.send_to_ibf_api(
                    flood_forecast_data=flood_forecast_dataset,
                    river_discharge_data=river_discharge_dataset,
                    flood_extent=self.forecast.flood_extent_filepath,
                )
