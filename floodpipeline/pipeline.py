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
        prepare: bool = True,
        extract: bool = True,
        forecast: bool = True,
        send: bool = True,
        save: bool = False,
    ):
        """Run the flood data pipeline"""

        countries = [c["name"] for c in self.settings.get_setting("countries")]
        if country is not None:
            if country != "ALL":
                if country not in countries:
                    raise ValueError(f"Country {country} not found in settings")
                countries = [country]
                

        if prepare:
            logging.info("prepare discharge data for all countries")
            self.extract.prepare_glofas_data(countries=countries)

        if not extract and not forecast:
            return
        for country in countries:
            if extract:
                logging.info(f"start {country}")
                logging.info(f"extract discharge data")
                discharge_dataset, discharge_station_dataset = (
                    self.extract.extract_glofas_data(country=country)
                )
                if save:
                    logging.info("save discharge data to storage")
                    self.load.save_pipeline_data(
                        data_type="discharge", dataset=discharge_dataset
                    )
                    self.load.save_pipeline_data(
                        data_type="discharge-station", dataset=discharge_station_dataset
                    )
            else:
                logging.info(f"get discharge data from storage")
                discharge_dataset = self.load.get_pipeline_data(
                    data_type="discharge",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
                discharge_station_dataset = self.load.get_pipeline_data(
                    data_type="discharge-station",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
            if forecast:
                logging.info("get thresholds from storage")
                thresholds_dataset = self.load.get_pipeline_data(
                    data_type="threshold", country=country
                )
                thresholds_station_dataset = self.load.get_pipeline_data(
                    data_type="threshold-station", country=country
                )
                logging.info("forecast floods")
                forecast_dataset = self.forecast.forecast(
                    discharge_dataset=discharge_dataset,
                    threshold_dataset=thresholds_dataset,
                )
                forecast_station_dataset = self.forecast.forecast_station(
                    discharge_dataset=discharge_station_dataset,
                    threshold_dataset=thresholds_station_dataset,
                )
                if save:
                    logging.info("save flood forecasts to storage")
                    self.load.save_pipeline_data(
                        data_type="forecast", dataset=forecast_dataset
                    )
                    self.load.save_pipeline_data(
                        data_type="forecast-station", dataset=forecast_station_dataset
                    )
            else:
                logging.info("get flood forecasts from storage")
                forecast_dataset = self.load.get_pipeline_data(
                    data_type="forecast",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
                forecast_station_dataset = self.load.get_pipeline_data(
                    data_type="forecast",
                    country=country,
                    start_date=date.today(),
                    end_date=date.today() + timedelta(days=1),
                )
            if send:
                logging.info("send data to IBF API")
                self.load.send_to_ibf_api(
                    forecast_data=forecast_dataset,
                    discharge_data=discharge_dataset,
                    forecast_station_data=forecast_station_dataset,
                    discharge_station_data=discharge_station_dataset,
                    flood_extent=self.forecast.flood_extent_filepath,
                )
