from floodpipeline.extract import Extract
from floodpipeline.forecast import Forecast
from floodpipeline.load import Load
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import PipelineDataSets
from floodpipeline.logger import logger


class Pipeline:
    """Base class for flood data pipeline"""

    def __init__(self, settings: Settings, secrets: Secrets, country: str):
        self.settings = settings
        if country not in [c["name"] for c in self.settings.get_setting("countries")]:
            raise ValueError(f"No config found for country {country}")
        self.country = country
        self.load = Load(settings=settings, secrets=secrets)
        self.data = PipelineDataSets(country=country, settings=settings)
        self.data.threshold_admin = self.load.get_pipeline_data(
            data_type="threshold", country=self.country
        )
        self.data.threshold_station = self.load.get_pipeline_data(
            data_type="threshold-station", country=self.country
        )
        self.extract = Extract(
            settings=settings,
            secrets=secrets,
            data=self.data,
        )
        self.forecast = Forecast(
            settings=settings,
            secrets=secrets,
            data=self.data,
        )

    def run_pipeline(
        self,
        prepare: bool = True,
        forecast: bool = True,
        send: bool = True,
        debug: bool = False,  # fast extraction on yesterday's data, using only one ensemble member
    ):
        """Run the flood data pipeline"""

        if prepare:
            logger.info("prepare discharge data")
            self.extract.prepare_glofas_data(country=self.country, debug=debug)

        if forecast:
            logger.info(f"extract discharge data")
            self.extract.extract_glofas_data(country=self.country, debug=debug)
            logger.info("forecast floods")
            self.forecast.compute_forecast()

        if send:
            logger.info("send data to IBF API")
            self.load.send_to_ibf_api(
                forecast_data=self.data.forecast_admin,
                discharge_data=self.data.discharge_admin,
                forecast_station_data=self.data.forecast_station,
                discharge_station_data=self.data.discharge_station,
                flood_extent=self.forecast.flood_extent_raster,
            )
