import os.path
import click
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.scenarios import Scenario

if not os.path.exists(".env"):
    print("credentials not found, run this test from root directory")


default_events = [
    {
        "station-code": "G5075",
        "type": "trigger",
        "lead-time": 5,
    },
    {
        "station-code": "G5189",
        "type": "medium-alert",
        "lead-time": 3,
    },
]


@click.command()
@click.option("--events", "-s", help="events", default=default_events)
@click.option("--country", "-c", help="country", default="UGA")
def run_scenario(events, country):

    pipe = Pipeline(
        country=country,
        settings=Settings("config/config.yaml"),
        secrets=Secrets(".env"),
    )

    scenario = Scenario(country=country, pipeline=pipe)
    scenario.get_discharge_scenario(events=events)

    print(f"forecast floods")
    pipe.forecast.compute_forecast()

    print(f"send to IBF API")
    pipe.load.send_to_ibf_api(
        forecast_data=pipe.data.forecast_admin,
        discharge_data=pipe.data.discharge_admin,
        forecast_station_data=pipe.data.forecast_station,
        discharge_station_data=pipe.data.discharge_station,
        flood_extent=pipe.forecast.flood_extent_raster,
    )


if __name__ == "__main__":
    run_scenario()
