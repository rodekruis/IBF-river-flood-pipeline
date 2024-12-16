import os.path
import click
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.scenarios import Scenario
import datetime

default_events = [{"station-code": "G5100", "type": "trigger", "lead-time": 5}]


@click.command()
@click.option("--events", "-s", help="list of events", default=default_events)
@click.option("--country", "-c", help="country", default="UGA")
@click.option(
    "--upload_time",
    "-d",
    help="upload datetime [optional]",
    default=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
)
def run_scenario(events, country, upload_time):

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
        upload_time=upload_time,
    )

    print(f"save the logs to storage")
    run_directory = (
        datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()
    )
    blob_directory = pipe.settings.get_setting("blob_storage_path")
    for file in os.listdir("logs"):
        pipe.load.save_to_blob(
            local_path=f"logs/{file}",
            file_dir_blob=f"{blob_directory}/dev-logs/{run_directory}/logs.json",
        )
    for raster in pipe.load.rasters_sent:
        pipe.load.save_to_blob(
            local_path=raster,
            file_dir_blob=f"{blob_directory}/dev-logs/{run_directory}/{os.path.basename(raster)}",
        )


if __name__ == "__main__":
    run_scenario()
