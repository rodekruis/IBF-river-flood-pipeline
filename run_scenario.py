import os.path
import click
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.scenarios import Scenario
from datetime import datetime, timezone

default_events = [{"station-code": "G5100", "type": "trigger", "lead-time": 5}]


@click.command()
@click.option("--events", "-s", help="list of events", default=default_events)
@click.option("--country", "-c", help="country", default="UGA")
@click.option(
    "--upload_time",
    "-d",
    help="upload datetime [optional]",
    default=datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
)
def run_scenario(events, country, upload_time):

    settings = Settings("config/config.yaml")

    pipe = Pipeline(
        country=country,
        settings=settings,
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

    upload_time_format = settings.get_setting("upload_time_format")
    upload_time_date = datetime.strptime(upload_time, upload_time_format)
    upload_time_file_name_format = settings.get_setting("upload_time_file_name_format")
    upload_time_file_name = upload_time_date.strftime(upload_time_file_name_format)
    blob_path = f"scenario-{upload_time_file_name}-{pipe.country}-{pipe.hazard}"
    pipe.load.send_to_blob_storage(blob_path)

    print(f"save the logs to storage")
    run_directory = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    blob_directory = pipe.settings.get_setting("blob_storage_path")
    for file in os.listdir("logs"):
        pipe.load.save_to_blob(
            local_path=f"logs/{file}",
            blob_path=f"{blob_directory}/dev-logs/{run_directory}/logs.json",
        )
    for raster in pipe.load.rasters_sent:
        pipe.load.save_to_blob(
            local_path=raster,
            blob_path=f"{blob_directory}/dev-logs/{run_directory}/{os.path.basename(raster)}",
        )


if __name__ == "__main__":
    run_scenario()
