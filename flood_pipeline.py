import logging

from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from datetime import date, datetime, timedelta
import click


@click.command()
@click.option("--country", help="country ISO3", default="UGA")
@click.option("--prepare", help="prepare discharge data", default=False, is_flag=True)
@click.option("--extract", help="extract discharge data", default=False, is_flag=True)
@click.option("--forecast", help="forecast floods", default=False, is_flag=True)
@click.option("--send", help="send to IBF", default=False, is_flag=True)
@click.option("--save", help="save to storage", default=False, is_flag=True)
@click.option(
    "--datetimestart",
    help="datetime start ISO 8601",
    default=date.today().strftime("%Y-%m-%dT%H:%M:%S"),
)
@click.option(
    "--datetimeend",
    help="datetime end ISO 8601",
    default=(date.today() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S"),
)
@click.option(
    "--debug",
    help="debug mode: process only one ensemble member from yesterday",
    default=False,
    is_flag=True,
)
def run_river_flood_pipeline(
    country, prepare, extract, forecast, send, save, datetimestart, datetimeend, debug
):
    datetimestart = datetime.strptime(datetimestart, "%Y-%m-%dT%H:%M:%S")
    datetimeend = datetime.strptime(datetimeend, "%Y-%m-%dT%H:%M:%S")
    try:
        pipe = Pipeline(
            country=country,
            settings=Settings("config/config.yaml"),
            secrets=Secrets(".env"),
        )
    except FileNotFoundError as e:
        logging.warning(f"Necessary dataset missing: {e}, skipping country {country}")
        return

    pipe.run_pipeline(
        prepare=prepare,
        extract=extract,
        forecast=forecast,
        send=send,
        save=save,
        debug=debug,
        datetimestart=datetimestart,
        datetimeend=datetimeend,
    )


if __name__ == "__main__":
    run_river_flood_pipeline()
