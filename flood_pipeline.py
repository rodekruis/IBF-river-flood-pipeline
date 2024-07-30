from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
import click


@click.command()
@click.option("--country", help="country ISO3", default="UGA")
@click.option("--extract", help="extract river data", default=False, is_flag=True)
@click.option("--forecast", help="forecast floods", default=False, is_flag=True)
@click.option("--send", help="send to IBF", default=False, is_flag=True)
def run_river_flood_pipeline(country, extract, forecast, send):
    pipe = Pipeline(
        settings=Settings("config/config-template.yaml"), secrets=Secrets(".env")
    )
    pipe.run_pipeline(
        extract=extract,
        forecast=forecast,
        send=send,
    )


if __name__ == "__main__":
    run_river_flood_pipeline()
