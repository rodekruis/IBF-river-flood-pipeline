from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
import click


@click.command()
@click.option("--country", help="country ISO3", default="UGA")
@click.option("--prepare", help="prepare discharge data", default=False, is_flag=True)
@click.option("--extract", help="extract discharge data", default=False, is_flag=True)
@click.option("--forecast", help="forecast floods", default=False, is_flag=True)
@click.option("--send", help="send to IBF", default=False, is_flag=True)
@click.option("--save", help="save to storage", default=False, is_flag=True)
def run_river_flood_pipeline(country, prepare, extract, forecast, send, save):
    pipe = Pipeline(
        settings=Settings("config/config-template.yaml"), secrets=Secrets(".env")
    )
    pipe.run_pipeline(
        country=country,
        prepare=prepare,
        extract=extract,
        forecast=forecast,
        send=send,
        save=save,
    )


if __name__ == "__main__":
    run_river_flood_pipeline()
