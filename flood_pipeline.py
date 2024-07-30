from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
import click


@click.command()
@click.option("--country", help="country ISO3", default="UGA")
def run_river_flood_pipeline(country):
    pipe = Pipeline(
        settings=Settings("config/config-template.yaml"), secrets=Secrets(".env")
    )
    pipe.run_pipeline(
        extract=True,
        forecast=True,
        send=False,
    )


if __name__ == "__main__":
    run_river_flood_pipeline()
