import os.path
import click
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from scenarios import Scenario, SCENARIOS

if not os.path.exists(".env"):
    print("credentials not found, run this test from root directory")

# SCENARIOS = [
#     0 "nothing",
#     1 "trigger-on-lead-time",
#     2 "trigger-after-lead-time",
#     3 "trigger-before-lead-time",
#     4 "trigger-multiple-on-lead-time",
#     5 "alert",
#     6 "alert-multiple",
#     7 "trigger-and-alert",
#     8 "trigger-and-alert-multiple",
#     9 "trigger-multiple-and-alert-multiple",
# ]


@click.command()
@click.option("--scenario", "-s", help="scenario", default=0)
@click.option("--country", "-c", help="country", default="UGA")
def upload_scenario(scenario, country):

    pipe = Pipeline(
        country=country,
        settings=Settings("config/config.yaml"),
        secrets=Secrets(".env"),
    )

    try:
        scenario = Scenario(
            scenario=SCENARIOS[scenario], country=country, pipeline=pipe
        )
    except IndexError:
        print(f"scenario {scenario} not found")
        return

    print(f"get mock data for scenario: {scenario.scenario}")
    scenario.get_discharge_scenario(random_stations=False, stations=["G5075", "G5220"])
    # for du in discharge_dataset.data_units:
    #     if du.discharge_mean > 100.0:
    #         print(vars(du))

    print(f"forecast floods")
    pipe.forecast.compute_forecast()

    # flood_raster = "data/output/empty_old.tif"
    # flood_raster = "data/output/flood_old.tif"

    print(f"send to IBF API")
    pipe.load.send_to_ibf_api(
        forecast_data=pipe.data.forecast_admin,
        discharge_data=pipe.data.discharge_admin,
        forecast_station_data=pipe.data.forecast_station,
        discharge_station_data=pipe.data.discharge_station,
        flood_extent=pipe.forecast.flood_extent_raster,
    )


if __name__ == "__main__":
    upload_scenario()
