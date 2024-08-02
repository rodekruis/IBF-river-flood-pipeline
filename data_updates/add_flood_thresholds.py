import requests
import os
import urllib.request
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import rasterio
from rasterstats import zonal_stats
from floodpipeline.load import Load
from floodpipeline.settings import Settings
from floodpipeline.secrets import Secrets
from floodpipeline.data import TriggerThreshold, TriggerThresholdDataUnit, BaseDataSet
import logging

RETURN_PERIODS = [
    1.5,
    2.0,
    5.0,
    10.0,
    20.0,
    50.0,
    100.0,
    200.0,
    500.0,
]
secrets = Secrets()
settings = Settings("config/config-template.yaml")
load = Load(settings=settings, secrets=secrets)


def add_flood_thresholds():
    os.makedirs("data/updates", exist_ok=True)
    upload_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Download flood thresholds
    html_page = BeautifulSoup(
        requests.get("https://confluence.ecmwf.int/display/CEMS/Auxiliary+Data").text,
        features="lxml",
    )
    flood_thresholds_files = {}
    for rp in RETURN_PERIODS:
        filename = f"flood_threshold_glofas_v4_rl_{'{:.1f}'.format(rp)}.nc"
        filepath = os.path.join("data", "updates", filename)
        if not os.path.exists(filepath):
            print(f"Downloading {filename}")
            urls = html_page.find_all("a", href=True)
            url = f'https://confluence.ecmwf.int{next(url["href"] for url in urls if filename in url["href"])}'
            urllib.request.urlretrieve(url, filepath)
        flood_thresholds_files[rp] = filepath

    # loop over countries
    for country_settings in settings.get_setting("countries"):
        country = country_settings["name"]
        if country != "UGA":
            continue
        ttdus = []

        # calculate thresholds per admin division
        for adm_level in country_settings["admin-levels"]:
            print(f"Calculating thresholds for {country}, admin level {adm_level}")
            country_gdf = load.get_adm_boundaries(
                country=country, adm_level=int(adm_level)
            )
            for rp, filename in flood_thresholds_files.items():
                with rasterio.open(filename) as src:
                    raster_array = src.read(1)
                    transform = src.transform
                # Perform zonal statistics
                stats = zonal_stats(
                    country_gdf,
                    raster_array,
                    affine=transform,
                    stats=["max", "median"],
                    all_touched=True,
                    nodata=0.0,
                )
                df = pd.DataFrame(stats).rename(
                    columns={"max": f"max_{rp}", "median": f"median_{rp}"}
                )
                country_gdf = pd.concat([country_gdf, df], axis=1)
            for ix, row in country_gdf.iterrows():
                ttdu = TriggerThresholdDataUnit(
                    adm_level=int(adm_level),
                    pcode=row[f"adm{adm_level}_pcode"],
                    trigger_thresholds=[
                        TriggerThreshold(
                            return_period=float(rp), threshold=row[f"max_{rp}"]
                        )
                        for rp in RETURN_PERIODS
                    ],
                )
                ttdus.append(ttdu)
        # save admin division thresholds to cosmos
        trigger_threshold_data = BaseDataSet(
            country=country,
            timestamp=datetime.today(),
            adm_levels=country_settings["admin-levels"],
            data_units=ttdus,
        )
        load.save_pipeline_data(
            "trigger-threshold", trigger_threshold_data, replace_country=True
        )

        # START: TO BE DEPRECATED
        # calculate thresholds per GloFAS station
        adm_level = country_settings["trigger-on-admin-level"]
        lead_time = country_settings["trigger-on-lead-time"]
        return_period = country_settings["trigger-on-return-period"]
        # get mapping of GloFAS stations to admin divisions
        adm_gdf = load.glofas_stations_to_adm_divisions(
            country=country, adm_level=adm_level
        )
        # prepare payload with station data for point-data/dynamic
        station_forecasts = {
            "triggerLevel": [],
        }
        for ix, adm_div in adm_gdf.iterrows():
            pcode = adm_div[f"adm{adm_level}_pcode"]
            trigger_threshold_data_unit = trigger_threshold_data.get_data_unit(pcode)
            station = adm_div["station"]
            station_forecasts["triggerLevel"].append(
                {
                    "fid": station,
                    "value": int(
                        trigger_threshold_data_unit.get_threshold(return_period)[
                            "threshold"
                        ]
                        or 0
                    ),
                }
            )

        # get exact thresholds for GloFAS stations based on its coordinate
        # station_forecasts = {
        #     "triggerLevel": [],
        # }
        # with rasterio.open(flood_thresholds_files[return_period]) as src:
        #     for ix, station in adm_gdf.iterrows():
        #         thresholds = []
        #         for shiftx in [-0.01, 0.01]:
        #             for shifty in [-0.01, 0.01]:
        #                 coords = [
        #                     (station["point"].x + shiftx, station["point"].y + shifty)
        #                 ]
        #                 threshold = float(
        #                     [x[0] for x in src.sample(coords, indexes=1)][0]
        #                 )
        #                 thresholds.append(threshold)
        #         station_forecasts["triggerLevel"].append(
        #             {
        #                 "fid": station["station"],
        #                 "value": max(thresholds),
        #             }
        #         )
        # print(station_forecasts)

        # save station thresholds to IBF API
        for indicator in station_forecasts.keys():
            body = {
                "leadTime": f"{lead_time}-day",
                "key": indicator,
                "dynamicPointData": station_forecasts[indicator],
                "pointDataCategory": "glofas_stations",
                "disasterType": "floods",
                "date": upload_time,
            }
            load.ibf_api_post_request("point-data/dynamic", body=body)
        # END: TO BE DEPRECATED


if __name__ == "__main__":
    add_flood_thresholds()
