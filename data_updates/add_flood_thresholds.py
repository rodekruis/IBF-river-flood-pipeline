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
from floodpipeline.data import (
    Threshold,
    ThresholdDataUnit,
    ThresholdStationDataUnit,
    StationDataSet,
    AdminDataSet,
)
from shapely import Point
from shapely.geometry import box
import geopandas as gpd
import click

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
settings = Settings("config/config.yaml")
load = Load(settings=settings, secrets=secrets)


@click.command()
@click.option("--country", "-c", help="country ISO3", default="all")
def add_flood_thresholds(country):
    os.makedirs("data/updates", exist_ok=True)

    if country != "all" and country not in [
        c["name"] for c in settings.get_setting("countries")
    ]:
        raise ValueError(f"No config found for country {country}")

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
        if country != "all" and country != country_settings["name"]:
            continue
        country_name = country_settings["name"]
        ttdus = []

        # calculate thresholds per admin division
        for adm_level in country_settings["admin-levels"]:
            print(f"Calculating thresholds for {country_name}, admin level {adm_level}")
            country_gdf = load.get_adm_boundaries(
                country=country_name, adm_level=int(adm_level)
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
                ttdu = ThresholdDataUnit(
                    adm_level=int(adm_level),
                    pcode=row[f"adm{adm_level}_pcode"],
                    thresholds=[
                        Threshold(
                            return_period=float(rp), threshold_value=row[f"max_{rp}"]
                        )
                        for rp in RETURN_PERIODS
                    ],
                )
                ttdus.append(ttdu)
        # save admin division thresholds to cosmos
        threshold_data = AdminDataSet(
            country=country_name,
            timestamp=datetime.today(),
            adm_levels=country_settings["admin-levels"],
            data_units=ttdus,
        )
        load.save_pipeline_data("threshold", threshold_data, replace_country=True)

        print(f"Calculating station thresholds for {country_name}")
        # extract thresholds for GloFAS stations based on their coordinates
        stations = load.ibf_api_get_request(
            f"glofas-stations/{country_name}",
        )
        threshold_stations = {}
        for rp, filename in flood_thresholds_files.items():
            with rasterio.open(filename) as src:
                for station in stations:
                    discharges = []
                    for shiftx in [-0.01, 0.01]:
                        for shifty in [-0.01, 0.01]:
                            coords = [
                                (
                                    float(station["lon"]) + shiftx,
                                    float(station["lat"]) + shifty,
                                )
                            ]
                            discharge = float(
                                [x[0] for x in src.sample(coords, indexes=1)][0]
                            )
                            discharges.append(discharge)
                    if station["stationCode"] not in threshold_stations.keys():
                        threshold_stations[station["stationCode"]] = []
                    threshold_stations[station["stationCode"]].append(
                        Threshold(
                            return_period=float(rp), threshold_value=max(discharges)
                        )
                    )

        print(
            f"Determining pcodes associated to each station for country {country_name}"
        )
        pcodes_stations = {}
        river_filepath = f"data/updates/rivers.gpkg"

        # get geodataframe of rivers
        if not os.path.exists(river_filepath):
            load.get_from_blob(
                river_filepath,
                f"{settings.get_setting('blob_storage_path')}/rivers/rivers.gpkg",
            )
        gdf_rivers = gpd.read_file(river_filepath)
        gdf_rivers = gdf_rivers.clip(
            box(*country_gdf.total_bounds)
        )  # clip rivers to country

        # dissolve rivers to avoid overlapping geometries
        gdf_rivers = gpd.GeoDataFrame(geometry=gdf_rivers.geometry.buffer(0.0001))
        res_union = gdf_rivers.overlay(gdf_rivers, how="union")
        gdf_rivers = res_union.dissolve().explode().reset_index(drop=True)
        gdf_rivers.to_file(
            f"data/updates/{country_name}_rivers_dissolved.gpkg", driver="GPKG"
        )

        # create a geodataframe with stations
        gdf_dict = {"stationCode": [], "geometry": []}
        for station in stations:
            gdf_dict["stationCode"].append(station["stationCode"])
            gdf_dict["geometry"].append(Point(station["lon"], station["lat"]))
        gdf_stations = gpd.GeoDataFrame(gdf_dict, crs="EPSG:4326")
        gdf_stations.to_file(
            f"data/updates/{country_name}_stations.gpkg", driver="GPKG"
        )

        # switch stations and rivers to projected CRS
        projected_crs = gdf_stations.estimate_utm_crs()
        gdf_stations = gdf_stations.to_crs(projected_crs)
        gdf_rivers = gdf_rivers.to_crs(projected_crs)

        # create a geodataframe with stations and river(s) which pass by them
        gdf_station_nearest_river = gpd.sjoin_nearest(gdf_stations, gdf_rivers).merge(
            gdf_rivers, left_on="index_right", right_index=True
        )
        gdf_station_nearest_river["geometry"] = gdf_station_nearest_river["geometry_y"]
        gdf_station_nearest_river = gdf_station_nearest_river.dissolve("stationCode")
        station_river = gpd.GeoDataFrame(geometry=gdf_station_nearest_river["geometry"])

        # for each adm level and station, get pcodes of adm divisions intersecting the river(s) passing by the station
        top_adm_level = country_settings["admin-levels"][0]
        for adm_level in country_settings["admin-levels"]:
            adm_gdf = load.get_adm_boundaries(country=country_name, adm_level=adm_level)
            adm_gdf = adm_gdf.to_crs(projected_crs)

            for ix, station_river_record in station_river.iterrows():
                if adm_level == top_adm_level:
                    adm_gdf_station = adm_gdf[
                        station_river_record["geometry"].intersects(adm_gdf.geometry)
                    ]
                else:
                    adm_gdf_station = adm_gdf[
                        adm_gdf[f"adm{adm_level-1}_pcode"].isin(
                            pcodes_stations[ix][adm_level - 1]
                        )
                    ]
                pcodes = list(set(adm_gdf_station[f"adm{adm_level}_pcode"].to_list()))
                if ix not in pcodes_stations.keys():
                    pcodes_stations[ix] = {adm_level: pcodes}
                else:
                    pcodes_stations[ix][adm_level] = pcodes

                print(f"Station {ix} intersects with {pcodes} at adm level {adm_level}")

                adm_gdf_station.to_file(
                    f"data/updates/{country_name}_{ix}_{adm_level}.gpkg",
                    driver="GPKG",
                )

        # save thresholds
        threshold_station_data = StationDataSet(
            country=country_name,
            timestamp=datetime.today(),
        )
        for station in stations:
            for lead_time in range(1, 8):
                threshold_station_data.upsert_data_unit(
                    ThresholdStationDataUnit(
                        station_code=station["stationCode"],
                        station_name=station["stationName"],
                        lat=station["lat"],
                        lon=station["lon"],
                        pcodes=pcodes_stations[station["stationCode"]],
                        thresholds=threshold_stations[station["stationCode"]],
                    )
                )
        load.save_pipeline_data(
            "threshold-station", threshold_station_data, replace_country=True
        )


if __name__ == "__main__":
    add_flood_thresholds()
