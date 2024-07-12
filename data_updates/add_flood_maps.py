import requests
import os
from datetime import datetime
import zipfile
import numpy as np
import rasterio
import geopandas as gpd
from floodpipeline.forecast import clip_raster, merge_rasters
from floodpipeline.load import Load
from floodpipeline.settings import Settings
from floodpipeline.secrets import Secrets
from shapely.geometry import box
import re

RETURN_PERIODS = [10, 20, 50, 75, 100, 200, 500]
secrets = Secrets()
settings = Settings("config/config-template.yaml")
load = Load(settings=settings, secrets=secrets)


def get_global_flood_maps(rp: int) -> gpd.GeoDataFrame:
    """Get GeoDataFrame of global flood maps"""
    if rp not in [10, 20, 50, 75, 100, 200, 500]:
        raise ValueError("Return Period must be in 10, 20, 50, 75, 100, 200 or 500")
    flood_map_html = requests.get(
        f"{settings.get_setting('global_flood_maps_url')}/RP{rp}/"
    ).text
    flood_map_files = re.findall(r"ID(.*?).tif", flood_map_html)
    flood_map_files = list(set([f"ID{file}.tif" for file in flood_map_files]))
    gdf_flood_map = gpd.GeoDataFrame(columns=["filename", "geometry"], crs="EPSG:4326")
    for file in flood_map_files:
        if "N" in file:
            max_lat = int(re.search(r"N(.*?)_", file)[0][1:-1])
        else:
            max_lat = -int(re.search(r"S(.*?)_", file)[0][1:-1])
        min_lat = max_lat - 10
        if "E" in file:
            min_lon = int(re.search(r"E(.*?)_", file)[0][1:-1])
        else:
            min_lon = -int(re.search(r"W(.*?)_", file)[0][1:-1])
        max_lon = min_lon + 10
        geom = box(min_lon, min_lat, max_lon, max_lat)
        gdf_flood_map.loc[len(gdf_flood_map)] = {"filename": file, "geometry": geom}
    gdf_flood_map = gdf_flood_map.set_crs("EPSG:4326")
    return gdf_flood_map


def add_flood_maps():

    os.makedirs("data/updates", exist_ok=True)
    load = Load(settings=settings, secrets=secrets)

    for country_settings in settings.get_setting("countries"):

        country = list(country_settings.keys())[0]
        print("Adding flood maps for", country)

        for rp in RETURN_PERIODS:
            gdf_flood_map = get_global_flood_maps(rp=int(rp))
            country_gdf = load.get_adm_boundaries(country=country, adm_level=1)
            country_gdf = country_gdf.to_crs("EPSG:4326")

            # filter global flood maps based on country boundary
            gdf_flood_map = gpd.clip(
                gdf_flood_map, country_gdf.total_bounds, keep_geom_type=True
            )

            # download and clip necessary flood maps
            flood_map_files = gdf_flood_map["filename"].tolist()
            flood_map_filepaths = []
            for flood_map_file in flood_map_files:
                # download
                flood_map_filepath = f"data/updates/{flood_map_file}"
                url = f"{settings.get_setting('global_flood_maps_url')}/RP{rp}/{flood_map_file}"
                if not os.path.exists(flood_map_filepath):
                    r = requests.get(url)
                    with open(flood_map_filepath, "wb") as file:
                        file.write(r.content)
                # clip
                flood_map_clipped_filepath = f"data/updates/{flood_map_file}".replace(
                    ".tif", "_clipped.tif"
                )
                if not os.path.exists(flood_map_clipped_filepath):
                    clip, out_meta = clip_raster(
                        flood_map_filepath, [country_gdf.geometry.union_all()]
                    )
                    with rasterio.open(
                        flood_map_clipped_filepath, "w", **out_meta
                    ) as dest:
                        dest.write(clip)
                flood_map_filepaths.append(flood_map_clipped_filepath)

            # merge flood maps
            merged_raster_filepath = f"data/updates/flood_map_RP{rp}.tif"
            mosaic, out_meta = merge_rasters(flood_map_filepaths)
            mosaic = np.nan_to_num(mosaic)
            out_meta.update(dtype=rasterio.float32, count=1, compress="lzw")  # compress
            with rasterio.open(merged_raster_filepath, "w", **out_meta) as dest:
                dest.write(mosaic.astype(rasterio.float32))

            # save to blob storage
            load.save_to_blob(
                local_path=merged_raster_filepath,
                file_dir_blob=f"flood/pipeline-input/flood-maps/{country}/flood_map_{country}_RP{int(rp)}.tif",
            )


if __name__ == "__main__":
    add_flood_maps()
