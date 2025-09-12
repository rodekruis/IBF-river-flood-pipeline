import geopandas as gpd
from sqlalchemy import create_engine
import json
import os
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from countries import get_countries

# load_dotenv()
#
# engine = create_engine(
#     f"postgresql://{os.getenv('SQL_USER')}:{os.getenv('SQL_PASSWORD')}"
#     f"@510-postgresql-flex-server.postgres.database.azure.com:5432/global510"
# )
# engine.connect()


def add_admin_boundaries():

    country_list = get_countries()

    for lvl in [1, 2, 3]:
        print(f"starting level {lvl}")
        gdf = gpd.read_file(
            rf"C:\Users\JMargutti\OneDrive - Rode Kruis\Rode Kruis\admin-boundaries-global\adm{lvl}_polygons\adm{lvl}_polygons.gpkg"
        )
        gdf = gdf[["geometry", "adm0_src", f"adm{lvl}_src", f"adm{lvl}_name"]]
        gdf = gdf.rename(
            columns={
                f"adm{lvl}_src": f"ADM{lvl}_PCODE",
                f"adm{lvl}_name": f"ADM{lvl}_EN",
            }
        )
        gdf = gdf.dropna(subset=[f"ADM{lvl}_PCODE"])
        gdf["adm0_src"] = gdf["adm0_src"].str.replace("ZAF_1", "ZAF")
        for country in tqdm(country_list):
            gdf_country = gdf[gdf["adm0_src"] == country.upper()]
            gdf_country = gdf_country[["geometry", f"ADM{lvl}_EN", f"ADM{lvl}_PCODE"]]
            if len(gdf_country) > 0:
                # print(f"uploading {country} ({len(gdf_country)})")
                # gdf_country.to_postgis(
                #     f"{country.lower()}_adm{lvl}",
                #     engine,
                #     schema="admin_boundaries_pcoded",
                #     if_exists="replace",
                # )
                gdf_country.geometry = gdf_country.geometry.simplify(
                    tolerance=0.01, preserve_topology=True
                )
                gdf_country.to_file(
                    f"adm_bnd/{country}_adm{lvl}.json", driver="GeoJSON"
                )
            else:
                print(f"no data for {country}")


if __name__ == "__main__":
    add_admin_boundaries()
