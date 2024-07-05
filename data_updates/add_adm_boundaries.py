import geopandas as gpd
from sqlalchemy import create_engine
import json
import os
import requests
from tqdm import tqdm
import pycountry
from dotenv import load_dotenv
load_dotenv('credentials/.env')

engine = create_engine(f"postgresql://{os.getenv('SQL_USER')}:{os.getenv('SQL_PASSWORD')}"
                       f"@{os.getenv('SQL_SERVER')}.postgres.database.azure.com:5432/global510")
engine.connect()


def add_admin_boundaries():

    country_list = list(pycountry.countries)
    country_list = [c for c in country_list if c.alpha_3.upper() in ['UGA', 'KEN', 'ETH', 'SSD', 'ZMB']]

    for lvl in [1, 2, 3, 4]:
        print(f'starting level {lvl}')
        gdf = gpd.read_file(rf"C:\Users\JMargutti\Downloads\adm{lvl}_polygons.gpkg\adm{lvl}_polygons.gpkg")
        gdf = gdf[['geometry', 'adm0_src', f'adm{lvl}_src', f'adm{lvl}_name']]
        gdf = gdf.rename(columns={f'adm{lvl}_src': f'adm{lvl}_pcode'})
        gdf = gdf.dropna(subset=[f'adm{lvl}_pcode'])
        for country in tqdm(country_list):
            gdf_country = gdf[gdf['adm0_src'] == country.alpha_3.upper()]
            gdf_country = gdf_country[['geometry', f'adm{lvl}_pcode', f'adm{lvl}_name']]
            if len(gdf_country) > 0:
                print(f'uploading {country.name} ({len(gdf_country)})')
                gdf_country.to_postgis(
                    f"{country.alpha_3.lower()}_adm{lvl}",
                    engine,
                    schema="admin_boundaries_pcoded",
                    if_exists='replace'
                )
            else:
                print(f'no data for {country.name}')


if __name__ == '__main__':
    add_admin_boundaries()