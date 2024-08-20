import os.path
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings

if not os.path.exists(".env"):
    raise FileNotFoundError("credentials not found, run this test from root directory")
pipe = Pipeline(
    secrets=Secrets(".env"), settings=Settings("config/config-template.yaml")
)

pipe.extract.prepare_glofas_data()
discharge_data, discharge_station_data = pipe.extract.extract_glofas_data("UGA")
for discharge_unit in discharge_data.data_units:
    print(vars(discharge_unit))

# gdf = pipe.load.get_adm_boundaries(country="PHL", adm_level=1)


# dataset = BaseDataSet(country="UGA", datetime=datetime.today(), adm_levels=[1])
#
# data_unit = dataset.get_data_unit(pcode="river_forecast", lead_time=1)
# print(data_unit)
#
# data_unit = RiverDischargeDataUnit(pcode="river_forecast", lead_time=1, river_discharge_ensemble=[123.45])
# dataset.upsert_data_unit(data_unit)
#
# data_unit = dataset.get_data_unit(pcode="river_forecast", lead_time=1)
# print(vars(data_unit))
#
# data_unit.river_discharge_ensemble.append(234.67)
# print(vars(data_unit))
# dataset.upsert_data_unit(data_unit)
#
# data_unit = dataset.get_data_unit(pcode="river_forecast", lead_time=1)
# print(vars(data_unit))
