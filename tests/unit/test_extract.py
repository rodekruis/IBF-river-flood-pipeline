import os.path
from datetime import datetime, timedelta
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.data import BaseDataSet, RiverDischargeDataUnit

if not os.path.exists("credentials/.env"):
    print('credentials not found, run this test from root directory')
pipe = Pipeline(secrets=Secrets("credentials/.env"))

# flood_data = pipe.extract.get_data(source="GloFAS", country="UGA", adm_levels=[1])
# for data_unit in flood_data.data_units:
#     print(vars(data_unit))

gdf = pipe.load.get_adm_boundaries(country="PHL", adm_level=1)


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


