import os.path
from datetime import datetime, timedelta
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, RiverDischargeDataUnit

if not os.path.exists(".env"):
    raise FileNotFoundError('credentials not found, run this test from root directory')
if not os.path.exists("config/config-template.yaml"):
    raise FileNotFoundError('config not found, run this test from root directory')
pipe = Pipeline(settings=Settings("config/config-template.yaml"), secrets=Secrets(".env"))

# flood_data = pipe.extract.get_data(source="GloFAS", country="UGA", adm_levels=[1])
# for data_unit in flood_data.data_units:
#     print(vars(data_unit))

pipe.forecast.get_flood_map(country="UGA", rp=10)
