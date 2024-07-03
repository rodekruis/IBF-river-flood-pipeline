import os.path
from datetime import datetime, timedelta
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.data import RiverDischargeDataSet, RiverDischargeDataUnit

if not os.path.exists("credentials/.env"):
    print('credentials not found, run this test from root directory')
pipe = Pipeline(secrets=Secrets("credentials/.env"))

# flood_data = pipe.extract.get_data(source="GloFAS", country="UGA", adm_levels=[1])

dataset = RiverDischargeDataSet(country="UGA", datetime=datetime.today(), adm_levels=[1])
dataset.add_ensemble_member(adm_level=1, pcode="river_forecast", lead_time=1, river_discharge=1.0)

data_unit = dataset.get_data_unit(pcode="river_forecast", lead_time=1)
print(vars(data_unit))

