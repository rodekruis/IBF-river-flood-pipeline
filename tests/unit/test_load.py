import os.path
from datetime import datetime, timedelta
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, RiverDischargeDataUnit

if not os.path.exists(".env"):
    print("credentials not found, run this test from root directory")
pipe = Pipeline(
    secrets=Secrets(".env"),
    settings=Settings("config/config-template.yaml"),
)

print("get trigger thresholds from storage")
thresholds = pipe.load.get_pipeline_data(data_type="trigger-threshold", country="UGA")
