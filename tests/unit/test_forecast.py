import os.path
import copy
from datetime import datetime, timedelta
from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.forecast import Forecast
from floodpipeline.data import (
    BaseDataSet,
    RiverDischargeDataUnit,
    TriggerThresholdDataUnit,
    FloodForecastDataUnit,
)

if not os.path.exists(".env"):
    raise FileNotFoundError("credentials not found, run this test from root directory")
if not os.path.exists("config/config-template.yaml"):
    raise FileNotFoundError("config not found, run this test from root directory")
# pipe = Pipeline(
#     settings=Settings("config/config-template.yaml"), secrets=Secrets(".env")
# )


pcode = "UG2"

# MOCK GLOFAS DATA AND MOCK THRESHOLDS
base_data = BaseDataSet(country="UGA", timestamp=datetime.today(), adm_levels=[1])
# print(vars(base_data))
flood_data = copy.deepcopy(base_data)
flood_data.upsert_data_unit(
    RiverDischargeDataUnit(
        adm_level=1,
        pcode=pcode,
        lead_time=1,
        river_discharge_ensemble=[10, 20, 20, 20, 10, 20],
    )
)
# trigger_thresholds_data = BaseDataSet()
trigger_thresholds_data = copy.deepcopy(base_data)
trigger_thresholds_data.upsert_data_unit(
    TriggerThresholdDataUnit(
        adm_level=1,
        pcode=pcode,
        lead_time=1,
        trigger_thresholds=[
            {"return_period": 1.5, "threshold": 3.5},
            {"return_period": 2, "threshold": 5.0},
            {"return_period": 5, "threshold": 10.0},
            {"return_period": 10, "threshold": 100.0},
        ],
    )
)
# print(vars(trigger_thresholds_data))

# RUN FORECAST
forecast = Forecast(
    settings=Settings("config/config-template.yaml"), secrets=Secrets(".env")
)
forecast = forecast.forecast(
    river_discharges=flood_data,
    trigger_thresholds=trigger_thresholds_data,
)
print([vars(du) for du in forecast.data_units])
