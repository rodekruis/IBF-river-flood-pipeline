from datetime import datetime
from typing import List, TypedDict


class BaseDataUnit:
    """Base class for pipeline data units"""

    def __init__(self, **kwargs):
        self.adm_level: int = kwargs.get("adm_level")
        self.pcode: str = kwargs.get("pcode")


class RiverDischargeDataUnit(BaseDataUnit):
    """River discharge data unit"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time = kwargs.get("lead_time")
        self.river_discharge_mean: float = kwargs.get("river_discharge_mean", None)
        self.river_discharge_ensemble: List[float] = kwargs.get(
            "river_discharge_ensemble", None
        )

    def compute_mean(self):
        """Compute mean river discharge"""
        self.river_discharge_mean = sum(self.river_discharge_ensemble) / len(
            self.river_discharge_ensemble
        )


class FloodForecast(TypedDict):
    return_period: float
    likelihood: float


class FloodForecastDataUnit(BaseDataUnit):
    """Flood forecast data unit"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time = kwargs.get("lead_time")
        self.flood_forecasts: List[FloodForecast] = kwargs.get("flood_forecasts", None)
        self.pop_affected: int = kwargs.get("pop_affected", 0)  # population affected
        self.pop_affected_perc: float = kwargs.get(
            "pop_affected_perc", 0.0
        )  # population affected (%)
        # START: TO BE DEPRECATED
        self.triggered: bool = kwargs.get("triggered", None)  # triggered or not
        self.return_period: float = kwargs.get("return_period", None)  # return period
        self.alert_class: float = kwargs.get("alert_class", None)  # alert class [0, 1]
        # END: TO BE DEPRECATED


# START: TO BE DEPRECATED
class GloFASStationFloodForecastDataUnit(BaseDataUnit):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time = kwargs.get("lead_time")
        self.likelihood: float = kwargs.get(
            "likelihood", None
        )  # probablity of occurrence [0, 1]
        self.severity: float = kwargs.get(
            "severity", None
        )  # severity of the event [0, 1]
        self.station: str = kwargs.get("station", None)  # station ID
        self.triggered: bool = kwargs.get("triggered", None)  # triggered or not
        self.return_period: float = kwargs.get(
            "return_period", None
        )  # return period in years
        self.alert_class: float = kwargs.get("alert_class", None)  # alert class [0, 1]


# END: TO BE DEPRECATED


class TriggerThreshold(TypedDict):
    return_period: float
    threshold: float


class TriggerThresholdDataUnit(BaseDataUnit):
    """Trigger threshold data unit"""

    def __init__(self, trigger_thresholds: List[TriggerThreshold], **kwargs):
        super().__init__(**kwargs)
        self.trigger_thresholds: List[TriggerThreshold] = trigger_thresholds

    def get_threshold(self, return_period: float) -> TriggerThreshold:
        """Get trigger threshold by return period"""
        trigger_threshold = next(
            filter(
                lambda x: x.get("return_period") == return_period,
                self.trigger_thresholds,
            ),
            None,
        )
        if not trigger_threshold:
            raise ValueError(f"Return period {return_period} not found")
        else:
            return trigger_threshold


class BaseDataSet:
    """Base class for pipeline data sets"""

    def __init__(
        self,
        country: str = None,
        timestamp: datetime = datetime.now(),
        adm_levels: List[int] = None,
        data_units: List[BaseDataUnit] = None,
    ):
        self.country = country
        self.timestamp = timestamp
        self.adm_levels = adm_levels
        self.data_units = data_units

    def get_pcodes(self, adm_level: int = None):
        """Return list of unique pcodes, optionally filtered by adm_level"""
        if not adm_level:
            return list(set([x.pcode for x in self.data_units]))
        else:
            return list(
                set([x.pcode for x in self.data_units if x.adm_level == adm_level])
            )

    def get_lead_times(self):
        """Return list of unique lead times"""
        return list(
            set([x.lead_time for x in self.data_units if hasattr(x, "lead_time")])
        )

    def get_data_unit(self, pcode: str, lead_time: int = None) -> BaseDataUnit:
        """Get data unit by pcode and optionally by lead time"""
        if not self.data_units:
            raise ValueError("Data units not found")
        if lead_time:
            bdu = next(
                filter(
                    lambda x: x.pcode == pcode and x.lead_time == lead_time,
                    self.data_units,
                ),
                None,
            )
        else:
            bdu = next(
                filter(lambda x: x.pcode == pcode, self.data_units),
                None,
            )
        if not bdu:
            raise ValueError(
                f"Data unit with pcode {pcode} and lead_time {lead_time} not found"
            )
        else:
            return bdu

    def upsert_data_unit(self, data_unit: BaseDataUnit):
        """Add data unit; if it already exists, update it"""
        if not self.data_units:
            self.data_units = [data_unit]
        if hasattr(data_unit, "lead_time"):
            bdu = next(
                filter(
                    lambda x: x[1].pcode == data_unit.pcode
                    and x[1].lead_time == data_unit.lead_time,
                    enumerate(self.data_units),
                ),
                None,
            )
        else:
            bdu = next(
                filter(
                    lambda x: x[1].pcode == data_unit.pcode,
                    enumerate(self.data_units),
                ),
                None,
            )
        if not bdu:
            self.data_units.append(data_unit)
        else:
            self.data_units[bdu[0]] = data_unit
