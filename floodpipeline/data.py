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
        self.river_discharge_ensemble: List[float] = kwargs.get(
            "river_discharge_ensemble", None
        )


class FloodForecastDataUnit(BaseDataUnit):
    """Flood forecast data unit"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time = kwargs.get("lead_time")
        self.likelihood: float = kwargs.get(
            "likelihood", None
        )  # probablity of occurrence [0, 1]
        self.severity: float = kwargs.get(
            "severity", None
        )  # severity of the event [0, 1]
        self.pop_affected: int = kwargs.get("pop_affected", None)  # population affected
        self.pop_affected_perc: float = kwargs.get(
            "pop_affected_perc", None
        )  # population affected (%)
        # START: TO BE DEPRECATED
        self.triggered: bool = kwargs.get("triggered", None)  # triggered or not
        self.alert_class: float = kwargs.get("alert_class", None)  # alert class [0, 1]
        self.return_period: int = kwargs.get(
            "return_period", None
        )  # return period in years
        # END: TO BE DEPRECATED

    # START: TO BE DEPRECATED
    # aliases for the IBF API
    @property
    def population_affected(self) -> int:
        return self.pop_affected

    @property
    def population_affected_percentage(self) -> float:
        return self.pop_affected_perc

    @property
    def alert_threshold(self) -> float:
        return self.alert_class

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
        self.alert_class: float = kwargs.get("alert_class", None)  # alert class [0, 1]
        self.return_period: int = kwargs.get(
            "return_period", None
        )  # return period in years


# END: TO BE DEPRECATED


class TriggerThreshold(TypedDict):
    return_period: str
    threshold: float


class TriggerThresholdDataUnit(BaseDataUnit):
    """Trigger threshold data unit"""

    def __init__(self, trigger_thresholds: List[TriggerThreshold], **kwargs):
        super().__init__(**kwargs)
        self.trigger_thresholds: List[TriggerThreshold] = trigger_thresholds


class BaseDataSet:
    """Base class for pipeline data sets"""

    def __init__(
        self,
        country: str = None,
        timestamp: datetime = datetime.now(),
        adm_levels: List[int] = None,
        data_units: List[BaseDataUnit] = [],
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
        return list(set([x.lead_time for x in self.data_units]))

    def get_data_unit(self, pcode: str, lead_time: int):
        """Get data unit by pcode and lead time"""
        bdu = next(
            filter(
                lambda x: x.pcode == pcode and x.lead_time == lead_time, self.data_units
            ),
            None,
        )
        if not bdu:
            return None
        else:
            return bdu

    def upsert_data_unit(self, data_unit: BaseDataUnit):
        """Add data unit; if it already exists, update it"""
        bdu = next(
            filter(
                lambda x: x[1].pcode == data_unit.pcode
                and x[1].lead_time == data_unit.lead_time,
                enumerate(self.data_units),
            ),
            None,
        )
        if not bdu:
            self.data_units.append(data_unit)
        else:
            self.data_units[bdu[0]] = data_unit
