from datetime import datetime
from typing import List, TypedDict
from floodpipeline.settings import Settings
from floodpipeline.secrets import Secrets


class AdminDataUnit:
    """Base class for admin data units"""

    def __init__(self, **kwargs):
        self.adm_level: int = kwargs.get("adm_level")
        self.pcode: str = kwargs.get("pcode")


class StationDataUnit:
    """Base class for station data units"""

    def __init__(self, **kwargs):
        self.station_code: str = kwargs.get("station_code")
        self.station_name: str = kwargs.get("station_name")
        self.lat: float = kwargs.get("lat")
        self.lon: float = kwargs.get("lon")
        self.pcodes: dict = kwargs.get(
            "pcodes"
        )  # pcodes of associated administrative divisions


class DischargeDataUnit(AdminDataUnit):
    """River discharge data unit"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time: int = kwargs.get("lead_time")
        self.discharge_ensemble: List[float] = kwargs.get("discharge_ensemble", None)
        self.discharge_mean: float = kwargs.get("discharge_mean", None)
        if hasattr(self.discharge_ensemble, "__iter__"):
            self.compute_mean()

    def compute_mean(self):
        """Compute mean river discharge"""
        self.discharge_mean = sum(self.discharge_ensemble) / len(
            self.discharge_ensemble
        )


class DischargeStationDataUnit(StationDataUnit):
    """River discharge data unit - station"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time: int = kwargs.get("lead_time")
        self.discharge_ensemble: List[float] = kwargs.get("discharge_ensemble", None)
        self.discharge_mean: float = kwargs.get("discharge_mean", None)
        if hasattr(self.discharge_ensemble, "__iter__"):
            self.compute_mean()

    def compute_mean(self):
        """Compute mean river discharge"""
        self.discharge_mean = sum(self.discharge_ensemble) / len(
            self.discharge_ensemble
        )


class FloodForecast(TypedDict):
    return_period: float
    likelihood: float


class ForecastDataUnit(AdminDataUnit):
    """Flood forecast data unit"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time: int = kwargs.get("lead_time")
        self.forecasts: List[FloodForecast] = kwargs.get("forecasts", None)
        self.pop_affected: int = kwargs.get("pop_affected", 0)  # population affected
        self.pop_affected_perc: float = kwargs.get(
            "pop_affected_perc", 0.0
        )  # population affected (%)
        self.triggered: bool = kwargs.get("triggered", None)  # triggered or not
        self.return_period: float = kwargs.get("return_period", None)  # return period
        self.alert_class: str = kwargs.get("alert_class", None)  # alert class


class ForecastStationDataUnit(StationDataUnit):
    """Flood forecast data unit - station"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lead_time: int = kwargs.get("lead_time")
        self.forecasts: List[FloodForecast] = kwargs.get("forecasts", None)
        self.triggered: bool = kwargs.get("triggered", None)  # triggered or not
        self.return_period: float = kwargs.get("return_period", None)  # return period
        self.alert_class: str = kwargs.get("alert_class", None)  # alert class


class Threshold(TypedDict):
    return_period: float
    threshold_value: float


class ThresholdDataUnit(AdminDataUnit):
    """Trigger/alert threshold data unit"""

    def __init__(self, thresholds: List[Threshold], **kwargs):
        super().__init__(**kwargs)
        self.thresholds: List[Threshold] = thresholds

    def get_threshold(self, return_period: float) -> Threshold:
        """Get trigger threshold by return period"""
        threshold = next(
            filter(
                lambda x: x.get("return_period") == return_period,
                self.thresholds,
            ),
            None,
        )
        if not threshold:
            raise ValueError(f"Return period {return_period} not found")
        else:
            return threshold["threshold_value"]


class ThresholdStationDataUnit(StationDataUnit):
    """Trigger/alert threshold data unit - station"""

    def __init__(self, thresholds: List[Threshold], **kwargs):
        super().__init__(**kwargs)
        self.thresholds: List[Threshold] = thresholds

    def get_threshold(self, return_period: float) -> Threshold:
        """Get trigger threshold by return period"""
        threshold = next(
            filter(
                lambda x: x.get("return_period") == return_period,
                self.thresholds,
            ),
            None,
        )
        if not threshold:
            raise ValueError(f"Return period {return_period} not found")
        else:
            return threshold["threshold_value"]


class AdminDataSet:
    """Base class for admin data sets"""

    def __init__(
        self,
        country: str = None,
        timestamp: datetime = datetime.now(),
        adm_levels: List[int] = None,
        data_units: List[AdminDataUnit] = None,
    ):
        self.country = country
        self.timestamp = timestamp
        self.data_units = data_units
        if not adm_levels and data_units and len(data_units) > 0:
            self.adm_levels = list(
                set([data_unit.adm_level for data_unit in data_units])
            )
        else:
            self.adm_levels = adm_levels

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

    def get_data_units(self, lead_time: int = None, adm_level: int = None):
        """Return list of data units filtered by lead time and/or admin level"""
        if not self.data_units:
            raise ValueError("Data units not found")
        if lead_time is not None and adm_level is not None:
            return list(
                filter(
                    lambda x: x.lead_time == lead_time and x.adm_level == adm_level,
                    self.data_units,
                )
            )
        elif lead_time is not None:
            return list(filter(lambda x: x.lead_time == lead_time, self.data_units))
        elif adm_level is not None:
            return list(filter(lambda x: x.adm_level == adm_level, self.data_units))
        else:
            return self.data_units

    def get_data_unit(self, pcode: str, lead_time: int = None) -> AdminDataUnit:
        """Get data unit by pcode and optionally by lead time"""
        if not self.data_units:
            raise ValueError("Data units not found")
        if lead_time is not None:
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

    def upsert_data_unit(self, data_unit: AdminDataUnit):
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

    def is_any_triggered(self):
        """Check if any data unit is triggered"""
        if not self.data_units:
            raise ValueError("Data units not found")
        if type(self.data_units[0]) != ForecastDataUnit:
            raise ValueError("Data units are not forecast data units")
        return any([x.triggered for x in self.data_units])


class StationDataSet:
    """Base class for station data sets"""

    def __init__(
        self,
        country: str = None,
        timestamp: datetime = datetime.now(),
        data_units: List[StationDataUnit] = None,
    ):
        self.country = country
        self.timestamp = timestamp
        self.data_units = data_units

    def get_data_unit(
        self, station_code: str, lead_time: int = None
    ) -> StationDataUnit:
        """Get data unit by station_code and optionally by lead time"""
        if not self.data_units:
            raise ValueError("Data units not found")
        if lead_time:
            bdu = next(
                filter(
                    lambda x: x.station_code == station_code
                    and x.lead_time == lead_time,
                    self.data_units,
                ),
                None,
            )
        else:
            bdu = next(
                filter(lambda x: x.station_code == station_code, self.data_units),
                None,
            )
        if not bdu:
            raise ValueError(
                f"Data unit with station_code {station_code} and lead_time {lead_time} not found"
            )
        else:
            return bdu

    def upsert_data_unit(self, data_unit: StationDataUnit):
        """Add data unit; if it already exists, update it"""
        if not self.data_units:
            self.data_units = [data_unit]
        if hasattr(data_unit, "lead_time"):
            bdu = next(
                filter(
                    lambda x: x[1].station_code == data_unit.station_code
                    and x[1].lead_time == data_unit.lead_time,
                    enumerate(self.data_units),
                ),
                None,
            )
        else:
            bdu = next(
                filter(
                    lambda x: x[1].station_code == data_unit.station_code,
                    enumerate(self.data_units),
                ),
                None,
            )
        if not bdu:
            self.data_units.append(data_unit)
        else:
            self.data_units[bdu[0]] = data_unit

    def get_lead_times(self):
        """Return list of unique lead times"""
        return list(
            set([x.lead_time for x in self.data_units if hasattr(x, "lead_time")])
        )

    def get_station_codes(self):
        """Return list of unique station codes"""
        return list(
            set([x.station_code for x in self.data_units if hasattr(x, "station_code")])
        )


class PipelineDataSets:
    """Collection of datasets used by the pipeline"""

    def __init__(self, country: str, settings: Settings):
        self.country = country
        self.discharge_admin = AdminDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=settings.get_country_setting(country, "admin-levels"),
        )
        self.discharge_station = StationDataSet(
            country=self.country, timestamp=datetime.today()
        )
        self.forecast_admin = AdminDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=settings.get_country_setting(country, "admin-levels"),
        )
        self.forecast_station = StationDataSet(
            country=self.country, timestamp=datetime.today()
        )
        self.threshold_admin = AdminDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=settings.get_country_setting(country, "admin-levels"),
        )
        self.threshold_station = StationDataSet(
            country=self.country, timestamp=datetime.today()
        )
