from datetime import datetime
from typing import List


class BaseDataUnit:
    """ Base class for pipeline data units """
    def __init__(self, **kwargs):
        self.adm_level: int = kwargs.get('adm_level')
        self.pcode: str = kwargs.get('pcode')
        self.lead_time = kwargs.get('lead_time')


class RiverDischargeDataUnit(BaseDataUnit):
    """ River discharge data unit """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.river_discharge_ensemble: List[float] = kwargs.get('river_discharge_ensemble', None)


class GloFASStationDataUnit(RiverDischargeDataUnit):
    """ GloFAS station data unit """
    def __init__(self, station_code: str, **kwargs):
        super().__init__(**kwargs)
        self.station_code = station_code
        

class FloodForecastDataUnit(BaseDataUnit):
    """ Flood forecast data unit """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.likelihood: float = kwargs.get('likelihood', None)  # probablity of occurrence [0, 1]
        self.severity: float = kwargs.get('severity', None)  # severity of the event [0, 1]
        self.pop_affected: int = kwargs.get('pop_affected', None)  # population affected
        self.pop_affected_perc: float = kwargs.get('pop_affected_perc', None)  # population affected (%)
        # START: TO BE DEPRECATED
        self.triggered: bool = kwargs.get('triggered', None)  # triggered or not
        self.alert_class: float = kwargs.get('alert_class', None)  # alert class [0, 1]
        self.return_period: int = kwargs.get('return_period', None)  # return period in years
        # END: TO BE DEPRECATED
        

class BaseDataSet:
    """ Base class for pipeline data sets """
    
    def __init__(self, **kwargs):
        self.country: str = kwargs.get('country')
        self.timestamp: datetime = kwargs.get('datetime', datetime.today())
        self.adm_levels: List[int] = kwargs.get('adm_levels', None)
        self.data_units: List[BaseDataUnit] = kwargs.get('data_units', [])
        
    def get_data_unit(self, pcode: str, lead_time: int) -> BaseDataUnit:
        """ Get data unit by pcode and lead time """
        gdu = next(filter(lambda x: x.pcode == pcode and x.lead_time == lead_time, self.data_units), None)
        if not gdu:
            raise ValueError(f"No data unit found for pcode {pcode} and lead_time {lead_time}")
        else:
            return gdu

        
class RiverDischargeDataSet(BaseDataSet):
    """ River discharge data set """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    def add_ensemble_member(self, adm_level: int, pcode: str, lead_time: int, river_discharge: float):
        """
        Add river discharge ensemble member to RiverDischargeDataUnit;
        if RiverDischargeDataUnit does not exist, create a new one
        """
        gdu = next(filter(lambda x: x.pcode == pcode and x.lead_time == lead_time, self.data_units), None)
        if not gdu:
            new_gdu = RiverDischargeDataUnit(
                adm_level=adm_level,
                pcode=pcode,
                lead_time=lead_time,
                river_discharge_ensemble=[river_discharge])
            self.data_units.append(new_gdu)
        else:
            gdu.river_discharge_ensemble.append(river_discharge)
