from floodpipeline.secrets import Secrets
from floodpipeline.data import BaseDataSet, FloodForecastDataUnit


class Forecast:
    """
    Forecast flood events based on river discharge data
    1. determine if trigger level is reached, with which probability, and the 'EAP Alert Class'
    2. compute exposure (people affected)
    3. compute flood extent
    """

    def __init__(self, secrets: Secrets = None):
        if secrets is not None:
            self.secret = secrets
        self.flood_data = BaseDataSet()
    
    def forecast(
            self,
            river_discharges: BaseDataSet,
            trigger_thresholds: BaseDataSet,
    ) -> BaseDataSet:
        self.__compute_triggers(river_discharges, trigger_thresholds)
        self.__compute_exposure()
        self.__compute_flood_extent()
        return self.flood_data
            
    def __compute_triggers(
            self,
            river_discharges: BaseDataSet,
            trigger_thresholds: BaseDataSet,
    ):
        """ Determine if trigger level is reached, its probability, and the 'EAP Alert Class' """
        pass
    
    def __compute_exposure(self):
        """ Compute exposure (people affected) """
        pass
        
    def __compute_flood_extent(self):
        """ Compute flood extent """
        pass
    
    # START: TO BE DEPRECATED
    def __compute_triggers_stations(
            self,
            river_discharges: BaseDataSet,
            trigger_thresholds: BaseDataSet,
    ):
        """ Determine if trigger level is reached, its probability, and the 'EAP Alert Class' """
        pass
    # END: TO BE DEPRECATED
