from floodpipeline.secrets import Secrets
# from floodpipeline.data import FloodDataSet, FloodDataUnit


class Forecast:
    """ Forecast flood events based on flood data """

    def __init__(self, secrets: Secrets = None):
        if secrets is not None:
            self.secret = secrets
            
    # def process_flood_dataset(self, FloodDataSet):
    