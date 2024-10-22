from floodpipeline.pipeline import Pipeline
from floodpipeline.data import (
    AdminDataSet,
    DischargeDataUnit,
    DischargeStationDataUnit,
    StationDataSet,
)
import random

SCENARIOS = [
    "nothing",
    "trigger-on-lead-time",
    "trigger-after-lead-time",
    "trigger-before-lead-time",
    "trigger-multiple-on-lead-time",
    "alert",
    "alert-multiple",
    "trigger-and-alert",
    "trigger-and-alert-multiple",
    "trigger-multiple-and-alert-multiple",
]


def get_ensemble_likelihood(ensemble, threshold):
    threshold_checks = map(
        lambda x: 1 if x > threshold else 0,
        ensemble,
    )
    likelihood = sum(threshold_checks) / len(ensemble)
    return likelihood


class Scenario:
    """Extract river discharge data from external sources"""

    def __init__(self, scenario: str, country: str, pipeline: Pipeline):
        self.scenario = scenario
        self.country = country
        self.pipe = pipeline
        self.noEns = self.pipe.settings.get_setting("no_ensemble_members")

    def get_discharge_per_return_period(self, return_period, pcode=None, station=None):
        """Get river discharge corresponding to given return period"""
        if pcode is not None:
            threshold_data_unit = self.pipe.data.threshold_admin.get_data_unit(pcode)
        elif station is not None:
            threshold_data_unit = self.pipe.data.threshold_station.get_data_unit(
                station
            )
        else:
            raise ValueError("Either pcode or station must be provided")
        trigger_threshold = threshold_data_unit.get_threshold(return_period)
        return trigger_threshold * 1.01

    def set_discharge(
        self,
        station: str,
        lead_time: float,
        return_period: float,
        probability: float = 1.0,
    ):
        """Set river discharge value"""
        value = self.get_discharge_per_return_period(
            station=station, return_period=return_period
        )
        discharge_station = self.pipe.data.discharge_station.get_data_unit(
            station, lead_time
        )
        ensemble_values = [random.gauss(value, value * 0.1) for _ in range(self.noEns)]
        while get_ensemble_likelihood(ensemble_values, value) < probability:
            ensemble_values = [e * 1.01 for e in ensemble_values]
        discharge_station.discharge_ensemble = ensemble_values
        discharge_station.discharge_mean = sum(ensemble_values) / len(ensemble_values)
        self.pipe.data.discharge_station.upsert_data_unit(discharge_station)

        for adm_level in discharge_station.pcodes.keys():
            for pcode in discharge_station.pcodes[adm_level]:
                value = self.get_discharge_per_return_period(
                    pcode=pcode, return_period=return_period
                )
                discharge_admin = self.pipe.data.discharge_admin.get_data_unit(
                    pcode, lead_time
                )
                ensemble_values = [
                    random.gauss(value, value * 0.1) for _ in range(self.noEns)
                ]
                while get_ensemble_likelihood(ensemble_values, value) < probability:
                    ensemble_values = [e * 1.01 for e in ensemble_values]
                discharge_admin.discharge_ensemble = ensemble_values
                discharge_admin.discharge_mean = sum(ensemble_values) / len(
                    ensemble_values
                )
                self.pipe.data.discharge_admin.upsert_data_unit(discharge_admin)

    def get_random_stations(self, n):
        """Get random station codes"""
        station_codes = self.pipe.data.threshold_station.get_station_codes()
        random_station_codes = []
        for i in range(n):
            random_station_code = random.choice(station_codes)
            while random_station_code in random_station_codes:
                random_station_code = random.choice(station_codes)
            random_station_codes.append(random.choice(station_codes))
        return random_station_codes

    def get_discharge_scenario(
        self,
        random_stations: bool = True,
        stations: list = None,
    ):
        """Get river discharge data for a given country based on scenario"""

        if self.scenario not in SCENARIOS:
            raise ValueError(
                f"Invalid scenario: {self.scenario}. Valid scenarios are: {SCENARIOS}"
            )

        adm_levels = self.pipe.settings.get_country_setting(
            self.country, "admin-levels"
        )

        trigger_on_lead_time = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-lead-time"
        )
        trigger_on_return_period = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-return-period"
        )
        trigger_on_probability = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-minimum-probability"
        )

        classify_alert_on = self.pipe.settings.get_country_setting(
            self.country, "classify-alert-on"
        )
        if classify_alert_on == "return-period":
            alert_on_return_period = self.pipe.settings.get_country_setting(
                self.country, "alert-on-return-period"
            )["med"]
            alert_on_probability = self.pipe.settings.get_country_setting(
                self.country, "alert-on-minimum-probability"
            )
        elif classify_alert_on == "probability":
            alert_on_return_period = self.pipe.settings.get_country_setting(
                self.country, "alert-on-return-period"
            )
            alert_on_probability = self.pipe.settings.get_country_setting(
                self.country, "alert-on-minimum-probability"
            )["med"]
        elif classify_alert_on == "disable":
            alert_on_return_period = self.pipe.settings.get_country_setting(
                self.country, "alert-on-return-period"
            )
            # if type(alert_on_return_period) == dict:
            #     alert_on_return_period = alert_on_return_period["max"]
            alert_on_probability = self.pipe.settings.get_country_setting(
                self.country, "alert-on-minimum-probability"
            )["max"]
            # if type(alert_on_probability) == dict:
            #     alert_on_probability = alert_on_probability["max"]
        alert_classes = ["min", "med", "min", "med"]

        if not random_stations:
            if not stations:
                raise ValueError("stations must be provided if random_pcodes is False")
        else:
            stations = self.get_random_stations(n=4)

        # prepare river discharge data
        for adm_level in adm_levels:
            country_gdf = self.pipe.load.get_adm_boundaries(
                country=self.country, adm_level=adm_level
            )
            for lead_time in range(1, 8):
                for ix, row in country_gdf.iterrows():
                    self.pipe.data.discharge_admin.upsert_data_unit(
                        DischargeDataUnit(
                            adm_level=adm_level,
                            lead_time=lead_time,
                            pcode=row[f"adm{adm_level}_pcode"],
                            discharge_ensemble=[0.01] * self.noEns,
                        )
                    )
        for station in self.pipe.data.threshold_station.data_units:
            for lead_time in range(1, 8):
                self.pipe.data.discharge_station.upsert_data_unit(
                    DischargeStationDataUnit(
                        station_code=station.station_code,
                        station_name=station.station_name,
                        lat=station.lat,
                        lon=station.lon,
                        pcodes=station.pcodes,
                        lead_time=lead_time,
                        discharge_ensemble=[0.01] * self.noEns,
                    )
                )

        # apply scenario
        if self.scenario == "nothing":
            pass

        elif self.scenario == "trigger-on-lead-time":
            self.set_discharge(
                station=stations[0],
                lead_time=trigger_on_lead_time,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )

        elif self.scenario == "trigger-after-lead-time":
            self.set_discharge(
                station=stations[0],
                lead_time=trigger_on_lead_time + 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )

        elif self.scenario == "trigger-before-lead-time":
            self.set_discharge(
                station=stations[0],
                lead_time=trigger_on_lead_time - 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )

        elif self.scenario == "trigger-multiple-on-lead-time":
            for station in stations:
                self.set_discharge(
                    station=station,
                    lead_time=trigger_on_lead_time,
                    return_period=trigger_on_return_period,
                    probability=trigger_on_probability,
                )

        elif self.scenario == "alert":
            self.set_discharge(
                station=stations[0],
                lead_time=2,
                return_period=alert_on_return_period,
                probability=alert_on_probability,
            )

        elif self.scenario == "alert-multiple":
            for ix, station in enumerate(stations):
                self.set_discharge(
                    station=station,
                    lead_time=2,
                    return_period=alert_on_return_period,
                    probability=alert_on_probability,
                )

        elif self.scenario == "trigger-and-alert":
            self.set_discharge(
                station=stations[0],
                lead_time=trigger_on_lead_time,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            self.set_discharge(
                station=stations[1],
                lead_time=2,
                return_period=alert_on_return_period,
                probability=alert_on_probability,
            )

        elif self.scenario == "trigger-and-alert-multiple":
            self.set_discharge(
                station=stations[0],
                lead_time=trigger_on_lead_time,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            for ix, station in enumerate(stations[1:]):
                self.set_discharge(
                    station=station,
                    lead_time=2,
                    return_period=alert_on_return_period,
                    probability=alert_on_probability,
                )

        elif self.scenario == "trigger-multiple-and-alert-multiple":
            for station in stations[:2]:
                self.set_discharge(
                    station=station,
                    lead_time=trigger_on_lead_time,
                    return_period=trigger_on_return_period,
                    probability=trigger_on_probability,
                )
            for ix, station in enumerate(stations[2:]):
                self.set_discharge(
                    station=station,
                    lead_time=2,
                    return_period=alert_on_return_period,
                    probability=alert_on_probability,
                )
