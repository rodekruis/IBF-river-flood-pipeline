from floodpipeline.pipeline import Pipeline
from floodpipeline.data import (
    AdminDataSet,
    DischargeDataUnit,
    DischargeStationDataUnit,
    StationDataSet,
)
import random
import ast


def get_ensemble_likelihood(ensemble, threshold):
    threshold_checks = map(
        lambda x: 1 if x > threshold else 0,
        ensemble,
    )
    likelihood = sum(threshold_checks) / len(ensemble)
    return likelihood


class Scenario:
    """Flood scenario with mock data"""

    def __init__(self, country: str, pipeline: Pipeline):
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

    def get_discharge_scenario(self, events):
        """Get river discharge data for a given country based on scenario"""
        if type(events) == str:
            events = ast.literal_eval(events)

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
        alert_on_return_period = self.pipe.settings.get_country_setting(
            self.country, "alert-on-return-period"
        )
        alert_on_probability = self.pipe.settings.get_country_setting(
            self.country, "alert-on-minimum-probability"
        )

        # prepare river discharge data
        for adm_level in adm_levels:
            country_gdf = self.pipe.load.get_adm_boundaries(
                country=self.country, adm_level=adm_level
            )
            for lead_time in range(0, 8):
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
            for lead_time in range(0, 8):
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
        for event in events:
            if event["type"] == "trigger":
                self.set_discharge(
                    station=event["station-code"],
                    lead_time=event["lead-time"],
                    return_period=trigger_on_return_period,
                    probability=trigger_on_probability,
                )
            elif event["type"] == "medium-alert" or event["type"] == "low-alert":
                level = "med" if event["type"] == "medium-alert" else "min"
                if classify_alert_on == "return-period":
                    self.set_discharge(
                        station=event["station-code"],
                        lead_time=event["lead-time"],
                        return_period=alert_on_return_period[level],
                        probability=alert_on_probability,
                    )
                elif classify_alert_on == "probability":
                    self.set_discharge(
                        station=event["station-code"],
                        lead_time=event["lead-time"],
                        return_period=alert_on_return_period,
                        probability=alert_on_probability[level],
                    )
            elif event["type"] == "no-trigger":
                pass
            else:
                raise ValueError(f"event type {event['type']} not supported")
