from floodpipeline.pipeline import Pipeline
from floodpipeline.secrets import Secrets
from floodpipeline.settings import Settings
from floodpipeline.data import BaseDataSet, RiverDischargeDataUnit
from datetime import datetime
import copy
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

    def __init__(
        self,
        scenario: str,
        country: str,
        settings: Settings = None,
        secrets: Secrets = None,
    ):
        self.scenario = scenario
        self.country = country
        self.pipe = Pipeline(settings=settings, secrets=secrets)
        self.trigger_thresholds_dataset = BaseDataSet()
        self.river_discharge_dataset = BaseDataSet()
        self.noEns = self.pipe.settings.get_setting("no_ensemble_members")

    def get_river_discharge_per_return_period(self, pcode, return_period):
        """Get river discharge corresponding to given return period"""
        trigger_threshold_data_unit = self.trigger_thresholds_dataset.get_data_unit(
            pcode
        )
        trigger_threshold = next(
            t["threshold"]
            for t in trigger_threshold_data_unit.trigger_thresholds
            if t["return_period"] == return_period
        )
        return trigger_threshold * 1.01

    def set_river_discharge(
        self,
        pcode: float,
        lead_time: float,
        return_period: float,
        probability: float = 1.0,
    ):
        """Set river discharge value"""
        value = self.get_river_discharge_per_return_period(pcode, return_period)
        rddu = self.river_discharge_dataset.get_data_unit(pcode, lead_time)
        ensemble_values = [random.gauss(value, value * 0.1) for _ in range(self.noEns)]
        while get_ensemble_likelihood(ensemble_values, value) < probability:
            ensemble_values = [e * 1.01 for e in ensemble_values]
        rddu.river_discharge_ensemble = ensemble_values
        rddu.river_discharge_mean = sum(ensemble_values) / len(ensemble_values)
        self.river_discharge_dataset.upsert_data_unit(rddu)

    def get_random_pcodes(self, adm_level):
        """Get random pcodes"""
        country_gdf = self.pipe.load.get_adm_boundaries(
            country=self.country, adm_level=adm_level
        )
        return list(
            country_gdf[f"adm{adm_level}_pcode"].sample(n=2, random_state=1).values
        )

    def get_river_discharge_scenario(
        self,
        random_pcodes: bool = True,
        pcodes: list = None,
    ):
        """Get river discharge data for a given country based on scenario"""

        if self.scenario not in SCENARIOS:
            raise ValueError(
                f"Invalid scenario: {self.scenario}. Valid scenarios are: {SCENARIOS}"
            )

        trigger_on_adm_level = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-admin-level"
        )
        trigger_on_lead_time = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-lead-time"
        )
        trigger_on_return_period = self.pipe.settings.get_country_setting(
            self.country, "trigger-on-return-period"
        )
        trigger_on_probability = (
            self.pipe.settings.get_country_setting(
                self.country, "trigger-on-minimum-probability"
            )
            + 0.01
        )

        alert_on_return_period = self.pipe.settings.get_country_setting(
            self.country, "alert-on-return-period"
        )
        alert_on_probability = (
            self.pipe.settings.get_country_setting(
                self.country, "alert-on-minimum-probability"
            )
            + 0.01
        )
        alert_classes = ["min", "max", "med", "min", "max", "med"]

        self.trigger_thresholds_dataset = self.pipe.load.get_pipeline_data(
            data_type="trigger-threshold", country=self.country
        )

        if not random_pcodes:
            if not pcodes:
                raise ValueError("pcodes must be provided if random_pcodes is False")
        else:
            pcodes = self.get_random_pcodes(trigger_on_adm_level)

        # prepare river discharge data
        self.river_discharge_dataset = BaseDataSet(
            country=self.country,
            timestamp=datetime.today(),
            adm_levels=self.pipe.settings.get_country_setting(
                self.country, "admin-levels"
            ),
        )
        for adm_level in self.river_discharge_dataset.adm_levels:
            country_gdf = self.pipe.load.get_adm_boundaries(
                country=self.country, adm_level=adm_level
            )
            for lead_time in range(0, 8):
                for ix, row in country_gdf.iterrows():
                    self.river_discharge_dataset.upsert_data_unit(
                        RiverDischargeDataUnit(
                            adm_level=adm_level,
                            lead_time=lead_time,
                            pcode=row[f"adm{adm_level}_pcode"],
                            river_discharge_ensemble=[1.0] * self.noEns,
                            river_discharge_mean=1.0,
                        )
                    )

        # apply scenario
        if self.scenario == "nothing":
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-on-lead-time":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=trigger_on_lead_time,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-after-lead-time":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=trigger_on_lead_time + 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-before-lead-time":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=trigger_on_lead_time - 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-multiple-on-lead-time":
            for pcode in pcodes:
                self.set_river_discharge(
                    pcode=pcode,
                    lead_time=trigger_on_lead_time,
                    return_period=trigger_on_return_period,
                    probability=trigger_on_probability,
                )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "alert":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=random.randint(1, 6),
                return_period=alert_on_return_period[alert_classes[0]],
                probability=alert_on_probability,
            )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "alert-multiple":
            for ix, pcode in enumerate(pcodes):
                self.set_river_discharge(
                    pcode=pcode,
                    lead_time=random.randint(1, 6),
                    return_period=alert_on_return_period[alert_classes[ix]],
                    probability=alert_on_probability,
                )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-and-alert":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=trigger_on_lead_time - 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            self.set_river_discharge(
                pcode=pcodes[1],
                lead_time=random.randint(1, 6),
                return_period=alert_on_return_period[alert_classes[1]],
                probability=alert_on_probability,
            )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-and-alert-multiple":
            self.set_river_discharge(
                pcode=pcodes[0],
                lead_time=trigger_on_lead_time - 1,
                return_period=trigger_on_return_period,
                probability=trigger_on_probability,
            )
            for ix, pcode in enumerate(pcodes[1:]):
                self.set_river_discharge(
                    pcode=pcode,
                    lead_time=random.randint(1, 6),
                    return_period=alert_on_return_period[alert_classes[ix]],
                    probability=alert_on_probability,
                )
            return copy.deepcopy(self.river_discharge_dataset)

        elif self.scenario == "trigger-multiple-and-alert-multiple":
            for pcode in pcodes[:2]:
                self.set_river_discharge(
                    pcode=pcode,
                    lead_time=trigger_on_lead_time,
                    return_period=trigger_on_return_period,
                    probability=trigger_on_probability,
                )
            for ix, pcode in enumerate(pcodes[2:]):
                self.set_river_discharge(
                    pcode=pcode,
                    lead_time=random.randint(1, 6),
                    return_period=alert_on_return_period[alert_classes[ix]],
                    probability=alert_on_probability,
                )
            return copy.deepcopy(self.river_discharge_dataset)
