import os
import yaml
from urllib.parse import urlparse
from typing import List


def is_url(x):
    try:
        result = urlparse(x)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


class Settings:
    """
    Settings (URLs, constants, etc.)
    """

    def __init__(self, path_or_url: str):
        self.setting_source: str = "yaml"
        self.setting_path: str = path_or_url
        if not os.path.exists(self.setting_path) and not is_url(self.setting_path):
            raise ValueError(f"Setting file {self.setting_path} not found")
        with open(self.setting_path, "r") as file:
            self.settings = yaml.load(file, Loader=yaml.FullLoader)

    def get_setting(self, setting: str):
        setting_value = None
        if setting in self.settings.keys():
            setting_value = self.settings[setting]
        else:
            for key in self.settings.keys():
                if type(self.settings[key]) == dict:
                    if setting in self.settings[key].keys():
                        setting_value = self.settings[key][setting]
                elif type(self.settings[key]) == list:
                    for i in range(len(self.settings[key])):
                        if setting in self.settings[key][i].keys():
                            setting_value = self.settings[key][i][setting]
        if not setting_value:
            raise ValueError(f"Setting {setting} not found in {self.setting_path}")
        return setting_value

    def get_country_setting(self, country: str, setting: str):
        country_setting = next(
            x for x in self.get_setting("countries") if x["name"] == country
        )
        if not country_setting:
            raise ValueError(f"Country {country} not found in {self.setting_path}")
        if setting not in country_setting.keys():
            raise ValueError(f"Setting {setting} not found for country {country}")
        return country_setting[setting]

    def check_settings(self, settings: List[str]):
        missing_settings = []
        for setting in settings:
            try:
                self.get_setting(setting)
            except ValueError:
                missing_settings.append(setting)
        if missing_settings:
            raise Exception(
                f"Missing settings {', '.join(missing_settings)} in {self.setting_path}"
            )
