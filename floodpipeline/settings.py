import os
import yaml
from urllib.parse import urlparse


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

    def get_setting(self, setting):
        setting_value = None
        for key in self.settings.keys():
            if setting in self.settings[key].keys():
                setting_value = self.settings[key][setting]
        if not setting_value:
            raise ValueError(f"Setting {setting} not found in {self.setting_path}")
        return setting_value

    def check_settings(self, settings):
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
