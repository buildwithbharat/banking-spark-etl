from pathlib import Path
import yaml


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Load application configuration.
    """

    with Path(config_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)