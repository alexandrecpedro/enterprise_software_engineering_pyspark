# src/config/settings.py
import yaml


def carregar_config(
    path: str = "./data-engineering-pyspark/config/settings.yaml",
) -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, "r") as file:
        return yaml.safe_load(file)
