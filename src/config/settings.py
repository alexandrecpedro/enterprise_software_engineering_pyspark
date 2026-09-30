# src/config/settings.py
import os
import yaml
import logging.config


def carregar_config(
    path: str = "./data-engineering-pyspark/config/settings.yaml",
) -> dict:
    """Carrega um arquivo de configuração YAML."""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def configurar_logging(config_logging: dict):
    """Aplica a configuração de logging lida do YAML, criando diretórios se necessário."""
    # Extrai o caminho do arquivo configurado no handler 'arquivo'
    handlers = config_logging.get("handlers", {})
    file_handler = handlers.get("file", {})
    filename = file_handler.get("filename")

    # Garante que o diretório onde o log será salvo exista antes de inicializar o logging
    if filename:
        log_dir = os.path.dirname(filename)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

    logging.config.dictConfig(config_logging)
    logging.getLogger(__name__).info("Logging configurado com sucesso via YAML.")
