# src/config/settings.py
import os
import logging.config
from pathlib import Path

import yaml


def carregar_config(path: str | None = None) -> dict:
    """Carrega o arquivo YAML de configuração.

    Ordem de resolução:
    1. Caminho explícito fornecido por argumento
    2. 'settings.yaml' na raiz de execução (quando distribuído via spark-submit --files)
    3. './data-engineering-pyspark/config/settings.yaml' (desenvolvimento local na IDE)
    """
    padrao = Path("./data-engineering-pyspark/config/settings.yaml")

    candidatos = [
        Path(path) if path else None,
        Path("settings.yaml"),
        padrao,
    ]

    # Retorna o primeiro caminho válido que realmente exista no disco
    caminho = next((c for c in candidatos if c and c.exists()), padrao)

    with open(caminho, "r", encoding="utf-8") as file:
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
