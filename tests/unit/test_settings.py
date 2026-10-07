# tests/unit/test_settings.py
import pytest
import yaml

from data_engineering_pyspark.config.settings import carregar_config


@pytest.fixture
def arquivo_config_valido(tmp_path):
    """Cria um settings.yaml mínimo e válido em diretório temporário."""
    config_data = {
        "spark": {"app_name": "TestApp"},
        "paths": {
            "clientes": "/dados/clientes.json.gz",
            "pedidos": "/dados/pedidos/",
            "output": "/dados/output/",
        },
        "file_options": {
            "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
        },
    }
    config_file = tmp_path / "settings.yaml"
    config_file.write_text(yaml.dump(config_data))
    return str(config_file)


class TestCarregarConfig:

    def test_carrega_yaml_valido_como_dicionario(self, arquivo_config_valido):
        assert isinstance(carregar_config(arquivo_config_valido), dict)

    def test_valores_sao_lidos_sem_distorcao(self, arquivo_config_valido):
        resultado = carregar_config(arquivo_config_valido)
        assert resultado["spark"]["app_name"] == "TestApp"
        assert resultado["file_options"]["pedidos_csv"]["sep"] == ";"

    def test_arquivo_inexistente_lanca_excecao(self):
        """O pipeline deve falhar rápido e com clareza, não silenciosamente com None."""
        with pytest.raises(FileNotFoundError):
            carregar_config("/caminho/que/nao/existe/settings.yaml")
