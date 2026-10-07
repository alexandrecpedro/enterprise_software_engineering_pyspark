# tests/unit/test_data_handler.py
import gzip
import json
import os

import pytest
from pyspark.sql.types import FloatType, LongType

from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import LoadPedidosException


@pytest.fixture
def arquivo_clientes_gz(tmp_path):
    """JSON gzipado com dois clientes."""
    clientes = [
        {
            "id": 1,
            "nome": "Ana Lima",
            "data_nasc": "1985-03-10",
            "cpf": "000.000.000-00",
            "email": "ana@test.com",
            "interesses": ["Tech"],
        },
        {
            "id": 2,
            "nome": "Carlos Melo",
            "data_nasc": "1990-07-22",
            "cpf": "111.111.111-11",
            "email": "carlos@test.com",
            "interesses": [],
        },
    ]
    gz_path = tmp_path / "clientes.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for c in clientes:
            f.write(json.dumps(c) + "\n")
    return str(gz_path)


@pytest.fixture
def arquivo_pedidos_gz(tmp_path):
    """CSV gzipado com dois pedidos (separador ';')."""
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
        "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
    ]
    gz_path = tmp_path / "pedidos.csv.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    return str(gz_path)


class TestLoadClientes:

    def test_le_json_e_aplica_schema(self, spark, arquivo_clientes_gz):
        """Lê o JSON gzipado e aplica o schema explícito (id como LongType, não String)."""
        df = DataHandler(spark).load_clientes(arquivo_clientes_gz)
        assert df.count() == 2
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["id"], LongType)


class TestLoadPedidos:

    def test_le_csv_e_aplica_tipos_numericos(self, spark, arquivo_pedidos_gz):
        """Sem schema, valor_unitario/quantidade viriam como String e a multiplicação falharia."""
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";",
        )
        assert df.count() == 2
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["valor_unitario"], FloatType)
        assert isinstance(tipos["quantidade"], LongType)


class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
        """Verificar só a criação da pasta não basta: relemos para garantir integridade."""
        df = spark.createDataFrame(
            [(1, 3000.0), (2, 300.0)],
            "id_cliente long, valor_total float",
        )
        output_path = str(tmp_path / "saida")
        DataHandler(spark).write_parquet(df, output_path)
        assert os.path.exists(output_path)
        assert spark.read.parquet(output_path).count() == 2


class TestLoadPedidosErros:

    def test_caminho_inexistente_lanca_load_pedidos_exception(self, spark, tmp_path):
        """Ler um caminho inexistente deve resultar em LoadPedidosException, não no erro cru do Spark."""
        caminho_invalido = str(tmp_path / "nao_existe.csv.gz")
        with pytest.raises(LoadPedidosException):
            DataHandler(spark).load_pedidos(
                caminho_invalido,
                compression="gzip",
                header=True,
                sep=";",
            )
