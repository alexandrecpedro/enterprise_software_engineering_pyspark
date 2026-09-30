# tests/unit/test_data_handler.py
import gzip
import json
import os

import pytest
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    StructField,
    StructType,
)

from io_utils.data_handler import DataHandler


@pytest.fixture
def arquivo_clientes_gz(tmp_path):
    """Arquivo JSON gzipado com dois clientes de exemplo."""
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
    """Arquivo CSV gzipado com três pedidos de exemplo."""
    linhas = [
        "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
        "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
        "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
        "abc-003;MONITOR;800.0;3;2024-01-03T12:00:00;MG;1",
    ]
    gz_path = tmp_path / "pedidos.csv.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    return str(gz_path)


class TestLoadClientes:

    def test_le_json_gz_e_retorna_dataframe(self, spark, arquivo_clientes_gz):
        df = DataHandler(spark).load_clientes(arquivo_clientes_gz)
        assert df.count() == 2

    def test_schema_aplica_tipos_corretos(self, spark, arquivo_clientes_gz):
        """Schema explícito evita type coercion: sem ele, 'id' viria como String e quebraria o JOIN."""
        df = DataHandler(spark).load_clientes(arquivo_clientes_gz)
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["id"], LongType)
        assert isinstance(tipos["interesses"], ArrayType)


class TestLoadPedidos:

    def test_le_csv_gz_com_separador_ponto_e_virgula(self, spark, arquivo_pedidos_gz):
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";",
        )
        assert df.count() == 3

    def test_schema_pedidos_tem_tipos_numericos(self, spark, arquivo_pedidos_gz):
        """Sem schema, valor_unitario e quantidade viriam como String e a multiplicação falharia."""
        df = DataHandler(spark).load_pedidos(
            arquivo_pedidos_gz,
            compression="gzip",
            header=True,
            sep=";",
        )
        tipos = {f.name: f.dataType for f in df.schema.fields}
        assert isinstance(tipos["valor_unitario"], FloatType)
        assert isinstance(tipos["quantidade"], LongType)


class TestWriteParquet:

    def test_dados_gravados_podem_ser_relidos(self, spark, tmp_path):
        """Verificar só a criação do diretório não basta: relemos para garantir integridade."""
        schema = StructType(
            [
                StructField("id_cliente", LongType(), True),
                StructField("valor_total", FloatType(), True),
            ]
        )
        df = spark.createDataFrame([(1, 3000.0), (2, 300.0)], schema)
        output_path = str(tmp_path / "saida_parquet")

        DataHandler(spark).write_parquet(df, output_path)

        assert os.path.exists(output_path)
        assert spark.read.parquet(output_path).count() == 2
