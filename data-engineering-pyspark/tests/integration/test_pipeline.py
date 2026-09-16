# tests/integration/test_pipeline.py
import gzip
import json
import pytest
from unittest.mock import MagicMock
from pyspark.sql.types import (
    ArrayType, DateType, FloatType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from io_utils.data_handler import DataHandler
from pipeline.pipeline import Pipeline
from processing.transformations import Transformation


SCHEMA_PEDIDOS = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor_unitario", FloatType(), True),
    StructField("quantidade", LongType(), True),
    StructField("data_criacao", TimestampType(), True),
    StructField("uf", StringType(), True),
    StructField("id_cliente", LongType(), True),
])

SCHEMA_CLIENTES = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("data_nasc", DateType(), True),
    StructField("cpf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("interesses", ArrayType(StringType()), True),
])


@pytest.fixture
def config_teste():
    return {
        "paths": {
            "clientes": "/mock/clientes.json.gz",
            "pedidos": "/mock/pedidos/",
            "output": "/mock/output/",
        },
        "file_options": {
            "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
        },
    }


@pytest.fixture
def dataframes_mock(spark):
    pedidos_df = spark.createDataFrame(
        [("p1", "TV", 1500.0, 2, None, "SP", 1),
         ("p2", "PC", 3000.0, 1, None, "RJ", 2)],
        SCHEMA_PEDIDOS,
    )
    clientes_df = spark.createDataFrame(
        [(1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
         (2, "Carlos Melo", None, "111.111.111-11", "carlos@test.com", None)],
        SCHEMA_CLIENTES,
    )
    return pedidos_df, clientes_df


def _handler_mock(pedidos_df, clientes_df):
    """DataHandler falso que devolve DataFrames pré-definidos, sem ler disco."""
    handler = MagicMock(spec=DataHandler)
    handler.load_clientes.return_value = clientes_df
    handler.load_pedidos.return_value = pedidos_df
    return handler


class TestPipelineOrquestracao:
    """Verifica SE e COMO o Pipeline chama suas dependências, usando um DataHandler mockado."""

    def test_le_clientes_com_path_da_config(self, spark, config_teste, dataframes_mock):
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.load_clientes.assert_called_once_with(path="/mock/clientes.json.gz")

    def test_le_pedidos_com_parametros_da_config(self, spark, config_teste, dataframes_mock):
        """Um separador errado faria o CSV ser lido como uma coluna só — sem erro, mas com dados errados."""
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.load_pedidos.assert_called_once_with(
            path="/mock/pedidos/", compression="gzip", header=True, sep=";",
        )

    def test_grava_no_path_de_output(self, spark, config_teste, dataframes_mock):
        handler = _handler_mock(*dataframes_mock)
        Pipeline(handler, Transformation()).run(config_teste)
        handler.write_parquet.assert_called_once()
        assert handler.write_parquet.call_args.kwargs["path"] == "/mock/output/"


class TestPipelineEndToEnd:
    """Dados reais pequenos percorrem TODO o pipeline e verificamos o Parquet final."""

    def test_pipeline_completo_gera_parquet_valido(self, spark, tmp_path):
        clientes = [
            {"id": 1, "nome": "Ana Lima", "data_nasc": "1985-03-10",
             "cpf": "000.000.000-00", "email": "ana@test.com", "interesses": ["Tech"]},
            {"id": 2, "nome": "Carlos Melo", "data_nasc": "1990-07-22",
             "cpf": "111.111.111-11", "email": "carlos@test.com", "interesses": []},
        ]
        clientes_path = tmp_path / "clientes.json.gz"
        with gzip.open(clientes_path, "wt", encoding="utf-8") as f:
            for c in clientes:
                f.write(json.dumps(c) + "\n")

        pedidos_lines = [
            "id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente",
            "abc-001;TV;1500.0;2;2024-01-01T10:00:00;SP;1",
            "abc-002;PC;3000.0;1;2024-01-02T11:00:00;RJ;2",
            "abc-003;MONITOR;800.0;1;2024-01-03T12:00:00;MG;1",
        ]
        pedidos_path = tmp_path / "pedidos.csv.gz"
        with gzip.open(pedidos_path, "wt", encoding="utf-8") as f:
            f.write("\n".join(pedidos_lines))

        output_path = str(tmp_path / "output")
        config = {
            "paths": {
                "clientes": str(clientes_path),
                "pedidos": str(pedidos_path),
                "output": output_path,
            },
            "file_options": {
                "pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}
            },
        }

        Pipeline(DataHandler(spark), Transformation()).run(config)

        resultado = spark.read.parquet(output_path)
        assert set(resultado.columns) == {"id_cliente", "nome", "email", "valor_total"}
        # Ana Lima: pedidos abc-001 (1500×2=3000) + abc-003 (800×1=800) = 3800
        ana = resultado.where("nome = 'Ana Lima'").collect()
        assert ana[0].valor_total == pytest.approx(3800.0)