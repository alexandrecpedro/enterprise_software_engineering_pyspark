# tests/integration/test_pipeline.py
import gzip
import json
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.pipeline.pipeline import Pipeline
from data_engineering_pyspark.processing.transformations import Transformation

SCHEMA_PEDIDOS = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
    ]
)

SCHEMA_CLIENTES = StructType(
    [
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("data_nasc", DateType(), True),
        StructField("cpf", StringType(), True),
        StructField("email", StringType(), True),
        StructField("interesses", ArrayType(StringType()), True),
    ]
)

CONFIG = {
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
def handler_mock(spark):
    """DataHandler falso que devolve DataFrames prontos, sem ler disco."""
    pedidos = spark.createDataFrame(
        [
            ("p1", "TV", 1500.0, 2, None, "SP", 1),
            ("p2", "PC", 3000.0, 1, None, "RJ", 2),
        ],
        SCHEMA_PEDIDOS,
    )
    clientes = spark.createDataFrame(
        [
            (1, "Ana Lima", None, "000.000.000-00", "ana@test.com", None),
            (2, "Carlos Melo", None, "111.111.111-11", "carlos@test.com", None),
        ],
        SCHEMA_CLIENTES,
    )
    handler = MagicMock(spec=DataHandler)
    handler.load_clientes.return_value = clientes
    handler.load_pedidos.return_value = pedidos
    return handler


class TestPipelineOrquestracao:

    def test_le_pedidos_com_parametros_da_config(self, handler_mock):
        """Separador errado leria o CSV como uma coluna só — erro silencioso. Garantimos os parâmetros."""
        Pipeline(handler_mock, Transformation()).run(CONFIG)
        handler_mock.load_pedidos.assert_called_once_with(
            path="/mock/pedidos/",
            compression="gzip",
            header=True,
            sep=";",
        )

    def test_grava_no_path_de_output(self, handler_mock):
        Pipeline(handler_mock, Transformation()).run(CONFIG)
        handler_mock.write_parquet.assert_called_once()
        assert handler_mock.write_parquet.call_args.kwargs["path"] == "/mock/output/"


class TestPipelineEndToEnd:

    def test_fluxo_completo_gera_parquet_correto(self, spark, tmp_path):
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
        # Ana Lima: abc-001 (1500×2=3000) + abc-003 (800×1=800) = 3800
        ana = resultado.where("nome = 'Ana Lima'").collect()
        assert ana[0].valor_total == pytest.approx(3800.0)
