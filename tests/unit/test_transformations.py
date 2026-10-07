# tests/unit/test_transformations.py
import pytest
from pyspark.sql.types import (
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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

SCHEMA_TOTAL = StructType(
    [
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", FloatType(), True),
    ]
)

# join_pedidos_clientes só usa id, nome e email do lado de clientes
SCHEMA_CLIENTES = StructType(
    [
        StructField("id", LongType(), True),
        StructField("nome", StringType(), True),
        StructField("email", StringType(), True),
    ]
)


class TestAddValorTotalPedidos:

    def test_calcula_valor_total(self, spark):
        """valor_total deve ser valor_unitario × quantidade."""
        df = spark.createDataFrame(
            [("p1", "TV", 1500.0, 2, None, "SP", 1)], SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(3000.0)

    @pytest.mark.parametrize(
        "valor_unitario, quantidade, esperado",
        [
            (10.0, 2, 20.0),  # caminho feliz
            (500.0, 0, 0.0),  # item devolvido: quantidade zero
            (1500.0, 1, 1500.0),  # unidade única
        ],
    )
    def test_add_valor_total_parametrizado(
        self, spark, valor_unitario, quantidade, esperado
    ):
        """Mesmo cálculo, vários cenários — cada tupla vira um teste independente."""
        df = spark.createDataFrame(
            [("p1", "TV", valor_unitario, quantidade, None, "SP", 1)],
            SCHEMA_PEDIDOS,
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total == pytest.approx(esperado)

    def test_add_valor_total_propaga_nulo(self, spark):
        """NULL em valor_unitario se propaga (aritmética do Spark) — caso especial, fora do parametrize."""
        df = spark.createDataFrame(
            [("p1", "TV", None, 2, None, "SP", 1)], SCHEMA_PEDIDOS
        )
        resultado = Transformation().add_valor_total_pedidos(df)
        assert resultado.collect()[0].valor_total is None


class TestGetTop10Clientes:

    def test_ordena_decrescente_e_limita_a_10(self, spark):
        """Retorna no máximo 10 clientes, do maior para o menor valor_total."""
        dados = [(i, float(i * 100)) for i in range(1, 16)]  # 15 clientes
        df = spark.createDataFrame(dados, SCHEMA_TOTAL)
        linhas = Transformation().get_top_10_clientes(df).collect()
        assert len(linhas) == 10
        assert linhas[0].id_cliente == 15  # maior valor primeiro
        assert linhas[-1].id_cliente == 6  # menor valor último

    def test_soma_pedidos_do_mesmo_cliente(self, spark):
        """Vários pedidos de um cliente devem ser SOMADOS, não contados."""
        df = spark.createDataFrame([(1, 100.0), (1, 200.0), (2, 500.0)], SCHEMA_TOTAL)
        totais = {
            r.id_cliente: r.valor_total
            for r in Transformation().get_top_10_clientes(df).collect()
        }
        assert totais[1] == pytest.approx(300.0)


class TestJoinPedidosClientes:

    def test_inner_join_so_expoe_colunas_do_relatorio(self, spark):
        """Só clientes com pedido entram; o resultado traz apenas id_cliente, nome, email e valor_total."""
        pedidos = spark.createDataFrame([(1, 1500.0)], SCHEMA_TOTAL)
        clientes = spark.createDataFrame(
            [(1, "Ana Lima", "ana@test.com"), (99, "Sem Pedido", "x@test.com")],
            SCHEMA_CLIENTES,
        )
        resultado = Transformation().join_pedidos_clientes(pedidos, clientes)
        assert resultado.count() == 1
        assert set(resultado.columns) == {"id_cliente", "nome", "email", "valor_total"}
        assert resultado.collect()[0].nome == "Ana Lima"
