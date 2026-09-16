# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade) ao DataFrame de pedidos."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def get_top_10_clientes(self, pedidos_df: DataFrame) -> DataFrame:
        """Calcula o valor total de pedidos por cliente e retorna os 10 maiores."""
        return (
            pedidos_df.groupBy("id_cliente")
            .agg(F.sum("valor_total").alias("valor_total"))
            .orderBy(F.desc("valor_total"))
            .limit(10)
        )

    def join_pedidos_clientes(
        self, pedidos_df: DataFrame, clientes_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e clientes."""
        return pedidos_df.join(
            clientes_df, clientes_df.id == pedidos_df.id_cliente, "inner"
        ).select(
            pedidos_df.id_cliente,
            clientes_df.nome,
            clientes_df.email,
            pedidos_df.valor_total,
        )
