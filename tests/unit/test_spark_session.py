# tests/unit/test_spark_session.py
from pyspark.sql import SparkSession

from session.spark_session import SparkSessionManager


class TestSparkSessionManager:

    def test_retorna_instancia_de_spark_session(self, spark):
        sessao = SparkSessionManager.get_spark_session(app_name="test-contrato")
        assert isinstance(sessao, SparkSession)

    def test_getorcreate_reutiliza_a_mesma_sessao(self, spark):
        """Chamadas subsequentes devem devolver a MESMA instância (Singleton via getOrCreate)."""
        sessao_a = SparkSessionManager.get_spark_session(app_name="test-a")
        sessao_b = SparkSessionManager.get_spark_session(app_name="test-b")
        assert sessao_a is sessao_b