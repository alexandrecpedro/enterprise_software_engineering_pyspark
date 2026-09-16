# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession compartilhada por toda a suíte de testes.

    scope="session" garante que a sessão seja criada uma única vez e
    reutilizada, evitando o overhead de inicialização do Spark em cada teste.
    """
    session = (
        SparkSession.builder
        .appName("test-pipeline-session")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()