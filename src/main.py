# src/main.py
import logging
import sys

from pyspark.errors import PySparkException

from data_engineering_pyspark.config.settings import carregar_config, configurar_logging
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import DataHandlerException, LoadPedidosException
from data_engineering_pyspark.pipeline.pipeline import Pipeline
from data_engineering_pyspark.processing.transformations import Transformation
from data_engineering_pyspark.session.spark_session import SparkSessionManager


def main():
    # 1. Carrega configurações do YAML
    config = carregar_config()

    # 2. Ativa o log instantaneamente
    configurar_logging(config["logging"])

    # Extrai o app_name do dicionário de configuração
    app_name = config["spark"]["app_name"]

    # 3. Inicia a aplicação
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando job: {config['spark']['app_name']}")

    spark = None  # Inicializa como None para segurança no finally
    try:
        # Raiz de Composição (Composition Root):
        # este é o ÚNICO lugar que monta as dependências concretas e as injeta.
        spark = SparkSessionManager.get_spark_session(app_name=app_name)
        data_handler = DataHandler(spark)
        transformer = Transformation()
        pipeline = Pipeline(data_handler, transformer)

        pipeline.run(config=config)

        logger.info("Pipeline finalizado com sucesso.")

    except LoadPedidosException as e:
        # 1. Tratamento específico para o dataset crítico de pedidos
        logger.exception(f"Falha no carregamento de pedidos: {e}")
        # Exemplo de possível tratamento
        # pipeline.run(fallback=True)
        sys.exit(1)

    except DataHandlerException as e:
        # 2. Tratamento genérico para qualquer outra falha de I/O
        logger.exception(f"Erro na camada de leitura/escrita de dados: {e}")
        sys.exit(1)

    except PySparkException as e:
        # 3. Falhas do Spark ocorridas fora da leitura (ex: ações nas transformações)
        logger.exception(
            f"Erro originado no PySpark [Classe: {e.getErrorClass()}]: {e}"
        )
        sys.exit(1)

    except Exception as e:
        logger.exception(f"Erro inesperado durante a execução do job: {e}")
        # Aqui poderíamos adicionar envio de notificação (Slack, Email, PagerDuty)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session encerrada.")


if __name__ == "__main__":
    main()
