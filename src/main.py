# src/main.py
from config.settings import carregar_config, configurar_logging
from session.spark_session import SparkSessionManager
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
from pipeline.pipeline import Pipeline
import logging
import sys

def main():
    # 1. Carrega configurações do YAML
    config = carregar_config()

    # 2. Ativa o log instantaneamente
    configurar_logging(config['logging'])

    # Extrai o app_name do dicionário de configuração
    app_name = config['spark']['app_name']

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
    except Exception as e:
        logger.error(f"Erro durante a execução do job: {e}", exc_info=True)
        # Aqui poderíamos adicionar envio de notificação (Slack, Email, PagerDuty)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
