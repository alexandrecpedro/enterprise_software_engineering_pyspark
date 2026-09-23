# src/pipeline/pipeline.py
import logging
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

logger = logging.getLogger(__name__)

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")

        logger.info("Abrindo o dataframe de clientes")
        path_clientes = config["paths"]["clientes"]
        logger.info(f"Obtido o path de clientes: {path_clientes}")
        clientes_df = self.data_handler.load_clientes(path=path_clientes)
        clientes_df.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

        logger.info(f"""
        Obtidos os seguintes parâmetros de pedidos: 
        - path: {path_pedidos}
        - compression: {compression_pedidos}
        - header: {header_pedidos}
        - separator: {separator_pedidos}
        """)

        pedidos_df = self.data_handler.load_pedidos(
            path=path_pedidos,
            compression=compression_pedidos,
            header=header_pedidos,
            sep=separator_pedidos,
        )

        logger.info("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)
        pedidos_df.show(5, truncate=False)

        logger.info(
            "Calculando o valor total de pedidos por cliente e filtrar os 10 maiores"
        )
        top_10_clientes_df = self.transformer.get_top_10_clientes(pedidos_df)

        top_10_clientes_df.show(10, truncate=False)

        logger.info("Fazendo a junção dos dataframes")
        relatorio_top_10_cliente_df = self.transformer.join_pedidos_clientes(
            top_10_clientes_df, clientes_df
        )
        relatorio_top_10_cliente_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        path_output = config["paths"]["output"]
        logger.info(f"Obtido o path de saída: {path_output}")
        self.data_handler.write_parquet(
            df=relatorio_top_10_cliente_df, path=path_output
        )

        logger.info("Pipeline concluído com sucesso!")
