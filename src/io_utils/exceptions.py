# src/io_utils/exceptions.py


class DataHandlerException(Exception):
    """Exceção base para qualquer falha na camada de I/O."""



class LoadPedidosException(DataHandlerException):
    """Lançada especificamente ao falhar o carregamento do dataset de pedidos."""

