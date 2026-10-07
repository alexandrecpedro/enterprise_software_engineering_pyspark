.PHONY: help install format lint test coverage build run clean check

PYTHON := python3
PIP := pip

help:
	@echo "Comandos disponíveis no projeto:"
	@echo "  make install   - Instala as dependências a partir do requirements.txt"
	@echo "  make format    - Formata o código-fonte com black"
	@echo "  make lint      - Verifica conformidade de código (black + ruff)"
	@echo "  make test      - Executa a suíte de testes com pytest"
	@echo "  make coverage  - Executa testes e exibe relatório de cobertura no terminal"
	@echo "  make build     - Compila o pacote distribuível (.whl)"
	@echo "  make run       - Executa o pipeline via spark-submit"
	@echo "  make clean     - Remove artefatos de compilação, caches e diretórios temporários"
	@echo "  make check     - Executa validação completa de qualidade (lint + test)"

install:
	$(PIP) install -r requirements.txt

format:
	black .

lint:
	black --check .
	ruff check .

test:
	pytest

coverage:
	pytest --cov=data_engineering_pyspark --cov-report=term-missing

build:
	$(PYTHON) -m build

run:
	spark-submit --master "local[*]" \
		--py-files dist/data_engineering_pyspark-0.1.0-py3-none-any.whl \
		--files config/settings.yaml \
		src/main.py

clean:
	rm -rf dist build *.egg-info .pytest_cache .ruff_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf data/output/* ../.coverage

check: lint test