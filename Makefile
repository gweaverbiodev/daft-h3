install-hooks:
	uv run pre-commit install

lint:
	ruff check .
	ruff format --check .
	mypy daft_h3/

format:
	ruff check --fix .
	ruff format .

test:
	pytest tests/ -v
