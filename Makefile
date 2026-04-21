install-hooks:
	uv run pre-commit install

develop:
	uv pip install --no-deps -e .

check:
	cargo check

build:
	cargo build --release

lint:
	ruff check .
	ruff format --check .
	mypy daft_h3/

format:
	ruff check --fix .
	ruff format .

test:
	pytest tests/ -v

bench:
	uv run --extra bench python benchmarks/bench_vs_udf.py
