.DEFAULT_GOAL := all
sources = arq tests

.PHONY: .uv  ## Check that uv is installed
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: install
install:
	uv sync --frozen --all-groups --all-packages --all-extras
	uv pip install pre-commit
	uv run pre-commit install

.PHONY: format
format:
	uv run ruff check --fix $(sources)
	uv run ruff format $(sources)

.PHONY: lint
lint:
	uv run ruff check $(sources)
	uv run ruff format --check $(sources)

.PHONY: test
test:
	uv run coverage run -m pytest

.PHONY: testcov
testcov: test
	@echo "building coverage html"
	@coverage html

.PHONY: mypy
mypy:
	uv run mypy arq

.PHONY: all
all: lint mypy testcov

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	make -C docs clean

.PHONY: docs
docs:
	make -C docs html
	rm -rf docs/_build/html/old
	unzip -q docs/old-docs.zip
	mv old-docs docs/_build/html/old
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: publish-docs
publish-docs:
	cd docs/_build/ && cp -r html site && zip -r site.zip site
	@curl -H "Content-Type: application/zip" -H "Authorization: Bearer ${NETLIFY}" \
			--data-binary "@docs/_build/site.zip" https://api.netlify.com/api/v1/sites/arq-docs.netlify.com/deploys
