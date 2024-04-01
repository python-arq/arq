.DEFAULT_GOAL := all
sources = arq tests

.PHONY: install
install:
	pip install -U pip pre-commit pip-tools
	pip install -r requirements/all.txt
	pip install -e .[watch]
	pre-commit install

.PHONY: refresh-lockfiles
refresh-lockfiles:
	find requirements/ -name '*.txt' ! -name 'all.txt' -type f -delete
	make update-lockfiles

.PHONY: update-lockfiles
update-lockfiles:
	@echo "Updating requirements/*.txt files using pip-compile"
	pip-compile -q --strip-extras -o requirements/linting.txt requirements/linting.in
	pip-compile -q --strip-extras -o requirements/testing.txt requirements/testing.in
	pip-compile -q --strip-extras -o requirements/docs.txt requirements/docs.in
	pip-compile -q --strip-extras -o requirements/pyproject.txt pyproject.toml --all-extras
	pip install --dry-run -r requirements/all.txt

.PHONY: format
format:
	ruff check --fix $(sources)
	ruff format $(sources)

.PHONY: lint
lint:
	ruff check $(sources)
	ruff format --check $(sources)

.PHONY: test
test:
	coverage run -m pytest

.PHONY: testcov
testcov: test
	@echo "building coverage html"
	@coverage html

.PHONY: mypy
mypy:
	mypy arq

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
