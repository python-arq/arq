.DEFAULT_GOAL := all
isort = isort arq tests
black = black -S -l 120 --target-version py37 arq tests

.PHONY: install
install:
	pip install -U pip setuptools
	pip install -r requirements.txt
	pip install -e .[watch]

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	flake8 arq/ tests/
	$(isort) --check-only --df
	$(black) --check

.PHONY: test
test:
	pytest --cov=arq

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
	python setup.py clean

.PHONY: docs
docs:
	make -C docs html
	rm -rf docs/_build/html/old
	unzip -q docs/old-docs.zip
	mv old-docs docs/_build/html/old
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: publish-docs
publish-docs: docs
	cd docs/_build/ && cp -r html site && zip -r site.zip site
	@curl -H "Content-Type: application/zip" -H "Authorization: Bearer ${NETLIFY}" \
			--data-binary "@docs/_build/site.zip" https://api.netlify.com/api/v1/sites/arq-docs.netlify.com/deploys
