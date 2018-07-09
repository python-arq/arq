.DEFAULT_GOAL := all

.PHONY: install
install:
	pip install -U pip setuptools
	pip install -r requirements.txt
	pip install -e .

.PHONY: isort
isort:
	isort -rc -w 120 arq
	isort -rc -w 120 tests

.PHONY: lint
lint:
	python setup.py check -rms
	flake8 arq/ tests/
	pytest arq -p no:sugar -q
	mypy --ignore-missing-imports --warn-unused-ignores arq/

.PHONY: test
test:
	TZ=Asia/Singapore pytest --cov=arq && coverage combine

.PHONY: testcov
testcov:
	TZ=Asia/Singapore pytest --cov=arq && (echo "building coverage html"; coverage combine; coverage html)

.PHONY: all
all: testcov lint

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -rf .cache
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
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: deploy-docs
deploy-docs: docs
	cd docs/_build/ && cp -r html site && zip -r site.zip site
	@curl -H "Content-Type: application/zip" -H "Authorization: Bearer ${NETLIFY}" \
			--data-binary "@docs/_build/site.zip" https://api.netlify.com/api/v1/sites/arq-docs.netlify.com/deploys
