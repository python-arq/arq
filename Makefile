.PHONY: install
install:
	pip install -U pip setuptools
	pip install -e .
	pip install -r tests/requirements.txt

.PHONY: isort
isort:
	isort -rc -w 120 arq
	isort -rc -w 120 tests

.PHONY: lint
lint:
	python setup.py check -rms
	flake8 arq/ tests/
	pytest arq -p no:sugar -q --cache-clear
	mypy --fast-parser --silent-imports arq/

.PHONY: test
test:
	TZ=Asia/Singapore pytest --cov=arq && coverage combine

.PHONY: .test-build-cov
.test-build-cov:
	TZ=Asia/Singapore pytest --cov=arq && (echo "building coverage html"; coverage combine; coverage html)

.PHONY: all
all: .test-build-cov lint

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
	ghp-import -m "update docs" -p docs/_build/html/
