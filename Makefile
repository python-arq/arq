install:
	pip install -U pip
	pip install -e .
	pip install -Ur tests/requirements.txt

isort:
	isort -rc -w 120 arq
	isort -rc -w 120 tests


lint:
	python setup.py check -rms
	flake8 arq/ tests/
	./tests/isort_test.sh

test:
	py.test --cov=arq && coverage combine

.test-build-cov:
	py.test --cov=arq && (echo "building coverage html"; coverage combine; coverage html)

all: .test-build-cov lint

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

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

.PHONY: install isort lint test .test-build-cov all clean doc
