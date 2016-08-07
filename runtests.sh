#!/usr/bin/env bash
py.test --cov=arq
pytest=$?
if [ $pytest == 0 ] ; then
    echo building coverage html
    coverage combine
    coverage html
fi
./tests/isort_test.sh
isort=$?
echo "pytest exit code: ${pytest}"
echo "isort exit code:  ${isort}"
flake8 arq/ tests/
flake=$?
echo "flake8 exit code: ${flake}"
exit $((${pytest} + ${flake} + ${isort}))
