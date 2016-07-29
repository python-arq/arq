#!/usr/bin/env bash
py.test --cov=arq
pytest=$?
if [ $pytest == 0 ] ; then
    echo building coverage html
    coverage combine
    coverage html
fi
echo "pytest exit code: ${pytest}"
flake8 arq/ tests/
flake=$?
echo "flake8 exit code: ${flake}"
exit $((${flake} + ${pytest}))
