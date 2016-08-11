#!/usr/bin/env bash
isort_result=$(isort -rc -w 120 --diff arq)
if [[ $isort_result == *"arq"* ]] ; then
    printf "changes:\n $isort_result\n\nisort indicates there's an import order problem\n"
    exit 1
fi
exit 0
