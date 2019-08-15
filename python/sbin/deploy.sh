#!/usr/bin/env bash
pip uninstall -y pyjava && python setup.py sdist bdist_wheel && cd ./dist/ && pip install pyjava-0.1.0-py3-none-any.whl && cd -
twine upload dist/*