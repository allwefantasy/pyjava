#!/usr/bin/env bash

version=${1:-0.3.3}

quoteVersion=$(cat python/pyjava/version.py|grep "__version__" |awk -F'=' '{print $2}'| xargs )

if [[ "${version}" != "${quoteVersion}" ]];then
   echo "version[${quoteVersion}] in python/pyjava/version.py is not match with version[${version}] you specified"
   exit 1
fi

if [[ ! -d '.repo' ]];then
   echo "Make sure this script executed in root directory of pyjava"
   exit 1
fi

echo "deploy pyjava jar based on spark243...."
mlsql_plugin_tool spark243
mvn clean deploy -DskipTests -Pdisable-java8-doclint -Prelease-sign-artifacts

echo "deploy pyjava jar based on spark311...."
mlsql_plugin_tool spark311
mvn clean deploy -DskipTests -Pdisable-java8-doclint -Prelease-sign-artifacts

echo "deploy pyjava pip...."
cd python
rm -rf dist
pip uninstall -y pyjava && python setup.py sdist bdist_wheel && cd ./dist/ && pip install pyjava-${version}-py3-none-any.whl && cd -
twine upload dist/*