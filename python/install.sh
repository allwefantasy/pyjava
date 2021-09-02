project=pyjava
version=0.3.1
rm -rf ./dist/* && pip uninstall -y ${project} && python setup.py sdist bdist_wheel && cd ./dist/ && pip install ${project}-${version}-py3-none-any.whl && cd -