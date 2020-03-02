import requests

request_url = "http://127.0.0.1:9007/run"


def registerPyAction(enableAdmin, params={}):
    datas = {"code": """from pyjava.api.mlsql import PythonContext
for row in context.fetch_once():
    print(row)    
context.build_result([{"content": "{}"}], 1)
    """, "action": "registerPyAction", "codeName": "echo"}
    if enableAdmin:
        datas = {**datas, **{"admin_token": "admin"}}
    r = requests.post(request_url, data={**datas, **params})
    print(r.text)
    print(r.status_code)


# echo
def pyAction(codeName, enableAdmin=True, params={}):
    datas = {"codeName": codeName, "action": "pyAction"}
    if enableAdmin:
        datas = {**datas, **{"admin_token": "admin"}}
    r = requests.post(request_url, data={**datas, **params})
    print(r.text)
    print(r.status_code)


def loadDB(db):
    datas = {"name": db, "action": "loadDB"}
    r = requests.post(request_url, data=datas)
    print(r.text)
    print(r.status_code)


def http(action, params):
    datas = {**{"action": action}, **params}
    r = requests.post(request_url, data=datas)
    return r


def controlReg():
    r = http("controlReg", {"enable": "true"})
    print(r.text)
    print(r.status_code)


def printRespose(r):
    print(r.text)
    print(r.status_code)


def test_db_config(db, user, password):
    return """
        {}:
              host: 127.0.0.1
              port: 3306
              database: {}
              username: {}
              password: {}
              initialSize: 8
              disable: true
              removeAbandoned: true
              testWhileIdle: true
              removeAbandonedTimeout: 30
              maxWait: 100
              filters: stat,log4j
        """.format(db, db, user, password)


def addDB(instanceName, db, user, password):
    # user-system
    datas = {"dbName": db, "instanceName": instanceName,
             "dbConfig": test_db_config(db, user, password), "admin_token": "admin"}
    r = http(addDB.__name__, datas)
    printRespose(r)


def userReg(name, password):
    r = http(userReg.__name__, {"userName": name, "password": password})
    printRespose(r)


def users():
    r = http(users.__name__, {})
    printRespose(r)


def uploadPlugin(file_path, data):
    values = {**data, **{"action": "uploadPlugin"}}
    files = {file_path.split("/")[-1]: open(file_path, 'rb')}
    r = requests.post(request_url, files=files, data=values)
    printRespose(r)


def addProxy(name, value):
    r = http(addProxy.__name__, {"name": name, "value": value})
    printRespose(r)


import json


def userLogin(userName, password):
    r = http(userLogin.__name__, {"userName": userName, "password": password})
    return json.loads(r.text)[0]['token']


def enablePythonAdmin(userName, token):
    r = http("pyAuthAction",
             {"userName": userName, "access-token": token,
              "resourceType": "admin",
              "resourceName": "admin", "admin_token": "admin", "authUser": "jack"})
    return r


def enablePythonRegister(userName, token):
    r = http("pyAuthAction",
             {"userName": userName, "access-token": token,
              "resourceType": "action",
              "resourceName": "registerPyAction", "authUser": "william"})
    return r


def enablePythonExecute(userName, token, codeName):
    r = http("pyAuthAction",
             {"userName": userName, "access-token": token,
              "resourceType": "custom",
              "resourceName": codeName, "authUser": "william"})
    return r


# userReg()
# users()
# addDB("ar_plugin_repo")
# pyAction("echo")
# addProxy("user-system", "http://127.0.0.1:9007/run")
# uploadPlugin("/Users/allwefantasy/CSDNWorkSpace/user-system/release/user-system-bin_2.11-1.0.0.jar",
#              {"name": "jack", "password": "123", "pluginName": "user-system"})

# addDB("user-system", "mlsql_python_predictor", "root", "mlsql")
# userReg("william", "mm")
# registerPyAction(False)
# pyAction("echo", False)
token = userLogin("jack", "123")
# r = enablePythonAdmin("jack", token)
# r = enablePythonExecute("jack", token, "echo")
# printRespose(r)

token = userLogin("william", "mm")
# registerPyAction(False, {"userName": "william", "access-token": token})
# enablePythonRegister("jack", token)
pyAction("echo", False, {"userName": "william", "access-token": token})
