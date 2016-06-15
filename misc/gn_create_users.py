"""
Demonstration of how to create a user in GeoNetwork 3+ via scripting. This could be expanded to read from a CSV of names and emails.
"""
import requests
import json


USERNAME = raw_input("Username: ")
PASSWORD = raw_input("Password: ")

BASE_URL = "https://geonet.lib.umn.edu/geonetwork/srv/eng/"
LOGIN = "https://geonet.lib.umn.edu/geonetwork/j_spring_security_check"
LOGOUT = "https://geonet.lib.umn.edu/geonetwork/j_spring_security_logout"
CREATE_USER = "admin.user.update"
OUT_JSON = "?_content_type=json"

session = requests.Session()
session.auth =(USERNAME, PASSWORD)
login_r = session.post(LOGIN, auth=(USERNAME, PASSWORD))

# Dummy input for demonstration purposes. Could replace with CSV input, where each user is a row
user = {
    "id":"",
    "operation":"newuser",
    "username":"foobar",
    "password":"foobar",
    "password2":"foobar",
    "name":"foo",
    "surname":"bar",
    "email":"foo@bar",
    "org":"",
    "address":"",
    "zip":"",
    "state":"",
    "city":"",
    "country":"",
    "profile":"RegisteredUser",
    "enabled":"true"
}

headers = {
    'content-type': 'application/json'
}

create_user_r = session.post(BASE_URL + CREATE_USER + OUT_JSON, params=user,headers=headers)
print(create_user_r.text)

logout_r = session.post(LOGOUT)
