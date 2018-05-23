# -*- coding: UTF-8 -*-
import requests
from pprint import pprint as pp
import json
import re
import datetime

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class LookerApi(object):

    def __init__(self, token, secret, host, port):

        self.token = token
        self.secret = secret
        self.host = host
        self.port = port

        self.session = requests.Session()
        self.session.verify = False

        self.auth()

    def auth(self):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'login')
        params = {'client_id':self.token,
                  'client_secret':self.secret
                  }
        r = self.session.post(url,params=params)
        access_token = r.json().get('access_token')
        self.session.headers.update({'Authorization': 'token {}'.format(access_token)})
        if r.status_code == requests.codes.ok:
            print('Successfully authenticated.')
        else:
            print('Authentication failed.')

# GET /lookml_models/
    def get_models(self,fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'lookml_models')
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /lookml_models/{{NAME}}
    def get_model(self,model_name=None,fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}'.format(self.host,self.port,'lookml_models', model_name)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /lookml_models/{{NAME}}/explores/{{NAME}}
    def get_explore(self,model_name=None,explore_name=None,fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}/{}'.format(self.host,self.port,'lookml_models', model_name, 'explores', explore_name)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects
    def get_projects(self,fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,self.port,'projects')
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects/{project_id}
    def get_project(self,project=None,fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host,self.port,'projects', project)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects/{project_id}/files
    def get_project_files(self,project=None,fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}'.format(self.host,self.port,'projects', project, 'files')
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# POST /queries/run/{result_format}
    def run_inline_query(self, result_format, body):
        url = 'https://{}:{}/api/3.0/{}/{}/{}'.format(self.host, self.port, 'queries', 'run', result_format)
        r = self.session.post(url, json.dumps(body))

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()
