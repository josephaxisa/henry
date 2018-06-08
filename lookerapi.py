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
        params = {'client_id': self.token,
                  'client_secret': self.secret
                  }
        r = self.session.post(url, params=params, timeout=60)
        access_token = r.json().get('access_token')
        self.session.headers.update({'Authorization': 'token {}'.format(access_token)})
        if r.status_code != requests.codes.ok:
            print('Authentication failed.')

        return

# GET /lookml_models/
    def get_models(self, fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'lookml_models')
        params = fields
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /lookml_models/{{NAME}}
    def get_model(self, model_name=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}'.format(self.host, self.port,  'lookml_models', model_name)
        params = fields
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /lookml_models/{{NAME}}/explores/{{NAME}}
    def get_explore(self, model_name=None, explore_name=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}/{}'.format(self.host, self.port, 'lookml_models', model_name, 'explores', explore_name)
        params = fields
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects
    def get_projects(self, fields={}):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'projects')
        params = fields
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects/{project_id}
    def get_project(self, project=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}'.format(self.host, self.port, 'projects', project)
        params = fields
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# GET /projects/{project_id}/files
    def get_project_files(self, project=None, fields={}):
        url = 'https://{}:{}/api/3.0/{}/{}/{}'.format(self.host, self.port, 'projects', project, 'files')
        params = fields
        r = self.session.get(url, params=params, timeout=60)
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
        r = self.session.post(url, json.dumps(body), timeout=60)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            error_message = str(r.status_code) + ': Call to ' + url + ' failed at ' + str(datetime.datetime.utcnow())
            print(error_message)
            f = open('api_errors.txt', 'a+')
            f.write(error_message)
            f.close()

# PATCH session
    def update_session(self, mode):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'session')
        body = { 'workspace_id' : str(mode)}

        r = self.session.patch(url, json=body)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

        return

# GET session
    def get_session(self):
        url = 'https://{}:{}/api/3.0/{}'.format(self.host, self.port, 'session')

        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

        return

# GET /projects/{project_id}/git_connection_tests
    def git_connection_tests(self, project_id):
        url = ('https://{}:{}/api/3.0/projects/{}'
               '/git_connection_tests').format(self.host, self.port, project_id)
        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

        return

# GET /projects/{project_id}/git_connection_tests/{test_id}
    def run_git_connection_test(self, project_id, test_id):
        url = ('https://{}:{}/api/3.0/projects/{}'
               '/git_connection_tests/{}').format(self.host, self.port, project_id, test_id)

        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

        return


# GET /connections
    def get_connections(self):
        url = 'https://{}:{}/api/3.0/connections'.format(self.host, self.port)

        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

        return


# PUT /connections/{connection_name}/test
    def test_connection(self, connection_name):
        url = 'https://{}:{}/api/3.0/connections/{}/test'.format(self.host, self.port, connection_name)

        r = self.session.put(url)

        if r.status_code == requests.codes.ok:
            return 'Pass'
        else:
            return r.status_code

# GET /legacy_features
    def get_legacy_features(self):
        url = 'https://{}:{}/api/3.0/legacy_features'.format(self.host, self.port)

        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /integrations
    def get_integrations(self):
        url = 'https://{}:{}/api/3.0/integrations'.format(self.host, self.port)
        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /versions
    def get_version(self):
        url = 'https://{}:{}/api/3.0/versions'.format(self.host, self.port)
        r = self.session.get(url)

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code
