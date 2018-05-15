# -*- coding: UTF-8 -*-
import requests
from pprint import pprint as pp
import json
import re

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class LookerApi(object):

    def __init__(self, token, secret, host):

        self.token = token
        self.secret = secret
        self.host = host

        self.session = requests.Session()
        self.session.verify = False

        self.auth()

    def auth(self):
        url = '{}{}'.format(self.host,'login')
        params = {'client_id':self.token,
                  'client_secret':self.secret
                  }
        r = self.session.post(url,params=params)
        access_token = r.json().get('access_token')
        # print(access_token
        self.session.headers.update({'Authorization': 'token {}'.format(access_token)})

    def auth_user(self, user_id):
        url = '{}{}/{}'.format(self.host,'login',user_id)
        params = {'client_id':self.token,
                  'client_secret':self.secret
                  }
        r = self.session.post(url,params=params)
        access_token = r.json().get('access_token')
        print(access_token)
        self.session.headers.update({'Authorization': 'token {}'.format(access_token)})

# POST /dashboards/{dashboard_id}/prefetch
    def create_prefetch(self, dashboard_id, ttl):
        url = '{}{}/{}/prefetch'.format(self.host,'dashboards',dashboard_id)
        params = json.dumps({'ttl':ttl,
                  })
        print(url)
        print(params)
        r = self.session.post(url,data=params)
        pp(r.request.url)
        pp(r.request.body)
        pp(r.json())

# PATCH
    def update_dashboard(self, dashboard_id):
        url = '{}{}/{}'.format(self.host,'dashboards',dashboard_id)
        params = json.dumps({'load_configuration':'prefetch_cache_run'
                  })
        print(url)
        print(params)
        r = self.session.patch(url,data=params)
        pp(r.request.url)
        pp(r.request.body)
        pp(r.json())

    def get_look_info(self,look_id,fields=''):
        url = '{}{}/{}'.format(self.host,'looks',look_id)
        print(url)
        params = {"fields":fields}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code
# GET /queries/
    def get_query(self,query_id):
        url = '{}{}/{}'.format(self.host,'queries',query_id)
        print(url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

    # POST /queries/
    def create_query(self,query_body, fields):
        url = '{}{}'.format(self.host,'queries')
        print(url)
        params = json.dumps(query_body)
        print(" --- creating query --- ")
        r = self.session.post(url,data=params, params = json.dumps({"fields": fields}))
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /looks/<look_id>/run/<format>
    def get_look(self,look_id, format='json', limit=500):
        url = '{}{}/{}/run/{}'.format(self.host,'looks',look_id, format)
        print(url)
        params = {limit:100000}
        r = self.session.get(url,params=params, stream=True)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

    def download_look(self,look_id, format='xlsx'):
        url = '{}{}/{}/run/{}'.format(self.host,'looks',look_id, format)
        params = {}
        r = self.session.get(url,params=params, stream=True)
        print(r.status_code)
        if r.status_code == requests.codes.ok:
            image_name = 'test2.xlsx'
            with open(image_name, 'wb') as f:
                for chunk in r:
                    f.write(chunk)
        else:
            print(r.json())
        return 'done'

    def create_look(self,look_body):
        url = '{}{}'.format(self.host,'looks')
        print(url)
        params = json.dumps(look_body)
        r = self.session.post(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /users
    def get_all_users(self):
        url = '{}{}'.format(self.host,'users')
        # print("Grabbing Users " + url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /users/id
    def get_user(self,id=""):
        url = '{}{}{}'.format(self.host,'users/',id)
        # print("Grabbing User(s) " + str(id))
        # print(url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# PATCH /users/id
    def update_user(self,id="",body={}):
        url = '{}{}{}'.format(self.host,'users/',id)
        # print("Grabbing User(s) " + str(id))
        print(url)
        params = json.dumps(body)
        r = self.session.patch(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code


# GET /user
    def get_current_user(self):
        url = '{}{}'.format(self.host,'user')
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# PUT /users/{user_id}/roles
    def set_user_role(self,id="", body={}):
        url = '{}{}{}{}'.format(self.host,'users/',id,'/roles')
        # print("Grabbing User(s) " + str(id))
        # print(url)
        params = json.dumps(body)
        r = self.session.post(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /users/{user_id}/roles
    def get_user_role(self,id=""):
        url = '{}{}{}{}'.format(self.host,'users/',id,'/roles')
        # print("Grabbing User(s) " + str(id))
        # print(url)
        r = self.session.get(url,params={})
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

    def get_roles(self):
        url = '{}{}'.format(self.host,'roles')
        # print("Grabbing role(s) ")
        # print(url)
        r = self.session.get(url,params={})
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code


# PATCH /users/{user_id}/access_filters/{access_filter_id}
    def update_access_filter(self, user_id = 0, access_filter_id = 0, body={}):
        url = '{}{}/{}/{}/{}'.format(self.host,'users',user_id,'access_filters',access_filter_id)
        params = json.dumps(body)
        r = self.session.patch(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

    def create_access_filter(self, user_id = 0, body={}):
        url = '{}{}/{}/{}'.format(self.host,'users',user_id,'access_filters')
        params = json.dumps(body)
        r = self.session.post(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code


# GET /users/me
    def get_me(self):
        url = '{}{}'.format(self.host,'user')
        print("Grabbing Myself: " + url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /lookml_models/
    def get_models(self,fields={}):
        url = '{}{}'.format(self.host,'lookml_models')
        # print(url)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code
# GET /lookml_models/{{NAME}}
    def get_model(self,model_name="",fields={}):
        url = '{}{}/{}'.format(self.host,'lookml_models', model_name)
        print(url)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /lookml_models/{{NAME}}/explores/{{NAME}}
    def get_explore(self,model_name=None,explore_name=None,fields={}):
        url = '{}{}/{}/{}/{}'.format(self.host,'lookml_models', model_name, 'explores', explore_name)
        # print(url)
        params = fields
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

#GET /scheduled_plans/dashboard/{dashboard_id}
    def get_dashboard_schedule(self,dashboard_id=0):
        url = '{}{}/{}/{}'.format(self.host,'scheduled_plans', 'dashboard',  dashboard_id)
        # print(url)
        r = self.session.get(url)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code


#GET /scheduled_plans
    def get_all_schedules(self, all_users=False):
        url = '{}{}'.format(self.host,'scheduled_plans')
        # print(url)
        params = json.dumps({'all_users':all_users})
        r = self.session.get(url)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

#GET /scheduled_plans/look/{dashboard_id}
    def get_look_schedule(self,look_id=0):
        url = '{}{}/{}/{}'.format(self.host,'scheduled_plans', 'look',  look_id)
        # print(url)
        r = self.session.get(url)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code



#PATCH /scheduled_plans/{scheduled_plan_id}
    def update_schedule(self, plan_id, body={}):
        url = '{}{}/{}'.format(self.host,'scheduled_plans',plan_id)
        params = json.dumps(body)
        # print(url)
        # print(params)
        r = self.session.patch(url,data=params)
        # pp(r.request.url)
        # pp(r.request.body)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code


#DELETE /looks/{look_id}
    def delete_look(self,look_id,fields=''):
        url = '{}{}/{}'.format(self.host,'looks',look_id)
        print(url)
        params = {"fields":fields}
        r = self.session.delete(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

#DELETE /dashboards/{dashboard_id}
    def delete_dashboard(self,dashboard_id,fields=''):
        url = '{}{}/{}'.format(self.host,'dashboards',dashboard_id)
        print(url)
        params = {"fields":fields}
        r = self.session.delete(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

#GET  /user_attributes/{user_attribute_id}/group_values
    def get_user_attribute_group_values(self,user_attribute_id):
        url = '{}{}/{}/{}'.format(self.host,'user_attributes',user_attribute_id,'group_values')
        print(url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

#GET /groups/{group_id}/users
    def get_group_users(self,group_id):
        url = '{}{}/{}/{}'.format(self.host,'groups',group_id,'users')
        print(url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /spaces/{space_id}/dashboards
    def get_space_dashboards(self,space_id):
        url = '{}{}/{}/{}'.format(self.host,'spaces',space_id,'dashboards')
        print(url)
        params = {}
        r = self.session.get(url,params=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# POST  /groups/{group_id}/users
    def add_users_to_group(self,group_id,user_id):
        url = '{}{}/{}/{}'.format(self.host,'groups',group_id,'users')
        print(url)
        params = json.dumps({'user_id': user_id})
        r = self.session.post(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code
# POST /scheduled_plans
    def create_scheduled_plan(self,body):
        url = '{}{}'.format(self.host,'scheduled_plans')
        print(url)
        params = body
        r = self.session.post(url,data=params)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# GET /scheduled_plans/space/{space_id}
    def get_scheduled_plans(self,space_id):
        url = '{}{}/{}/{}'.format(self.host,'scheduled_plans', 'space',  space_id)
        # print(url)
        r = self.session.get(url)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code

# POST /queries/run/{result_format}
    def run_inline_query(self, result_format, body):
        url = '{}{}/{}/{}'.format(self.host, "queries", 'run', result_format)
        r = self.session.post(url, json.dumps(body))

        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            return r.status_code
