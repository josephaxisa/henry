import yaml ### install the pyyaml package
import json
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from collections import OrderedDict

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

# host name in config.yml
host = 'sandbox'
# model that you wish to analyze
model_name = 'trace_surfing'

### ------- OPEN THE CONFIG FILE and INSTANTIATE API -------
f = open('config.yml')
params = yaml.load(f)
f.close()

my_host = params['hosts'][host]['host']
my_secret = params['hosts'][host]['secret']
my_token = params['hosts'][host]['token']

looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

# print('Getting fields in '+model_name+'...')

def get_explores(model):
    print('Getting model ' + model_name)
    model = looker.get_model(model)
    explore_names = [i['name'] for i in model['explores']]
    explores = [looker.get_explore(model_name, i) for i in explore_names]
    pprint(explores)
    return(explores)


def get_fields(model):
    schema = []
    for explore in get_explores(model):
        fields = []
        explore_name = explore['name']
        fields.append([dimension['name'] for dimension in explore['fields']['dimensions']])
        fields.append([measure['name'] for measure in explore['fields']['measures']])
        fields.append([filter['name'] for filter in explore['fields']['filters']])
        schema.append(
        {'explore': explore_name,
         'fields':fields
         # 'dimensions': dimensions,
         # 'measures' : measures,
         # 'fitlers' : filters
        }
        )
    # pprint(schema)
get_fields(model_name)
