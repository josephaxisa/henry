import yaml ### install the pyyaml package
import json
from lookerapi import LookerApi
from datetime import datetime
from pprint import pprint
from collections import defaultdict
from itertools import groupby

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

# host name in config.yml
host = 'master'
# model that you wish to analyze
model_name = 'dcl_tiny_dashboard'
# How far you wish to look back
timeframe = '28 days'

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

print('Getting fields in '+model_name+'...')

model = looker.get_model(model_name)
explore_names = [i['name'] for i in model['explores']]

explore = [looker.get_explore(model_name, i) for i in explore_names]
