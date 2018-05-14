import yaml ### install the pyyaml package
import json
from lookerapi import LookerApi
from pprint import pprint
from collections import defaultdict
from itertools import groupby
import pandas as pd
import re

### ------- HERE ARE PARAMETERS TO CONFIGURE -------

# host name in config.yml
host = 'mylooker'
# model that you wish to analyze
modelName = 'ML, postgres, i__looker'
# How far you wish to look back
timeframe = '90 days'

def main():
    my_host, my_token, my_secret = get_api_creds()

    looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)

    response = get_fields_usage(looker, modelName, timeframe)
    print(json.dumps(response))
    print(format(response))

def get_fields_usage(looker, modelName, timeframe):
    body={
        "model":"i__looker",
        "view":"history",
        "fields":["query.model","query.view","query.formatted_fields","query.formatted_filters","query.sorts","query.formatted_pivots","history.query_run_count"],
        "filters":{"history.created_date":timeframe,"query.model":modelName},
        "limit":"50000"
    }

    response = looker.run_inline_query("json", body)

    return response

    # print('Getting fields in '+model_name+'...')
    #
    # model = looker.get_model(model_name)
    #
    # explore_names = [i['name'] for i in model['explores']]
    #
    # explore = [looker.get_explore(model_name, i) for i in explore_names]
def format(json):
    dd = dict.fromkeys('fields', [])
    result = defaultdict(lambda: dd)

    for i in json:
        #print(i)
        d = [(m.group(1), m.group(2)) for view, field in i['query.formatted_fields']]
        #result[i['query.model']]['view'].append(i['query.view'])

    return result

def get_api_creds():
    f = open('config.yml')
    params = yaml.load(f)
    f.close()

    my_host = params['hosts'][host]['host']
    my_secret = params['hosts'][host]['secret'] # client_secret
    my_token = params['hosts'][host]['token']  # client_id

    return my_host, my_token, my_secret

if __name__ == "__main__":
    main()
