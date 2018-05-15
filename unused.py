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
# host = 'lookerv54'
# model that you wish to analyze
model_names = 'ML, postgres, i__looker'
# model_names = 'jax_adventureworks, thelook'
# How far you wish to look back
timeframe = '90 days'



def main():
    my_host, my_token, my_secret = get_api_creds()

    looker = LookerApi(host=my_host,
                 token=my_token,
                 secret = my_secret)
    # schema_builder(looker, 'thelook, jax_adventureworks')
    # response = get_fields_usage(looker, model_names, timeframe)
    # print(json.dumps(response))
    # print(format(response))

    # get_fields_usage(looker, model_names, timeframe)


def get_explores(looker, model_names):
    explores = []
    for model in model_names.replace(' ','').split(','):
        model_body = looker.get_model(model)
        explore_names = [explore['name'] for explore in model_body['explores']]
        [explores.append(looker.get_explore(model, explore)) for explore in explore_names]
    return(explores)

def get_fields(looker, model_names):
    fields =[]
    for explore in get_explores(looker, model_names):
        [fields.append(dimension['name']) for dimension in explore['fields']['dimensions']]
        [fields.append(measure['name']) for measure in explore['fields']['measures']]
    distinct_fields = sorted(set(fields))
    return(fields)

def schema_builder(looker, model_names):
    schema = []
    distinct_fields = sorted(set(get_fields(looker, model_names)))
    view_field_pairs = [field.split('.') for field in distinct_fields]
    for key, group in groupby(view_field_pairs, lambda x:x[0]):
        schema.append({"view": key,
        "fields": [i[1] for i in list(group)]
        })
    pprint(schema)

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

# def format(json):
#     dd = dict.fromkeys('fields', [])
#     result = defaultdict(lambda: dd)
#
#     for i in json:
#         #print(i)
#         d = [(m.group(1), m.group(2)) for view, field in i['query.formatted_fields']]
#         #result[i['query.model']]['view'].append(i['query.view'])
#
#     return result

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
